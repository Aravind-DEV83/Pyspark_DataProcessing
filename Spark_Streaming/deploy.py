import os
import logging
from itertools import chain
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType, IntegerType, FloatType, TimestampType, LongType, DoubleType
from pyspark.sql.functions import from_json, col, lit, to_timestamp, round, when, create_map

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')

## GCP Variables
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = 'data-project.json'
project_id = "PROJECT_ID"
dataset="DATASET_ID"
temp_bucket = "spark-temp-406509"


def intialize_spark_session(app_name):

    spark = SparkSession.builder. \
                    appName(app_name). \
                    config(). \
                    getOrCreate()
    
                    # config("spark.jars.packages", packages). \
                    # config("spark.sql.shuffle.partitions", 50). \
                    
    logging.info("Spark Session Intiated sucessfully")
    return spark

def get_streaming_data(spark, TOPIC, BOOT_STRAP):

    df = spark.readStream. \
                format("kafka"). \
                option("kafka.bootstrap.servers", BOOT_STRAP). \
                option("subscribe", TOPIC). \
                option("startingOffsets", "latest"). \
                load()
    
    logging.info("Fetching kafka messages ...................")
    return df

def parse_stream_data(df):

    schema = StructType([
        StructField("VendorID", IntegerType(), True),
        StructField("tpep_pickup_datetime", TimestampType(), True),
        StructField("tpep_dropoff_datetime", TimestampType(), True),
        StructField("passenger_count", IntegerType(), True),
        StructField("trip_distance", FloatType(), True),
        StructField("pickup_longitude", FloatType(), True),
        StructField("pickup_latitude", FloatType(), True),
        StructField("RatecodeID", IntegerType(), True),
        StructField("store_and_fwd_flag", StringType(), True),
        StructField("dropoff_longitude", FloatType(), True),
        StructField("dropoff_latitude", FloatType(), True),
        StructField("payment_type", IntegerType(), True),
        StructField("fare_amount", FloatType(), True),
        StructField("extra", FloatType(), True),
        StructField("mta_tax", FloatType(), True),
        StructField("tip_amount", FloatType(), True),
        StructField("tolls_amount", FloatType(), True),
        StructField("improvement_surcharge", FloatType(), True),
        StructField("total_amount", FloatType(), True),
        StructField("eventTime", TimestampType(), True)
    ])

    parsed_df = df. \
            selectExpr("CAST(value as STRING)"). \
            withColumn("json_data", from_json(col("value"), schema)).select("json_data.*")
    
    return parsed_df

def transform_stream_data(parsed_df):

    payment_type = {
        1: "Credit card",
        2:"Cash",
        3:"No charge",
        4:"Dispute",
        5:"Unknown",
        6:"Voided tip"
    }
    rate_code = {
        1:"Standard rate",
        2:"JFK",
        3:"Newark",
        4:"Nassau or Westchester",
        5:"Negotiated fare",
        6:"Group ride"
    }

    average_speed = 30

    featured_df = parsed_df. \
                    withColumn("tpep_pickup_datetime", to_timestamp(col("tpep_pickup_datetime"))). \
                    withColumn("tpep_dropoff_datetime", to_timestamp(col("tpep_dropoff_datetime"))). \
                    withColumn("total_ride_time", when(col("tpep_pickup_datetime") != col("tpep_dropoff_datetime"), 
                                    round(col("tpep_dropoff_datetime").cast("long") - col("tpep_pickup_datetime").cast("long"))).otherwise(None)
                    ). \
                    withColumn("estimated_ride_time", round(col("trip_distance") / average_speed * 60 * 60)). \
                    withColumn("total_speed", when(col("total_ride_time") != 0, 
                                                round((col("trip_distance") / col("total_ride_time")) * 3600)).otherwise(None))

    payment_expr = create_map([lit(x) for x in chain(*payment_type.items())])
    rate_code_expr = create_map([lit(x) for x in chain(*rate_code.items())])


    new_df = featured_df. \
                    withColumn("payment_type_name", payment_expr[col("payment_type")]). \
                    withColumn("RateCode_name", rate_code_expr[col("RatecodeID")])
    
    return new_df

def foreach_batch_function(dataframe, id):
    dataframe.write.format('bigquery') \
            .option("table", f"{project_id}.{dataset}.streaming") \
            .option('parentProject', project_id) \
            .option("temporaryGcsBucket", temp_bucket) \
            .mode("append") \
            .save()

def streaming_to_bigquery(df):
    logging.info("Writing kafka messages to bigquery ...................")
    query = df \
        .writeStream \
        .outputMode("append") \
        .foreachBatch(foreach_batch_function) \
        .start()
    
    # query = new_df. \
    #         writeStream. \
    #         format("console"). \
    #         outputMode("append"). \
    #         start()

    query.awaitTermination()

def run_pipeline():
    # scala_version = "2.12"
    # spark_version = "3.5.0"

    # packages = f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}'
    app_name = "Spark Structured Streaming"

    BOOT_STRAP = "34.70.130.6:9092"
    TOPIC = "kafkademo"

    # spark = intialize_spark_session(app_name, packages)
    spark = intialize_spark_session(app_name)

    if spark:
        df = get_streaming_data(spark, TOPIC, BOOT_STRAP)

        if df:
            parsed_df = parse_stream_data(df)
            transformed_df = transform_stream_data(parsed_df)

            streaming_to_bigquery(transformed_df)


if __name__ == "__main__":
    run_pipeline()