from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, StringType, TimestampType
from pyspark.sql.functions import from_unixtime, from_json, col, window, sum, to_timestamp, session_window, when, count

def intialize_spark_session(app_name, packages):

    spark = SparkSession.builder. \
                    appName(app_name). \
                    config("spark.jars.packages", packages). \
                    config("spark.sql.shuffle.partitions", 1). \
                    getOrCreate()
    
                    # config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false"). \
                    
    return spark

def get_streaming_data(spark, TOPIC, BOOT_STRAP):

    df = spark.readStream. \
                format("kafka"). \
                option("kafka.bootstrap.servers", BOOT_STRAP). \
                option("subscribe", TOPIC). \
                option("startingOffsets", "latest"). \
                load()
    
    return df

def parse_stream_data(df):

    schema = StructType([
        StructField("Store_id", StringType(), True),
        StructField("Store_location", StringType(), True),
        StructField("Product_id", StringType(), True),
        StructField("Product_category", StringType(), True),
        StructField("number_of_pieces_sold", IntegerType(), True),
        StructField("buy_rate", IntegerType(), True),
        StructField("sell_price", IntegerType(), True),
        StructField("eventTime", StringType(), True),
    ])

    return df.selectExpr("CAST(value as STRING)"). \
        withColumn("json_data", from_json(col("value"), schema)).select("json_data.*"). \
        withColumn("eventTime", to_timestamp(from_unixtime(col("eventTime"), "yyyy-MM-dd HH:mm:ss.SSS")))

def run_pipeline():
    scala_version = "2.12"
    spark_version = "3.5.0"

    packages = f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}'
    app_name = "Windowing Concepts"

    BOOT_STRAP = "localhost:9092"
    TOPIC = "my-first"

    spark = intialize_spark_session(app_name, packages)


    df = get_streaming_data(spark, TOPIC, BOOT_STRAP)
    parsed_df = parse_stream_data(df)

    parsed_df = parsed_df. \
                        withColumn("profit", (col("sell_price") - col("buy_rate") ) * col("number_of_pieces_sold"))
    
    ## Tumbling Windows
    total_counts = parsed_df.withWatermark("eventTime", "1 minute"). \
    groupby(window("eventTime", "1 minute")
    ).agg(count(col("Store_id")).alias("total_counts")). \
    select(
        col("total_counts"),
        col("window.start").alias("window_starts"), 
        col("window.end").alias("window_ends")
    )

    ## Sliding Windows
    total_counts = parsed_df.withWatermark("eventTime", "30 seconds"). \
    groupby(
        "Store_id",
        window("eventTime", "2 minute", "1 minute")
    ).agg(count(col("Store_id")).alias("total_counts")). \
    select(
        "Store_id", "total_counts", col("window.start"), col("window.end")
    )

    ## Session Window static duration
    total_counts = parsed_df.withWatermark("eventTime", "1 minute"). \
    groupby(
        "Store_id", session_window("eventTime", "1 minute")
    ).agg(count(col("Store_id")).alias("total_counts")). \
    select(
        "Store_id", "total_counts", col("session_window.start"), col("session_window.end")
    )
    # ## Session with dyanmic gap duration
    windowed = session_window("eventTime", when(col("Store_id") == 'STR_5', "20 seconds").otherwise("2 minutes"))
    total_counts = parsed_df.withWatermark("eventTime", "5 minutes"). \
    groupby(
        "Store_id",
        windowed
    ).agg(count(col("Store_id")).alias("total_counts")). \
    select(
        col("Store_id"), col("total_counts"), col("session_window.start"), col("session_window.end")
    )


    query = total_counts. \
            writeStream. \
            format("console"). \
            outputMode("complete"). \
            start(). \
            awaitTermination()

if __name__ == "__main__":
    run_pipeline()