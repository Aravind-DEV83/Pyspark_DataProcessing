import os
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp, date_format, concat_ws, md5, col, lit, when, row_number, to_timestamp
from google.cloud import bigquery


## GCP Project Variables
service_account_path = "data-project.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = service_account_path
project_id = "PROJECT_ID"
dataset = "pyspark_scd"
temp_bucket = "spark-temp-406509"

class Operations:
    def __init__(self, sk_id) -> None:
        self.sk_id = sk_id
        self.spark = SparkSession.builder. \
                                appName("spark-bq-connect"). \
                                master("local[1]"). \
                                config("google.cloud.auth.service.account.enable", "true"). \
                                config("google.cloud.auth.service.account.json.keyfile", service_account_path). \
                                config('temporaryGcsBucket', temp_bucket). \
                                getOrCreate()
        
    
    def write_dataframe(self, df: DataFrame, table_name: str) -> None:

        df.write.format('bigquery') \
            .option("table", f"{project_id}.{dataset}.{table_name}") \
            .option('parentProject', project_id) \
            .option("temporaryGcsBucket", temp_bucket) \
            .mode("overwrite") \
            .save()
    
    def read_dataframe(self, table_name: str) -> DataFrame:

        return self.spark.read.format("bigquery") \
            .option("table", f"{project_id}.{dataset}.{table_name}") \
            .load()
    
    def tablesList(self, dataset_id: str, req_table: str) -> bool:

        list_objects = list(client.list_tables(dataset_id))
        l=[]
        for i in list_objects:
            l.append(i.table_id)
        return True if req_table in l else False
    
    def get_hash(self, df: DataFrame, keys_list: list) -> DataFrame:

        columns = [col(column) for column in keys_list]
        if columns:
            hash_values = df.withColumn("hash_md5", md5(concat_ws("_", *columns)))
        else:
            hash_values = df.withColumn("hash_md5", md5(lit(1)))

        return hash_values
    
    def column_renamer(self, df: DataFrame, suffix: str, append: str) -> DataFrame:
        if append:
            new_column_names = list(map(lambda x: x + suffix, df.columns))
        else:
            new_column_names = list(map(lambda x: x.replace(suffix, ""), df.columns))

        return df.toDF(*new_column_names)
    
    def no_change(self, merged_df: DataFrame, df_history_open: DataFrame) -> DataFrame:

        df = self.column_renamer(merged_df.filter(col("Action")=='NOCHANGE'), suffix="_history", append=False).\
                    select(df_history_open.columns)
        return df

    def inserted(self, merged_df: DataFrame, current_df: DataFrame, max_sk) -> DataFrame:

        df = self.column_renamer(merged_df.filter(col("action") == 'INSERT'), suffix="_current", append=False). \
                                            select(current_df.columns). \
                                            withColumn("effective_date", date_format(current_timestamp(), timestamp_format)). \
                                            withColumn("expiration_date", date_format(lit(dummy_timestamp), timestamp_format)). \
                                            withColumn("row_number",row_number().over(window_spec)). \
                                            withColumn(self.sk_id,col("row_number") + max_sk). \
                                            withColumn("is_current", lit(True)). \
                                            drop("row_number")
        return df
    
    def deleted(self, merged_df, df_history_open):
        df = self.column_renamer(merged_df.filter(col("action") == 'DELETE'), suffix="_history", append=False). \
                                                select(df_history_open.columns). \
                                                withColumn("expiration_date", date_format(current_timestamp(), timestamp_format)). \
                                                withColumn("is_current", lit(False))
        return df
        
    def updated(self, merged_df: DataFrame, df_history_open: DataFrame, current_df: DataFrame) -> DataFrame:
        df = self.column_renamer(merged_df.filter(col("action") == 'UPDATE'), suffix="_history", append=False). \
                                                    select(df_history_open.columns). \
                                                    withColumn("expiration_date", date_format(current_timestamp(), timestamp_format)). \
                                                    withColumn("is_current", lit(False)). \
                            unionByName(
                            self.column_renamer(merged_df.filter(col("action") == 'UPDATE'), suffix="_current", append=False). \
                                                    select(current_df.columns). \
                                                    withColumn("effective_date", date_format(current_timestamp(), timestamp_format)). \
                                                    withColumn("expiration_date", date_format(lit(dummy_timestamp), timestamp_format)). \
                                                    withColumn("row_number",row_number().over(window_spec)). \
                                                    withColumn(self.sk_id, col("row_number") + max_sk). \
                                                    withColumn("is_current", lit(True)). \
                                                    drop("row_number")
                                                    )
        return df
    

if __name__ == "__main__":

    #Project Variables
    timestamp_format = "yyyy-MM-dd HH:mm:ss"
    dummy_timestamp = "9999-12-31 23:59:59"
    scd_columns = ["company_name", "email", "phone", "zip_code"]

    source_table = "source"
    scd_table = "scd"
    pk_id = "CustomerID"
    sk_id = "sk_customer_id"


    #Instantiate a object from Operations class
    ops = Operations(sk_id)
    window_spec = Window.orderBy(pk_id)

    dataset_id = f'{project_id}.{dataset}'
    client = bigquery.Client()
    dest_table = ops.tablesList(dataset_id, scd_table)

    
    if dest_table: 
        print("Reading from source df as current - 1")
        current_df = ops.read_dataframe(source_table)

        print("Reading from staging df as history - 2")
        history_df = ops.read_dataframe(scd_table)

        max_sk = history_df.agg({sk_id: "max"}).collect()[0][0]

        df_history_open = history_df.where(col("is_current"))
        df_history_closed = history_df.where(col("is_current") == lit(False))


        df_history_open_hash = ops.column_renamer(ops.get_hash(df_history_open, scd_columns), suffix="_history", append=True)

        df_current_hash = ops.column_renamer(ops.get_hash(current_df, scd_columns), suffix="_current", append=True)


        merged_df = df_history_open_hash\
                    .join(df_current_hash, col(f"{pk_id}_current") ==  col(f"{pk_id}_history"), how="full_outer")\
                    .withColumn("Action", when(col("hash_md5_current") == col("hash_md5_history")  , 'NOCHANGE')\
                    .when(col(f"{pk_id}_current").isNull(), 'DELETE')\
                    .when(col(f"{pk_id}_history").isNull(), 'INSERT')\
                    .otherwise('UPDATE'))
        
        no_change_records = ops.no_change(merged_df, df_history_open)
        inserted_records = ops.inserted(merged_df, current_df, max_sk)

        if inserted_records.isEmpty():
            max_sk = merged_df.agg({f"{sk_id}_history": "max"}).collect()[0][0]
        else:
            max_sk = inserted_records.agg({sk_id: "max"}).collect()[0][0]

        deleted_records = ops.deleted(merged_df, df_history_open)
        updated_records = ops.updated(merged_df, df_history_open, current_df)

        final_df = df_history_closed\
            .unionByName(no_change_records)\
            .unionByName(inserted_records)\
            .unionByName(deleted_records)\
            .unionByName(updated_records)
        
        #Writing Final Dataframe to destination scd table
        ops.write_dataframe(final_df, scd_table)
        
    else:
        ## Intial Load
        print("Intial Load")
        source_df = ops.read_dataframe(source_table). \
                            withColumn(sk_id, row_number().over(window_spec)).\
                            withColumn("effective_date", date_format(current_timestamp(), timestamp_format)). \
                            withColumn("expiration_date", date_format(lit(dummy_timestamp), timestamp_format)). \
                            withColumn("is_current", lit(True))
        print("Intial Write")
        ops.write_dataframe(source_df, scd_table)

