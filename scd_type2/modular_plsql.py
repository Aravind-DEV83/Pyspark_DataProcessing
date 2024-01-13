import os
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp, date_format, to_date, concat_ws, md5, col, lit, when, row_number, to_timestamp

# URL to connect to my postgresql local server
url = "jdbc:postgresql://localhost:5432/retail_db"
user_name = os.environ.get("user_name")
pdb_password = os.environ.get("pdb_pass")
scd_columns = ["company_name", "email", "phone", "zip_code"]
timestamp_format = "yyyy-MM-dd HH:mm:ss"
dummy_timestamp = "9999-12-31 23:59:59"


class Operations:

    def __init__(self) -> None:
        self.max_sk = 0

    def get_hash(self, df: DataFrame, keys_list: list):

        columns = [col(column) for column in keys_list]
        if columns:
            hash_values = df.withColumn("hash_md5", md5(concat_ws("_", *columns)))
        else:
            hash_values = df.withColumn("hash_md5", md5(lit(1)))

        return hash_values
    
    def column_renamer(self, df: DataFrame, suffix: str, append: str):
        if append:
            new_column_names = list(map(lambda x: x + suffix, df.columns))
        else:
            new_column_names = list(map(lambda x: x.replace(suffix, ""), df.columns))

        return df.toDF(*new_column_names)

    def read_dataframe(self, table_name: str) -> DataFrame:

        df = spark.read.format("jdbc"). \
                                option("url", url). \
                                option("driver", "org.postgresql.Driver"). \
                                option("dbtable", table_name). \
                                option("user", user_name). \
                                option("password", pdb_password). \
                                load()
        return df
    
    def write_dataframe(self, df, table_name: str, mode: str) -> None:

        df.write.format("jdbc"). \
                mode(mode). \
                option("url", url). \
                option("driver", "org.postgresql.Driver"). \
                option("dbtable", table_name). \
                option("user", user_name). \
                option("password", pdb_password). \
                save()
        return

    def no_change(self, merged_df: DataFrame, df_history_open: DataFrame) -> DataFrame:
        no_change = self.column_renamer(merged_df.filter(col("action") == 'NOCHANGE'), suffix="_history", append=False). \
                                    select(df_history_open.columns)
        return no_change

    def insert(self, merged_df: DataFrame, customers_current: DataFrame, window_spec, max_sk) -> DataFrame:
        
        customers_insert = self.column_renamer(merged_df.filter(col("action") == 'INSERT'), suffix="_current", append=False). \
                                                    select(customers_current.columns). \
                                                    withColumn("effective_date", date_format(current_timestamp(), timestamp_format)). \
                                                    withColumn("expiration_date", date_format(lit(dummy_timestamp), timestamp_format)). \
                                                    withColumn("row_number",row_number().over(window_spec)). \
                                                    withColumn("sk_customer_id",col("row_number")+ max_sk). \
                                                    withColumn("is_current", lit(True)). \
                                                    drop("row_number")

        return customers_insert

    def update(self, merged_df: DataFrame,  df_history_open: DataFrame, customers_current: DataFrame, window_spec, max_sk) -> DataFrame:

        customers_update = self.column_renamer(merged_df.filter(col("action") == 'UPDATE'), suffix="_history", append=False). \
                                                    select(df_history_open.columns). \
                                                    withColumn("expiration_date", date_format(current_timestamp(), timestamp_format)). \
                                                    withColumn("is_current", lit(False)). \
                            unionByName(
                            self.column_renamer(merged_df.filter(col("action") == 'UPDATE'), suffix="_current", append=False). \
                                                    select(customers_current.columns). \
                                                    withColumn("effective_date", date_format(current_timestamp(), timestamp_format)). \
                                                    withColumn("expiration_date", date_format(lit(dummy_timestamp), timestamp_format)). \
                                                    withColumn("row_number",row_number().over(window_spec)). \
                                                    withColumn("sk_customer_id",col("row_number")+ max_sk). \
                                                    withColumn("is_current", lit(True)). \
                                                    drop("row_number")
                                                    )

        return customers_update
    
    def delete(self, merged_df: DataFrame, df_history_open: DataFrame, customers_insert: DataFrame) -> DataFrame:


        customers_delete = self.column_renamer(merged_df.filter(col("action") == 'DELETE'), suffix="_history", append=False). \
                                                    select(df_history_open.columns). \
                                                    withColumn("expiration_date", date_format(current_timestamp(), timestamp_format)). \
                                                    withColumn("is_current", lit(False))
        
        return customers_delete


if __name__ == "__main__":
    ops = Operations()

    spark = SparkSession.builder. \
                    master("local[1]"). \
                    appName("SCD_Type_2_Implementation") .\
                    getOrCreate()
    
    ## Read from a table
    print("Reading from Source table -- > 1")
    window_spec  = Window.orderBy("customerid")

    customers_df = ops.read_dataframe("source"). \
                                withColumn("sk_customer_id", row_number().over(window_spec)).\
                                withColumn("effective_date", date_format(current_timestamp(), timestamp_format)). \
                                withColumn("expiration_date", date_format(lit(dummy_timestamp), timestamp_format)). \
                                withColumn("is_current", lit(True))

    ## Write to postgresql as incremental table
    print('Writing to Incremental table')
    ops.write_dataframe(customers_df, "incremental", "overwrite")


    print("Reading from a source table as current --> 2")
    ## Read current data (i.e., soruce table)
    customers_current = ops.read_dataframe("source")
    print("Reading from a incremental table as history --> 3")
    ## Read history data (i.e., Incremental table)
    customers_history = ops.read_dataframe("incremental"). \
                                withColumn("effective_date", to_timestamp("effective_date")). \
                                withColumn("expiration_date", to_timestamp("expiration_date"))
    

    ## Update the Maximum Surrogate Key ID
    max_sk = customers_history.agg({"sk_customer_id": "max"}).collect()[0][0]
    print("Maximum surrogate Key id", max_sk)

    df_history_open = customers_history.where(col("is_current"))
    df_history_closed = customers_history.where(col("is_current") == lit(False))


    customers_history_open_hash = ops.column_renamer(ops.get_hash(df_history_open, scd_columns), suffix="_history", append=True)

    customers_current_hash = ops.column_renamer(ops.get_hash(customers_current, scd_columns), suffix="_current", append=True)


    merged_df = customers_history_open_hash\
                .join(customers_current_hash, col("CustomerID_current") ==  col("CustomerID_history"), how="full_outer")\
                .withColumn("Action", when(col("hash_md5_current") == col("hash_md5_history")  , 'NOCHANGE')\
                .when(col("CustomerID_current").isNull(), 'DELETE')\
                .when(col("CustomerID_history").isNull(), 'INSERT')\
                .otherwise('UPDATE'))

    window_spec  = Window.orderBy("customerid")

    no_change_records = ops.no_change(merged_df, df_history_open)
    inserted_records = ops.insert(merged_df, customers_current, window_spec, max_sk)

    if inserted_records.isEmpty():
        max_sk = merged_df.agg({"sk_customer_id_history": "max"}).collect()[0][0]
    else:
        max_sk = inserted_records.agg({"sk_customer_id": "max"}).collect()[0][0]

    print("Maximum surrogate Key Id: ", max_sk)

    deleted_records = ops.delete(merged_df, df_history_open, inserted_records)
    updated_records = ops.update(merged_df, df_history_open, customers_current, window_spec, max_sk)

    final_df = df_history_closed.unionByName(no_change_records). \
                                unionByName(inserted_records). \
                                unionByName(deleted_records). \
                                unionByName(updated_records)
    
    ## Write the updated df to SCD Type 2 table
    ops.write_dataframe(final_df, "scd", "overwrite")
    
    ## SCD in Sync with Incremental table. Reduce processing previous operations performed
    sync_scd_incremental = ops.read_dataframe("scd")                    
    ops.write_dataframe(sync_scd_incremental, "incremental", "overwrite")


    
