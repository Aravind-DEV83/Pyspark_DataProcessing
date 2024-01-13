import os
import pyspark
import pandas as pd
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format, count, col, sum, round



spark = SparkSession.builder. \
                    getOrCreate()

spark.sql(''' 
    CREATE OR REPLACE TEMPORARY VIEW orders (
          order_id INT, 
          order_date STRING,
          order_customer_id INT, 
          order_status STRING
    ) USING CSV
        OPTIONS (
          path='data/retail_db/orders/part-00000',
          sep=','
        )
''')

spark.sql(''' 
    CREATE OR REPLACE TEMPORARY VIEW order_items (
          order_item_id INT, 
          order_item_order_id INT,
          order_item_product_id INT, 
          order_item_quantity INT,
          order_item_subtotal FLOAT,
          order_item_product_price FLOAT
    ) USING CSV
        OPTIONS (
          path='data/retail_db/order_items/part-00000',
          sep=','
        )
''')

spark.sql(''' 
    INSERT OVERWRITE DIRECTORY 'data/retail_db/daily_product_revenue'
    USING CSV
    SELECT 
          o.order_date,
          oi.order_item_product_id, 
          round(sum(oi.order_item_subtotal), 2) as daily_revenue
    FROM orders o 
    INNER JOIN order_items oi
    ON o.order_id = oi.order_item_order_id
    WHERE o.order_status in ('COMPLETE', 'CLOSED')
    GROUP BY o.order_date, oi.order_item_product_id
    order by 1, 3 desc
''')

spark.stop()