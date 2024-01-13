import json
import time
import kafka
import csv
from datetime import datetime

TOPIC = 'myfirst'

def configure_kafka():

    producer = kafka.KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )

    return producer

def transform_data(line: json):

    return {
        'VendorID': int(line["VendorID"]), 
        'tpep_pickup_datetime': line["tpep_pickup_datetime"], 
        'tpep_dropoff_datetime': line["tpep_dropoff_datetime"], 
        'passenger_count': int(line["passenger_count"]), 
        'trip_distance': float(line["trip_distance"]), 
        'pickup_longitude': float(line["pickup_longitude"]), 
        'pickup_latitude': float(line["pickup_latitude"]), 
        'RatecodeID': int(line["RatecodeID"]), 
        'store_and_fwd_flag': line["store_and_fwd_flag"], 
        'dropoff_longitude': float(line["dropoff_longitude"]), 
        'dropoff_latitude': float(line["dropoff_latitude"]), 
        'payment_type': int(line["payment_type"]), 
        'fare_amount': float(line["fare_amount"]), 
        'extra': float(line["extra"]), 
        'mta_tax': float(line["mta_tax"]), 
        'tip_amount': float(line["tip_amount"]), 
        'tolls_amount': float(line["tolls_amount"]), 
        'improvement_surcharge': float(line["improvement_surcharge"]), 
        'total_amount': float(line["total_amount"])
    }

def intiate_stream():
    kafka_producer = configure_kafka()

    with open('uber_5.csv', 'r') as file:
        lines = csv.DictReader(file)

        count = 0
        for line in lines:
            count+=1

            transformed = transform_data(line)
            transformed["eventTime"] = str(datetime.now())

            kafka_producer.send(TOPIC, value=transformed)
            print(f'Published {transformed} to topic {TOPIC} - {count}')
            time.sleep(5)

if __name__ == "__main__":
    intiate_stream()