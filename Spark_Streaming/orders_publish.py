import json, csv
import time 
import kafka
from datetime import datetime

TOPIC = 'my-first'

def configure_kafka():

    producer = kafka.KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )

    return producer

def transform_data(line: json):

    return {
        'Store_id': line["Store_id"], 
        'Store_location': line["Store_location"], 
        'Product_id': line["Product_id"], 
        'Product_category': line["Product_category"], 
        'number_of_pieces_sold': int(line["number_of_pieces_sold"]), 
        'buy_rate': int(line["buy_rate"]),
        'sell_price': int(line["sell_price"])
    }


def add_unix():
    x = datetime.now()
    y = datetime.strptime(str(x), '%Y-%m-%d %H:%M:%S.%f')
    unix_timestamp = int(y.timestamp())
    return unix_timestamp


def intiate_stream():
    kafka_producer = configure_kafka()

    with open('store_sales.csv', 'r') as file:
        lines = csv.DictReader(file)
        number = 0
        for line in lines:
            number+=1
            
            transformed = transform_data(line)
            transformed["eventTime"] = add_unix()
            kafka_producer.send(TOPIC, value=transformed)
            print(f'Published {transformed} to topic {TOPIC} - {number}')
            time.sleep(3)

if __name__ == "__main__":
    intiate_stream()