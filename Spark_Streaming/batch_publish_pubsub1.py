from concurrent import futures
from google.cloud import pubsub_v1
import os
import time

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'pubsub.json'
TOPIC = 'TOPIC_ID'
project_id = 'PROJECT_ID'

batch_settings = pubsub_v1.types.BatchSettings(
    max_messages=1000
)

def configure_pubsub():

    publisher = pubsub_v1.PublisherClient(batch_settings)
    return publisher

def callback(future: pubsub_v1.publisher.futures.Future) -> None:
    message_id = future.result()
    # print("Messaged ID - > ", message_id)

def publish_messages(publisher, messages):

    publish_futures = []
    for element in messages:
        element = element.encode("utf-8")
        print('Message is', element)
        future_callback = publisher.publish(TOPIC, element)
        # future_callback.add_done_callback(callback)
        publish_futures.append(future_callback)
       

    futures.wait(publish_futures, return_when=futures.ALL_COMPLETED)
    print(f"Published messages without batch settings to {TOPIC}.")


def main():

    publisher = configure_pubsub()

    input_file = "uber_data.csv"
    with open(input_file, 'r') as read:
        data = read.readlines()[1:]
        size = 1000
        count = 0
        
        for i in range(0, len(data), size):
            count+=1
            batch = data[i: i+ size]
            print("Publishing {} messages to TOPIC counting {}".format(len(batch), count))
            publish_messages(publisher, batch)
            time.sleep(1)

if __name__ == "__main__":
    main()