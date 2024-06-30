import os
import csv
import json
from google.cloud import pubsub_v1
import time

# Set Google Cloud credentials environment variable
cred_path = 'E:/Capstone/orbital-anchor-419817-5f8424b1e901.json'
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = cred_path

# Initialize PublisherClient and topic path
publisher = pubsub_v1.PublisherClient()
topic_path = 'projects/orbital-anchor-419817/topics/capstone'


def read_customer_data_from_csv(file_path):
    customer_data = []
    with open(file_path, 'r', encoding='utf-8') as csvfile:  # Specify the correct encoding
        csv_reader = csv.DictReader(csvfile)
        for row in csv_reader:
            customer_data.append(row)
    return customer_data

# Publish customer data to Pub/Sub topic in batches
def publish_to_pubsub(data):
    current_position=0
    while current_position < len(data):
        batch = data[current_position:current_position + 10]  # Get the next 10 rows
        batch_json = json.dumps(batch).encode('utf-8')
        publisher.publish(topic_path, data=batch_json)
        print("Published batch to Pub/Sub:", batch)
        time.sleep(10)  # Sleep for 5 seconds between batches
        current_position += 10  # Move to the next batch

if __name__ == "__main__":
    # Read customer data from CSV file
    csv_file_path = 'E:\Capstone\Dataset\production_orders.csv'
    customer_data = read_customer_data_from_csv(csv_file_path)

    # Publish customer data to Pub/Sub in batches
    publish_to_pubsub(customer_data)
