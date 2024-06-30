import os
import pip._vendor.requests
from google.cloud import pubsub_v1
import time

# Set Google Cloud credentials environment variable
cred_path = 'E:/Capstone/orbital-anchor-419817-5f8424b1e901.json'
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = cred_path

# Initialize PublisherClient and topic path
publisher = pubsub_v1.PublisherClient()
topic_path = 'projects/orbital-anchor-419817/topics/capstone'

api="https://www.boredapi.com/activity/"

while True:
    response=pip._vendor.requests.request("GET",api,verify=0).text
    print("Received response:", response)
    publisher.publish(topic_path,data=response.encode('utf-8'))
    time.sleep(10)