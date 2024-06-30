"""
This script imports a function to 
generate fake user log data and then
publishes it to a pub/sub topic 
"""
from faker import Faker
import time
import random
import os
import numpy as np
from datetime import datetime, timedelta

import logging
from google.cloud import pubsub_v1
import random
import time
import configparser

import requests
import os
import time
import json
import configparser
from google.cloud import pubsub_v1


LINE = """\
{remote_addr} - - [{time_local}] "{request_type} {request_path} HTTP/1.1" [{status}] {body_bytes_sent} "{http_referer}" "{http_user_agent}"\
"""

cred='E:\Capstone\orbital-anchor-419817-5f8424b1e901.json'

def generate_log_line():
    fake = Faker()
    now = datetime.now()
    remote_addr = fake.ipv4()
    time_local = now.strftime('%d/%b/%Y:%H:%M:%S')
    request_type = random.choice(["GET", "POST", "PUT"])
    request_path = "/" + fake.uri_path()

    status = np.random.choice([200, 401, 404], p = [0.9, 0.05, 0.05])
    body_bytes_sent = random.choice(range(5, 1000, 1))
    http_referer = fake.uri()
    http_user_agent = fake.user_agent()

    log_line = LINE.format(
        remote_addr=remote_addr,
        time_local=time_local,
        request_type=request_type,
        request_path=request_path,
        status=status,
        body_bytes_sent=body_bytes_sent,
        http_referer=http_referer,
        http_user_agent=http_user_agent
    )

    return log_line


class Publish:
    def __init__(self, config):
        self.project_id = str('orbital-anchor-419817')
        self.topic_id = str('projects/orbital-anchor-419817/topics/pubsub_capstone')
        # self.api_url = str(config['API']['API_URL'])+str(config['API']['API_KEY'])
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = cred
    def publish(self, message):
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(self.project_id,self.topic_id)
        data = message.encode('utf-8')
        return publisher.publish(topic_path, data = data)



    def callback(self, message_future):
        # When timeout is unspecified, the exception method waits indefinitely.
        if message_future.exception(timeout=30):
            print('Publishing message on {} threw an Exception {}.'.format(
                self.topic_id, message_future.exception()))
        else:
            print(message_future.result())


if __name__=='__main__':
    #read configuration file
    config = configparser.ConfigParser()
    config.read('streamingDataPipeline/utils/example.cfg')
    
    pub = Publish(config)
    i=0
    while i<5:
        line = generate_log_line()
        print(line)
        message_future = pub.publish(line)
        message_future.add_done_callback(pub.callback)

        sleep_time = random.choice(range(1, 3, 1))
        time.sleep(sleep_time)
        i+=1