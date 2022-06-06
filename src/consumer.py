import json
from time import sleep

from kafka import KafkaConsumer

if __name__ == '__main__':
    parsed_topic_name = 'parsed_news'

    consumer = KafkaConsumer(parsed_topic_name, auto_offset_reset='earliest',
                             bootstrap_servers=['localhost:9091'], api_version=(0, 10), consumer_timeout_ms=1000)
    for msg in consumer:
        record = json.loads(msg.value)
        title = record['title']
        author = record['submit_by']
        publication_date = record['publication_date']
        description = record['description']

        if title:
            print('Title: {}, Submit_by: {}, Publication_date: {}, Description: {}'.format(title, author, publication_date, description))
        print("record...")

        sleep(3)

    if consumer is not None:
        consumer.close()