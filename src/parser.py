import json
from time import sleep

from bs4 import BeautifulSoup
from kafka import KafkaConsumer, KafkaProducer


def publish_message(producer_instance, topic_name, key, value):
    try:
        key_bytes = bytes(key, encoding='utf-8')
        value_bytes = bytes(value, encoding='utf-8')
        producer_instance.send(topic_name, key=key_bytes, value=value_bytes)
        producer_instance.flush()
        print('Message published successfully.')
    except Exception as ex:
        print('Exception in publishing message')
        print(str(ex))


def connect_kafka_producer():
    _producer = None
    try:
        _producer = KafkaProducer(bootstrap_servers=['localhost:9091'], api_version=(0, 10))
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return _producer


def parse(markup):
    title = '-'
    submit_by = '-'
    description = '-'
    publication_date = '-'
    rec = {}

    try:

        soup = BeautifulSoup(markup, 'lxml')
        # title
        title_section = soup.select('#content > header > h1')
        #submitter
        submitter_section = soup.select('#viewlet-above-content-body > div.detalhe-noticia > strong')
        #description
        description_section = soup.select('#content > header > div.documentDescription.description')
        #date_submitter
        publication_date_section = soup.select('#viewlet-above-content-body > div.detalhe-noticia > span')

        if submitter_section:
            submit_by = submitter_section[0].text.strip()

        if title_section:
            title = title_section[0].text

        if description_section:
            description = description_section[0].text.strip().replace('"', '')

        if publication_date_section:
            publication_date = publication_date_section[0].text.strip().replace('"', '')

        rec = {'title': title, 'submit_by': submit_by, 'description': description,
               'publication_date': publication_date}
    except Exception as ex:
        print('Exception while parsing')
        print(str(ex))
    finally:
        return json.dumps(rec)


if __name__ == '__main__':
    print('Running Consumer..')
    parsed_records = []
    topic_name = 'raw_news'
    parsed_topic_name = 'parsed_news'

    consumer = KafkaConsumer(topic_name, auto_offset_reset='earliest',
                             bootstrap_servers=['localhost:9091'], api_version=(0, 10), consumer_timeout_ms=1000)
    for msg in consumer:
        html = msg.value
        result = parse(html)
        parsed_records.append(result)
    consumer.close()
    sleep(5)

    if len(parsed_records) > 0:
        print('Publishing records..')
        producer = connect_kafka_producer()
        for rec in parsed_records:
            publish_message(producer, parsed_topic_name, 'parsed', rec)
        if producer is not None:
            producer.close()