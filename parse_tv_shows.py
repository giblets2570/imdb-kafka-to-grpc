import requests
from bs4 import BeautifulSoup
import re
import time, json
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
        _producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return _producer

def parse(markup):
    soup = BeautifulSoup(markup, features='html.parser')
    try:
        title = soup.find('div', {'class': 'title_wrapper'}).find('h1').text.strip()
        rating = soup.find('div', {'class': 'ratings_wrapper'}).find('span', {'itemprop': 'ratingValue'}).text

        return {
            'title': title,
            'rating': rating,
        }
    except Exception as e:
        print(e)

if __name__ == '__main__':

    parsed_records = []
    topic_name = 'raw_pages'
    parsed_topic_name = 'parsed_pages'

    consumer = KafkaConsumer(topic_name, auto_offset_reset='earliest',
                             bootstrap_servers=['localhost:9092'], consumer_timeout_ms=1000)


    producer = connect_kafka_producer()
    try:
        while True:
            topic_partitions = consumer.poll(500)
            if len(topic_partitions.keys()) == 0:
                continue
            for partition in topic_partitions.values():
                for msg in partition:
                    html = msg.value
                    result = parse(html)
                    if result:
                        publish_message(producer, parsed_topic_name, msg.key.decode('utf-8'), json.dumps(result))
    except KeyboardInterrupt:
        pass

    finally:
        consumer.close()
        producer.close()
