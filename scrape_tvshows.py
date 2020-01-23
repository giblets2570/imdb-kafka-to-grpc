import requests
from bs4 import BeautifulSoup
import re
import time
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

def scrape_homepage():
    html = requests.get('https://www.imdb.com')

    soup = BeautifulSoup(html.text.strip(), features="html.parser")

    anchors = soup.find_all('a', href=True)

    title_links = [
        "https://www.imdb.com" + anchor['href']
        for anchor in anchors
        if re.match("/title/\w+", anchor['href'])
    ]

    return title_links

def get_raw_html(link):
    response = requests.get(link)
    return response.text.strip()

if __name__ == '__main__':
    kafka_producer = connect_kafka_producer()
    title_links = scrape_homepage()
    print(f'{len(title_links)} found')
    for link in title_links:
        print(link)
        raw_html = get_raw_html(link)
        publish_message(kafka_producer, 'raw_pages', link, raw_html)
    if kafka_producer is not None:
        kafka_producer.close()
