import logging
import time
import sys

from kafka.errors import KafkaError
from kafka import KafkaConsumer

class ReadFromKafkaStream:
    '''
    KafkaStreamReader.py
        Connect to Kafka and pull stream object
    IN
        Kafka connection config, Kafka topic, retry interval
    OUT
        Kafka Consumer object
    '''
    version = '0.2'

    def __init__(self, config, topic, retry_interval):
        self._kafka_topic = topic
        self._interval = retry_interval
        self._kafka_conn_config = config

    def kafka_connect(self):
        """ TODO: Set a loop for retry if initial connection attempt does not work"""
        # while not events.exit.is_set():
        try:
            self._consumer = KafkaConsumer(**self._kafka_conn_config)
            self._consumer.subscribe([self._kafka_topic])
        except KafkaError as e:
            # Retry with backoff interval
            logging.error("Problem communicating with Kafka (%s), retrying in %d seconds..." % (e, self._interval))
            time.sleep(self._interval)
        return self._consumer
