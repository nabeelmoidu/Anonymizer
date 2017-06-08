'''
Subscribe to Kafka topic that streams the Master OMS database,
sanitize the data and replay it on the database
for BI queries

'''
#!/usr/bin / env python

from kafka import KafkaConsumer
from kafka.errors import KafkaError
from pprint import pprint
from mysql.connector import errorcode
from mysql.connector import IntegrityError

import threading
import time
import logging
import json
import sys
import atexit
import mysql.connector

KAFKA_CONFIG_FILE="kafka_connect.json"
MYSQL_CONFIG_FILE="mysql_connect.json"
KAFKA_BINLOG_TOPIC="testtopic.kafka"
KAFKA_CONN_RETRY_INTERVAL=10

class PersistSanitizedData :

