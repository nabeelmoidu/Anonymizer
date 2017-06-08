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

class ReadFromKafkaStream:
    ' Import data from Kafka and push it to second MySQL instance '
    version = '0.2'

    def __init__(self, mysql_file, kafka_file, kafka_topic, interval):
        # We'll be using these two variables later on
        self._kafka_topic = kafka_topic
        self._interval = interval
        try:
            'Load connection parameters for MySQL connection'
            self._mysqlconn_config = json.load(open(mysql_file))
        except ValueError, e:
            print "Corrupted json file %s. \n Cannot proceed with database replay. \n Exiting" % mysql_file
            sys.exit()
                
        try: 
            'Load connection parameters for Kafka connection'
            self._kafkaconn_config = json.load(open(kafka_file))
        except ValueError, e:
            print "Corrupted json file %s. \n Cannot proceed with database replay. \n Exiting" % mysql_file
            sys.exit()

    def _kafka_connect(self):
        ' TODO: Set a loop for retry if initial connection attempt does not work'
        # while not events.exit.is_set():
        try:
            self._consumer = KafkaConsumer(**self._kafkaconn_config)
            self._consumer.subscribe([self._kafka_topic])
        except KafkaError as e:
            # Retry with backoff interval
            log.error("Problem communicating with Kafka (%s), retrying in %d seconds..." % (e, self._interval))
            time.sleep(self._interval)

    
    def _handle_event_type(self,event_type):
        'Invoke corresponding function for different event types :'
        ' INSERT/UPDATE/DELETE or schema changes'
        if event_type == "insert":
            self._construct_insert_stmt()
        elif event_type == "update":
            self._construct_update_stmt()
        elif event_type == "delete":
            self._construct_delete_stmt()
        else:
        logging.basicConfig(
            format='Unknown event type %s in BinLog entry' % event_type ,
            level=logging.INFO
            )


class PersistSanitizedData:

class WriteToMySQL(WriteToDataWarehouse):

        def _run_sql(self):
        if self._mysql_conn is None:
            self._mysql_connect()
        ' Insert message stream as row into MySQL'
        try:
             self._cursor.execute(self._sql_stmt)
             try :
                 if self._cdc_event["commit"]:
                     logging.info('Committing Transaction ID {0} to table {1}'.format(
                     self._cdc_event["xid"],
                     self._cdc_event["table_name"]
                     )
                     self._mysql_conn.commit()
                     logging.debug('{}'.format(self._cursor._executed)
                 else :
                     logging.debug('Uncommitted transaction in progress ID {0} to table {1}'.format(
                     self._cdc_event["xid"],
                     self._cdc_event["table_name"]
                     )
        except KeyError :
            logging.error('Key error {0} on Transaction ID {1} in table {2}'.format(
            e,
            self._cdc_event["xid"],
            self._cdc_event["table_name"]
            )
        except UnicodeDecodeError as e:
            logging.error('Unicode Decode Error {0} on Transaction ID {1} in table {2}'.format(
            e,
            self._cdc_event["xid"],
            self._cdc_event["table_name"]
            )
        except IntegrityError as e:
            logging.error('Row integrity error {0} on Transaction ID {1} in table {2}. Skipping row'.format(
            e,
            self._cdc_event["xid"],
            self._cdc_event["table_name"]
            )
        '''
        TBD : Run commits every N number of queries
        self._query_count += 1
        if self._query_count == 1000 or (time.time() - self._last_commit) > 60:
            self._connection.commit()
            self.save_state()
            self._query_count = 0
            self._last_commit = time.time()
        '''
     
    def _mysql_connect(self):
        ' Prepare connection to MySQL database to replay logs into '
        #Test database access credentials
        try:
            self._mysql_conn = mysql.connector.connect(**self._mysqlconn_config)
            self._cursor = self._mysql_conn.cursor()
        except mysql.connector.Error as err:
            ' Throw access error and close connection '
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
                sys.exit()
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
                sys.exit()
            else:
                print(err)

    def _construct_insert_stmt(self):	
        'Empty k/v pairs from previous iteration'
        keys = ""
        values = ""
        'Extract the key value pairs from the event row and '
        'put them in separate variables for use in the replay SQL statement '
        for k, v in self._cdc_event["data"].items():
            keys += "{0},".format(k)
            values += "\"{0}\",".format(v)
        ' remove trailing comma'
        self._keys = keys[: -1]
        self._values = values[: -1]
        'Construct sql for INSERT statement '
        self._sql_stmt = "INSERT INTO %s (%s) VALUES (%s)" % (
              self._cdc_event["table"], self._keys, self._values
               )
    
    def _construct_update_stmt(self):
        'Empty k/v pairs from previous iteration'
        curr_col = ""
        row_str = ""
        'WHERE clause part is referred as \"old\" in the json object in the change log'
        curr_col_old = ""
        row_str_old = ""

        'Construct SET clause in UPDATE statement '
        for k,v in self._cdc_event["data"].items():
            curr_col = " %s = \"%s\"" % (format(k), format(v))
            row_str = "{0} {1},".format(row_str,curr_col)
        'Strip trailing comma'
        stmt_set_clause = row_str.strip()[:-1]

        'Construct sql for UPDATE statement '
        for k,v in self._cdc_event["old"].items():
            curr_col_old = " %s = \"%s\"" % (format(k), format(v))
            row_str_old = "{0} {1} AND".format(row_str_old,curr_col_old)
        'Strip trailing \"and\"'
        stmt_where_clause = row_str_old.strip().rsplit(' ',1)[0]

        self._sql_stmt = "UPDATE %s SET %s WHERE %s" % (
               self._cdc_event["table"], stmt_set_clause, stmt_where_clause
               )


    def _construct_delete_stmt(self):
        'Empty k/v pairs from previous iteration'
        curr_column = ""
        row_str = ""
        'Construct WHERE clause for DELETE statement '
        for k,v in self._cdc_event["data"].items():
            curr_column = " %s = \"%s\"" % (format(k), format(v))
            row_str = "{0} {1} AND".format(row_str,curr_column)

        'Strip trailing word \"and\" from row_str above before passing to SQL'
        self._sql_stmt = "DELETE FROM %s WHERE %s" % (
              self._cdc_event["table"], row_str.strip().rsplit(' ',1)[0]
               )

    def _anonymize_message(self):
        pass

     def _cleanup_connections(self,message):
        print message
        self._cursor.close()
        self.__mysql_conn.close()
        self._consumer.close()

    def _process_stream(self,message):
        '''
        First, check if the commit log entry is a committed transaction (commit = true),
        else it is likely a multi row update. Raise an assert for such events to
        skip them in the loop invoking the rest of the workflow
        '''
        'The consumer instance now has the stream of events from the BinLog.'
        'We will now loop through the entries to process the statements'
        self._cdc_event = json.loads(message.value)
        'Sanitize sensitive entries in the event stream before replaying on the slave database'
        self._anonymize_message()
        'According to the event type - INSERT/UPDATE/DELETE construct the SQL statement accordingly'
        self._handle_event_type(self._cdc_event["type"])
        'Once the SQL is prepared, run it on the slave instance'
        self._run_sql()
    
    def _start_resume_stream(self) :
        ' Resume stream replay from last known entry in bin log'
        ' TODO: Insert logic here to fetch last known entry in bin log stream and resume from where we last stopped.'

        ' self._consumer is the kafka stream subscribed to KAFKA_BINLOG_TOPIC. Loop through'
        for message in self._consumer:
            self._process_stream(message)
           
    def main(self):
        'First check mysql connectivity and ensure the config provided is working. Else exit program'
        self._mysql_connect()

        'Connect to Kafka brokers and subsribe to KAFKA_BINLOG_TOPIC'
        self._kafka_connect()

        'Register connection cleanup function with atexit to be invoked if program is killed'
        #atexit.register(self._cleanup_connections("Abort received. Closing Kafka, MySQL connections!"))

        ' Start or resume reading from event stream in kafka '
        self._start_resume_stream()

        'Cleanup connections before exiting'
        self._cleanup_connections("Anonymized stream replayed on to slave MySQL database")
        

if __name__ == "__main__":
    # TODO logging.config.fileConfig('log.ini')
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.INFO
    ) 
    K_inst = KafkaToMysql(
        MYSQL_CONFIG_FILE,
        KAFKA_CONFIG_FILE,
        KAFKA_BINLOG_TOPIC,
        KAFKA_CONN_RETRY_INTERVAL
    )
    K_inst.main()
