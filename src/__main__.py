'''
Subscribe to Kafka topic that streams the Master database,
sanitize the data and replay it on the database
for BI queries

'''
#!/usr/bin / env python

import threading
import time
import logging
import json
import sys
import atexit
import yaml

from Anonymizer._process import ProcessEventStream
from Anonymizer._mysql import WriteToMySQL
#from _persist import PersistSanitizedData
#from _sanitize import SanitizeDataStream

CONFIG_FILE="config.yaml"

def main(args=None):
    """The main routine.
    """
    if args is None:
        args = sys.argv[1:]

    config_file = open(CONFIG_FILE)
    try:
        config = yaml.load(config_file)
    except ValueError, e:
        print "Corrupted configuration file %s. \n Cannot connect to Kafka brokers. \n Exiting" % config_file
        sys.exit()

    'Connect to Kafka brokers and subsribe to interested topics'
    cdc_inst = ProcessEventStream(
        config['kafka'],
        config['anonymizer']['kafka_topic']
    )

    """
    Get SQL statement from processed event stream,
    pass it to be executed on anonymized instance database
    """
    sql_conn_inst = WriteToMySQL(config['mysql'])
    stmt_cache = {}
    value_cache = {}
    stmt_cache,value_cache = cdc_inst._process_stream()
    for ts, stmt in stmt_cache.items():
        sql_conn_inst.run_sql(stmt, value_cache[ts])
    'Register connection cleanup function with atexit to be invoked if program is killed'
    #atexit.register(sql_conn_inst._cleanup_connections("Abort received. Closing Kafka, MySQL connections!"))

    'Cleanup connections before exiting'
    # self._cleanup_connections("Anonymized stream replayed on to slave MySQL database")


if __name__ == "__main__":
    # TO DO logging.config.fileConfig('log.ini')
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.INFO
    )
    main()
