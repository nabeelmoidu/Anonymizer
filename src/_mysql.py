"""
Subscribe to Kafka topic that streams the Master OMS database,
sanitize the data and replay it on the database
for BI queries
"""

#!/usr/bin/env python

from mysql.connector import errorcode
from mysql.connector import IntegrityError

import threading
import time
import logging
import sys
import mysql.connector


class WriteToMySQL():

    def __init__(self, mysql_conn_config):
        """Prepare connection to MySQL database to replay logs into.
          First check mysql connectivity and ensure the config provided is working. Else exit program
          Test database access credentials
          :rtype: object
        """
        try:
            self._mysql_conn = mysql.connector.connect(**mysql_conn_config)
            self._cursor = self._mysql_conn.cursor()
        except mysql.connector.Error as err:
            # Throw access error and close connection
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print('Wrong user name or password')
                sys.exit()
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print('Database does not exist')
                sys.exit()
            else:
                print(err)

    def run_sql(self,sql_stmt,values):
        # if self._mysql_conn is None:
        #    self._mysql_connect()
        """ Insert message stream as row into MySQL"""
        try:
            print sql_stmt
            print values
            self._cursor.execute(sql_stmt,values)
            self._mysql_conn.commit()
        except mysql.connector.ProgrammingError as e:
            logging.error('SQL Syntax error {0} in {1} Skipping row'.format(e, sql_stmt))
        except mysql.connector.Error as e:
            print " Error %s" % (format(e))
        """
        TBD : Run commits every N number of queries
        self._query_count += 1
        if self._query_count == 1000 or (time.time() - self._last_commit) > 60:
            self._connection.commit()
            self.save_state()
            self._query_count = 0
            self._last_commit = time.time()
        """

    def _cleanup_connections(self, message):
        print message
        self._cursor.close()
        self.__mysql_conn.close()
        self._consumer.close()


