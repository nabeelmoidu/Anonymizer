import json
import logging

from kafka import KafkaConsumer
from kafka.errors import KafkaError

class ProcessEventStream:
    """
    ProcessEventStream.py
        Process stream object, iterate through events,  identify event type,
        pass individual events to SanitizeDataStream class
    IN
        Kafka Consumer object
    OUT
        CDC Event, SQL Statement

    """

    def __init__(self, config, topic, retry_interval):
        """ Initialize Kafka consumer object created in KafkaStreamReader """
        """ TODO: Set a loop for retry if initial connection attempt does not work"""
        # while not events.exit.is_set():
        try:
            self._consumer = KafkaConsumer(**config)
            self._consumer.subscribe([topic])
        except KafkaError as e:
            # Retry with backoff interval
            logging.error("Problem communicating with Kafka (%s), retrying in %d seconds..." % (e, interval))
            time.sleep(interval)

    def _process_stream(self):
        sql_cache = {}
        'Consumer instance now has stream of events from the BinLog.'
        for message in self._consumer:
            self._cdc_event = json.loads(message.value)
            event_type = self._cdc_event["type"]
            'Invoke corresponding function for different event types :'
            ' INSERT/UPDATE/DELETE or schema changes'
            if event_type == "insert":
                sql_stmt = self._construct_insert_stmt()
            elif event_type == "update":
                sql_stmt = self._construct_update_stmt()
            elif event_type == "delete":
                sql_stmt = self._construct_delete_stmt()
            else:
                logging.basicConfig(
                    format='Unknown event type %s in BinLog entry' % event_type,
                    level=logging.INFO
                )
            sql_cache[self._cdc_event["ts"]] = sql_stmt
            print self._cdc_event["ts"]
        for stmt in sql_cache.items():
            print format(stmt)
        return sql_cache

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
        sql_stmt = "INSERT INTO %s (%s) VALUES (%s)" % (
            self._cdc_event["table"],
            self._keys,
            self._values
        )
        return sql_stmt

    def _construct_update_stmt(self):
        """
        Empty k/v pairs from previous iteration
        """
        col_bef = ""
        row_bef = ""
        col_aft = ""
        row_aft = ""

        'Construct SET clause in UPDATE statement '
        for k, v in self._cdc_event["data"].items():
            col_aft = " %s = \"%s\"" % (format(k), format(v))
            row_aft = "{0} {1},".format(row_aft, col_aft)
        'Strip trailing comma'
        stmt_set_clause = row_aft.strip()[:-1]

        'Construct sql for UPDATE statement '
        for k, v in self._cdc_event["old"].items():
            col_bef = " %s = \"%s\"" % (format(k), format(v))
            row_bef = "{0} {1} AND".format(row_bef, col_bef)
        'Strip trailing \"and\"'
        stmt_where_clause = row_bef.strip().rsplit(' ', 1)[0]

        sql_stmt = "UPDATE %s SET %s WHERE %s" % (
            self._cdc_event["table"],
            stmt_set_clause,
            stmt_where_clause
        )
        return sql_stmt

    def _construct_delete_stmt(self):
        'Empty k/v pairs from previous iteration'
        col = ""
        row = ""
        'Construct WHERE clause for DELETE statement '
        for k, v in self._cdc_event["data"].items():
            col = " %s = \"%s\"" % (format(k), format(v))
            row = "{0} {1} AND".format(row, col)

        'Strip trailing word \"and\" from row_str above before passing to SQL'
        sql_stmt = "DELETE FROM %s WHERE %s" % (
            self._cdc_event["table"],
            row.strip().rsplit(' ', 1)[0]
        )
        return sql_stmt
