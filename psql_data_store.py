#!/usr/bin/env python

import sys
import psycopg2
import pprint
from Logger import Logger

class Data_Store(object):

    def __init__(self, db_host, db_name, db_port, db_user, db_pass=None, log_dir=None):

        self._db_host = db_host
        self._db_name = db_name
        self._db_port = db_port
        self._db_user = db_user
        self._db_pass = db_pass

        self._logger = Logger().get_logger('Data_Store', log_dir)
        self._conn()

    def _conn(self):
        """ Connect to the DB
        """
        if self._db_pass is None:
            conn_str = "dbname='%s' user='%s' host='%s'" % (self._db_name, self._db_user, self._db_host)
        else:
            conn_str = "dbname='%s' user='%s' host='%s' password='%s'" % (self._db_name, self._db_user, self._db_host, self._db_pass)

        try:
            self._logger.info('Establing connection to "%s" on "%s" as "%s" user' % (self._db_name, self._db_host, self._db_user))
            self._dbh = psycopg2.connect(conn_str)
            self._cursor = self._dbh.cursor()
            self._logger.info('Connection established.')
        except:
            exceptionType, exceptionValue, exceptionTraceback = sys.exc_info()
            exit("Database connection failed!\n ->%s" % (exceptionValue))

    def fetch_result(self):
        """
        Fetch the result stored in the psycopg DB handler cursor

        Returns:
            The result from the cursor, or None if no result is stored in the cursor
        """
        return self._cursor.fetchall()

    def execute(self, query):
        """
        Execute the qeury specified

        Args:
            query: The psql query to be executed in a string
        """
        try:
            self._logger.debug('Executing query: "%s"' % query)

            self._cursor.execute(query)
        except Exception as e:
            self._logger.error('Error: %s' % e)
            self._logger.info('Error encountered, program interrupted')
            sys.exit(1)

    def execute_and_commit(self, query):
        """
        Execute the qeury specified

        Args:
            query: The psql query to be executed in a string
        """
        try:
            self._logger.debug('Executing query: "%s"' % query)

            self._cursor.execute(query)
            self._dbh.commit()
        except Exception as e:
            self._logger.error('Error: %s' % e)
            self._logger.info('Error encountered, program interrupted')
            sys.exit(1)


if __name__ == "__main__":
    pass


