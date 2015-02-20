#!/usr/bin/env python

import sys
import psycopg2
import pprint
from Logger import Logger

class Data_Store(object):

    def __init__(self, db_info, log_dir=None):
        self.__db_info = db_info
        self._logger = utils.get_logger('DataStore', log_dir)
        self._conn()

    def _conn(self):
        """ Connect to the DB
        """
        if self.__db_info['password'] is None:
            conn_str = "dbname='{dbname}' user='{user}' host='{host}'".format(**self.__db_info)
        else:
            conn_str = "dbname='{dbname}' user='{user}' host='{host}' password='{password}'".format(**self.__db_info)

        try:
            self._logger.info('Establing connection to "{dbname}" on "{host}" as "{user}" user'.format(**self.__db_info))
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
            return (True, self._cursor)
        except Exception as e:
            self._logger.error('Error: %s' % e)
            self._logger.info('Error encountered, program interrupted')
            return (False, None)

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
            return True
        except Exception as e:
            self._logger.error('Error: %s' % e)
            self._dbh.rollback()
            return False


if __name__ == "__main__":
    pass


