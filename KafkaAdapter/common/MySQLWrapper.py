# This is a MySQL Wrapper Python script.
"""
"""

#import CommonLogger
#import ConfigManager
#import time
import sys
import pymysql.cursors

class MySQLWrapper:

    _commit_timeout = 60
    _transaction_timeout = 60
    _logger = None

    def __init__(self):

        self._delivered_records = 0
        self._rollback_records = 0
        self._connection = 0

    def __del__(self):
        #myfunc = sys._getframe().f_code.co_name
        #self._logger.info(f"[{myfunc}] MySQLWrapper del")
        #del self._connection
        if self._connection:
            pass
            #self.db_close()
            #self._connection = 0


    def set_logger(self, logger):
        self._logger = logger

    def db_connect(self, db_connection_info, auto_commit=False):
        """
        MySQL connection function.
        """
        myfunc = sys._getframe().f_code.co_name
        try:
            self._logger.debug(f"[{myfunc}] MySQL connection start")

            self._connection = pymysql.connect(host=db_connection_info['host'],
                                         user=db_connection_info['user'],
                                         password=db_connection_info['password'],
                                         database=db_connection_info['database'],
                                         charset=db_connection_info['charset'],
                                         cursorclass=pymysql.cursors.DictCursor)

        except Exception as err:
            self._logger.error(f"[{myfunc}] MySQL connection error. err: ({err}), config:({db_connection_info})")
            raise
        else:
            self._logger.info(f"[{myfunc}] MySQL connected. config:({db_connection_info['host']})")

    def db_select(self, fetchtype, sql):
        try:
            myfunc = sys._getframe().f_code.co_name
            with self._connection.cursor() as cursor:
                self._logger.debug(f"[{myfunc}] SQL:[{sql}]")
                cursor.execute(sql)
                if fetchtype == "all":
                    result = cursor.fetchall()
                else:
                    result = cursor.fetchone()
                return result
        except Exception as err:
            cursor.close()
            self._logger.error(f"[{myfunc}] MySQL db_select error. err: ({err}), sql:({sql})")
            raise


    def db_insert(self, datafrm, table, option="delete"):
        try:
            myfunc = sys._getframe().f_code.co_name
            # Creating a list of tupples from the dataframe values
            tpls = [tuple(x) for x in datafrm.to_numpy()]

            # dataframe columns with Comma-separated
            cols = ','.join(list(datafrm.columns))
            params = "%s" + ",%s" * (len(datafrm.columns) - 1)
            conn = self._connection

            self._logger.info(f"[{myfunc}] DB insert start. table({table}) columns({len(datafrm.columns)}) rows({len(datafrm)})")

            # SQL query to execute
            sql = "INSERT INTO %s(%s) VALUES(%s)" % (table, cols, params)
            cursor = conn.cursor()
            try:
                #truncate_sql = "truncate table %s" % table
                #self._logger.info(f"[{myfunc}] SQL:{truncate_sql}")
                #cursor.execute(truncate_sql)
                #self._logger.info(f"[{myfunc}] Table truncate success. table({table})")
                if option == "delete":
                    delete_sql = "delete from %s" % table
                    self._logger.info(f"[{myfunc}] SQL:{delete_sql}")
                    cursor.execute(delete_sql)
                    self._logger.info(f"[{myfunc}] Table delete_sql success. table({table})")

                self._logger.info(f"[{myfunc}] SQL:{sql}")
                cursor.executemany(sql, tpls)
                self._logger.info(f"[{myfunc}] DB insert success. table({table})")
                cursor.close()
            except Exception as err:
                self._logger.info(f"[{myfunc}] DB insert execute error. table({table}) err:{err}")
                cursor.close()
                raise
        except Exception as err:
            self._logger.info(f"[{myfunc}] DB insert error. table({table}) err:{err}")
            raise

    def db_execute(self, sql):
        try:
            myfunc = sys._getframe().f_code.co_name
            with self._connection.cursor() as cursor:
                self._logger.debug(f"[{myfunc}] SQL:[{sql}]")
                cursor.execute(sql)
                cursor.close()
        except Exception as err:
            cursor.close()
            self._logger.error(f"[{myfunc}] MySQL db_execute error. err: ({err}), sql:({sql})")
            raise

    def db_commit(self):
        try:
            myfunc = sys._getframe().f_code.co_name
            self._connection.commit()
        except Exception as err:
            self._logger.error(f"[{myfunc}] MySQL db_commit error. err: ({err})")
            raise

    def db_rollback(self):
        try:
            myfunc = sys._getframe().f_code.co_name
            self._connection.rollback()

        except Exception as err:
            self._logger.error(f"[{myfunc}] MySQL db_rollback error. err: ({err})")
            raise

    def db_close(self):
        myfunc = sys._getframe().f_code.co_name
        if self._connection:
            self._connection.close()
            self._connection = 0
            self._logger.info(f"[{myfunc}] MySQL disconnected.")



