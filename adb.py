# Asynchronous database interface for Tornado
#
# Author: Ovidiu Predescu
# Date: August 2011

from functools import partial
import psycopg2

import tornado.ioloop

from threadpool import ThreadPool
from adisp import process, async

class Database:
    """Asynchronous database interface.

    The `driver' argument specifies which database to use. Possible
    values are:

    MySQLdb - for MySQL
    psycopg2 - for Postgres
    """
    def __init__(self,
                 driver=None,
                 database=None, user=None, password=None,
                 host='localhost',
                 ioloop=tornado.ioloop.IOLoop.instance(),
                 num_threads=10,
                 queue_timeout=1):
        if not(driver):
            raise ValueError("Missing 'driver' argument")
        self._driver = driver
        self._database = database
        self._user = user
        self._password = password
        self._host = host
        self._threadpool = ThreadPool(
            per_thread_init_func=self.create_connection,
            num_threads=num_threads,
            queue_timeout=queue_timeout)
        self._ioloop = ioloop

    def create_connection(self):
        """This method is executed in a worker thread.

        Initializes the per-thread state. In this case we create one
        database connection per-thread.
        """
        if self._driver == "psycopg2":
            try:
                import psycopg2
                conn = psycopg2.connect(database=self._database,
                                        user=self._user,
                                        password=self._password,
                                        host=self._host)
            except Exception as ex:
                raise ex
        elif self._driver == "MySQLdb":
            try:
                import MySQLdb
                conn = MySQLdb.connect(db=self._database,
                                       user=self._user,
                                       passwd=self._password,
                                       host=self._host,
                                       port=3306)
            except Exception as ex:
                raise ex
        else:
            raise ValueError("Unknown driver %s" % self._driver)
        return conn

    def stop(self):
        self._threadpool.stop()

    @async
    def runQuery(self, query, args=None, callback=None):
        """Send a SELECT query to the database.

        The callback is invoked with all the rows in the result.
        """
        self._threadpool.add_task(partial(self._query, query, args), callback)

    def _query(self, query, args, thread_state=None):
        """This method is called in a worker thread.

        Execute the query and return the result so it can be passed as
        argument to the callback.
        """
        conn = thread_state
        cursor = conn.cursor()
        cursor.execute(query, args)
        rows = cursor.fetchall()
        cursor.close()
        return rows

    @async
    def runOperation(self, stmt, args=None, callback=None):
        """Execute a SQL statement other than a SELECT.

        The statement is committed immediately. The number of rows
        affected by the statement is passed as argument to the
        callback.
        """
        self._threadpool.add_task(partial(self._execute, stmt, args), callback)

    def _execute(self, stmt, args, thread_state=None):
        """This method is called in a worker thread.

        Executes the statement.
        """
        # Check if stmt is a tuple. This can happen when we use map()
        # with adisp to execute multiple statements in parallel.
        if isinstance(stmt, tuple):
            args = stmt[1]
            stmt = stmt[0]
        conn = thread_state
        cursor = conn.cursor()
        cursor.execute(stmt, args)
        conn.commit()
        rowcount = cursor.rowcount
        cursor.close()
        return rowcount
