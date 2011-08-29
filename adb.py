# Asynchronous database interface for Tornado with transaction support.
#
# Author: Ovidiu Predescu
# Date: August 2011

from functools import partial
import psycopg2
from collections import deque

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
                 tx_connection_pool_size=5,
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
            per_thread_close_func=self.close_connection,
            num_threads=num_threads,
            queue_timeout=queue_timeout)
        self._ioloop = ioloop

        # Connection pool for transactions
        self._connection_pool = []
        for i in xrange(tx_connection_pool_size):
            conn = self.create_connection()
            self._connection_pool.append(conn)
        self._waiting_on_connection = deque()

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

    def close_connection(self, conn):
        conn.close()

    def stop(self):
        self._threadpool.stop()
        for conn in self._connection_pool:
            conn.close()

    @async
    def beginTransaction(self, callback):
        """Begins a transaction. Picks up a transaction from the pool
        and passes it to the callback. If none is available, adds the
        callback to `_waiting_on_connection'.
        """
        if self._connection_pool:
            conn = self._connection_pool.pop()
            callback(conn)
        else:
            self._waiting_on_connection.append(callback)

    @async
    def commitTransaction(self, connection, callback):
        self._threadpool.add_task(
            partial(self._commitTransaction, connection, callback))

    def _commitTransaction(self, conn, callback, thread_state=None):
        """Invoked in a worker thread.
        """
        conn.commit()
        self._ioloop.add_callback(
            partial(self._releaseConnectionInvokeCallback, conn, callback))

    @async
    def rollbackTransaction(self, connection, callback):
        self._threadpool.add_task(
            partial(self._rollbackTransaction, connection, callback))

    def _rollbackTransaction(self, conn, callback, thread_state=None):
        """Invoked in a worker thread.
        """
        conn.rollback()
        self._ioloop.add_callback(
            partial(self._releaseConnectionInvokeCallback, conn, callback))

    def _releaseConnectionInvokeCallback(self, conn, callback):
        """Release the connection back in the connection pool and
        invoke the callback. Invokes any waiting callbacks before
        releasing the connection into the pool.
        """
        # First invoke the callback to let the program know we're done
        # with the transaction.
        callback(conn)
        # Now check to see if we have any pending clients. If so pass
        # them the newly released connection.
        if self._waiting_on_connection:
            callback = self._waiting_on_connection.popleft()
            callback(conn)
        else:
            self._connection_pool.append(conn)

    @async
    def runQuery(self, query, args=None, conn=None, callback=None):
        """Send a SELECT query to the database.

        The callback is invoked with all the rows in the result.
        """
        self._threadpool.add_task(
            partial(self._query, query, args, conn), callback)

    def _query(self, query, args, conn=None, thread_state=None):
        """This method is called in a worker thread.

        Execute the query and return the result so it can be passed as
        argument to the callback.
        """
        if not conn:
            conn = thread_state
        cursor = conn.cursor()
        cursor.execute(query, args)
        rows = cursor.fetchall()
        cursor.close()
        return rows

    @async
    def runOperation(self, stmt, args=None, conn=None, callback=None):
        """Execute a SQL statement other than a SELECT.

        The statement is committed immediately. The number of rows
        affected by the statement is passed as argument to the
        callback.
        """
        self._threadpool.add_task(
            partial(self._execute, stmt, args, conn), callback)

    def _execute(self, stmt, args, conn=None, thread_state=None):
        """This method is called in a worker thread.

        Executes the statement.
        """
        # Check if stmt is a tuple. This can happen when we use map()
        # with adisp to execute multiple statements in parallel.
        if isinstance(stmt, tuple):
            args = stmt[1]
            stmt = stmt[0]
        if not conn:
            conn = thread_state
            should_commit = True
        else:
            should_commit = False
        cursor = conn.cursor()
        cursor.execute(stmt, args)
        if should_commit:
            conn.commit()
        rowcount = cursor.rowcount
        cursor.close()
        return rowcount
