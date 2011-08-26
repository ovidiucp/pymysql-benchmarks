# Author: Ovidiu Predescu
# Date: July 2011
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
"""
A simple benchmark for various asynchronous Python MySQL client libraries.
"""

import sys
import time

from adisp import process
import adb

import tornado.options
from tornado.options import define, options

define("dbhost", default="127.0.0.1", help="Database host")
define("dbuser", default="", help="Database user to use")
define("dbpasswd", default="", help="User's password")
define("db", default="test", help="Database to use")

define("use_tornado", default=False,
       help="Use tornado twisted reactor instead of twisted's reactor")

define("use_txmysql", default=False, help="Use txMySQL database module")
define("use_adbapi", default=False, help="Use twisted's adbapi module")
define("use_adb", default=False, help="Use our own adb module")

define("use_postgres", default=False, help="Use Postgres with psycopg2")
define("use_mysql", default=False, help="Use MySQL")

define("pool_size", default=10, help="Database connection pool size")

class DbBenchmark:
    def __init__(self):
        self._pool = None

    def run(self):
        d = self._createTable()
        d.addCallback(self._startBenchmark)
        d.addErrback(self._dbError)

    def _dbError(self, error):
        print "Error accessing the database: %s" % error

    def _createTable(self):
        print 'creating benchmark table'
        return self._pool.runOperation(
            """drop table if exists benchmark;
               create table benchmark(
                id INT NOT NULL PRIMARY KEY,
                data VARCHAR(100)
               )
            """)

    def _startBenchmark(self, ignored):
        print "Starting benchmark %s" % self.__class__
        self._start_time = time.time()
        self._totalNumInserts = 100000
        self._numDone = 0
        for i in xrange(self._totalNumInserts):
            d = self._doInsert(i)
            d.addCallback(self._insertDone)
            d.addErrback(self._dbError)

    def _doInsert(self, i):
        raise NotImplementedError

    def _insertDone(self, ignored):
        self._numDone += 1
        if self._numDone == self._totalNumInserts:
            d = self._pool.runQuery("select count(*) from benchmark")
            d.addCallback(self._allInsertsDone)
            d.addErrback(self._dbError)

    def _allInsertsDone(self, results):
        self._end_time = time.time()
        row = results[0]
        num = row[0]
        print "inserted %d records, time taken = %f seconds" %\
            (num, (self._end_time - self._start_time))
        reactor.stop()

class TwistedDbAPI(DbBenchmark):
    def __init__(self):
        from twisted.enterprise import adbapi
        print "Creating twisted adbapi ConnectionPool"
        self._pool = adbapi.ConnectionPool(dbapiName="MySQLdb",
                                           host=options.dbhost,
                                           port=3306,
                                           unix_socket='',
                                           user=options.dbuser,
                                           passwd=options.dbpasswd,
                                           db=options.db,
                                           cp_min=options.pool_size,
                                           cp_max=options.pool_size)
        self._numRuns = 0

    def _doInsert(self, i):
        return self._pool.runOperation(
            "insert benchmark (id, data) values (%s, %s)" % (i, i))


class TxMySQL(DbBenchmark):
    def __init__(self):
        from txmysql import client
        print "Creating txMySQL ConnectionPool"
        self._pool = client.ConnectionPool(hostname=options.dbhost,
                                           username=options.dbuser,
                                           password=options.dbpasswd,
                                           database=options.db,
                                           num_connections=options.pool_size)

    def _doInsert(self, i):
        return self._pool.runOperation(
            "insert benchmark(data) values (%d)" % i)

class TwistedDbPostgresAPI(DbBenchmark):
    def __init__(self):
        from twisted.enterprise import adbapi
        print "Creating twisted adbapi ConnectionPool"
        self._pool = adbapi.ConnectionPool(dbapiName="psycopg2",
                                           host=options.dbhost,
                                           user=options.dbuser,
                                           password=options.dbpasswd,
                                           database=options.db,
                                           cp_min=options.pool_size,
                                           cp_max=options.pool_size)
        self._numRuns = 0

    def _doInsert(self, i):
        return self._pool.runOperation(
            "insert into benchmark (id, data) values (%s, %s)" % (i, i))


class AsyncDatabaseBenchmark:
    def __init__(self, driver,
                 database=None, user=None, password=None, host=None):
        self.adb = adb.Database(driver=driver, database=database, user=user,
                                password=password, host=host,
                                num_threads=options.pool_size,
                                queue_timeout=0.1)

    def run(self):
        self.ioloop = tornado.ioloop.IOLoop.instance()
        self.ioloop.add_callback(self.whenRunningCallback)

    @process
    def whenRunningCallback(self):
        # Drop the table if it exists
        yield self.adb.runOperation("drop table if exists benchmark")
        yield self.adb.runOperation("""
          create table benchmark (
            userid int not null primary key,
            data VARCHAR(100)
          );
        """)
        rows_to_insert = 100000
        # Insert some rows
        start_time = time.time()
        stmts = []
        for i in xrange(rows_to_insert):
            stmts.append(
                ("insert into benchmark (userid, data) values (%s, %s)",
                 (i, i)))
        numrows = yield map(self.adb.runOperation, stmts)
        end_time = time.time()

        rows = yield self.adb.runQuery("select count(*) from benchmark")
        print 'inserted %s records, time taken = %s seconds' % \
            (rows, end_time - start_time)
        self.ioloop.stop()
        self.adb.stop()

if __name__ == "__main__":
    tornado.options.parse_command_line()
    if options.use_tornado:
        import tornado.platform.twisted
        tornado.platform.twisted.install()
    from twisted.internet import reactor

    benchmark = None
    if options.use_adbapi:
        if options.use_postgres:
            benchmark = TwistedDbPostgresAPI()
        elif options.use_mysql:
            benchmark = TwistedDbAPI()
    elif options.use_txmysql:
        # This only works with MySQL
        benchmark = TxMySQL()
    elif options.use_adb:
        if options.use_postgres:
            driver = "psycopg2"
        elif options.use_mysql:
            driver = "MySQLdb"
        else:
            print 'Use --use_postgres or --use_mysql to specify database.'
            sys.exit(1)
        benchmark = AsyncDatabaseBenchmark(driver,
                                           database=options.db,
                                           user=options.dbuser,
                                           password=options.dbpasswd,
                                           host=options.dbhost)

    if benchmark:
        benchmark.run()
    else:
        print 'Could not find a useful combination of options'
        print 'At least one of --use_adbapi or --use_txmysql should be '\
            'specified'
        sys.exit(1)

    if options.use_adb:
        print "Using Tornado IOLoop directly"
        tornado.ioloop.IOLoop.instance().start()
    else:
        reactor.suggestThreadPoolSize(options.pool_size)
        print "Using reactor %s" % reactor
        reactor.run()

