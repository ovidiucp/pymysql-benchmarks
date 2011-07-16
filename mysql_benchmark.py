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
import time

import tornado.options
from tornado.options import define, options

define("mysql_host", default="127.0.0.1", help="Database host")
define("mysql_user", default="", help="MySQL user to use")
define("mysql_passwd", default="", help="MySQL password")
define("mysql_db", default="test", help="MySQL database to use")

define("use_tornado", default=False,
       help="Use tornado twisted reactor instead of twisted's reactor")

define("use_txmysql", default=False, help="Use txMySQL database module")
define("use_adbapi", default=False, help="Use twisted's adbapi module")

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
                id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
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
                                           host=options.mysql_host,
                                           port=3306,
                                           unix_socket='',
                                           user=options.mysql_user,
                                           passwd=options.mysql_passwd,
                                           db=options.mysql_db,
                                           cp_min=options.pool_size,
                                           cp_max=options.pool_size)
        self._numRuns = 0

    def _doInsert(self, i):
        return self._pool.runOperation(
            "insert benchmark(data) values (%d)" % i)


class TxMySQL(DbBenchmark):
    def __init__(self):
        from txmysql import client
        print "Creating txMySQL ConnectionPool"
        self._pool = client.ConnectionPool(hostname=options.mysql_host,
                                           username=options.mysql_user,
                                           password=options.mysql_passwd,
                                           database=options.mysql_db,
                                           num_connections=options.pool_size)

    def _doInsert(self, i):
        return self._pool.runOperation(
            "insert benchmark(data) values (%d)" % i)


if __name__ == "__main__":
    tornado.options.parse_command_line()
    if options.use_tornado:
        import tornado.platform.twistedreactor
        tornado.platform.twistedreactor.install()
    from twisted.internet import reactor
    print "Using reactor %s" % reactor

    reactor.suggestThreadPoolSize(options.pool_size)

    if options.use_adbapi:
        benchmark = TwistedDbAPI()
        benchmark.run()
    elif options.use_txmysql:
        benchmark = TxMySQL()
        benchmark.run()
    else:
        print 'At least one of --use_adbapi or --use_txmysql should be '\
            'specified'
    reactor.run()
