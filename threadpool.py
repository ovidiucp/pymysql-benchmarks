# Thread pool to be used with Tornado.
#
# Author: Ovidiu Predescu
# Date: August 2011

import sys
import thread
from threading import Thread
from Queue import Queue, Empty
from functools import partial
import tornado.ioloop
import time

class ThreadPool:
    """Creates a thread pool containing `num_threads' worker threads.

    The caller can execute a task in a worker thread by invoking
    add_task(). The `func' argument will be executed by one of the
    worker threads as soon as one becomes available. If `func' needs
    to take any arguments, wrap the function using functools.partial.

    The caller has the option of specifying a callback to be invoked
    on the main thread's IOLoop instance. In a callback is specified
    it is passed as argument the return value of `func'.

    You can initialize per-thread state by setting the
    `per_thread_init_func'. This function is called before the worker
    threads are started and its return value is stored internally by
    each thread. This state is then passed as an optional argument to
    the `func' function using the `thread_state' named argument.

    Per-thread state is useful if you want to use the thread pool for
    database interaction. Create a database connection for each thread
    and store them as thread state.

    If you don't use per-thread state, you should define your worker
    function like this (add any other arguments when using
    functools.partial):

    def func(**kw):
        ...
        return some-result

    If you plan on using per-thread state, you could use the following
    prototype:

    def func(thread_state=None):
        ...
        return some-result

    To stop the worker threads in the thread pool use the stop()
    method.

    The queue_timeout parameter sets the time queue.get() waits for an
    object to appear in the queue. The default is 1 second, which is
    low enough for interactive usage. It should be lowered to maybe
    0.001 (1ms) to make unittests run fast, and increased when you
    expect the thread pool to be rarely stopped (like in a production
    environment).
    """

    def __init__(self,
                 per_thread_init_func=None,
                 per_thread_close_func=None,
                 num_threads=10,
                 queue_timeout=1,
                 ioloop=tornado.ioloop.IOLoop.instance()):
        self._ioloop = ioloop
        self._num_threads = num_threads
        self._queue = Queue()
        self._queue_timeout = queue_timeout
        self._threads = []
        self._running = True
        for i in xrange(num_threads):
            t = WorkerThread(self, per_thread_init_func, per_thread_close_func)
            t.start()
            self._threads.append(t)

    def add_task(self, func, callback=None):
        """Add a function to be invoked in a worker thread."""
        self._queue.put((func, callback))

    def stop(self):
        self._running = False
        map(lambda t: t.join(), self._threads)

class WorkerThread(Thread):
    def __init__(self, pool, per_thread_init_func, per_thread_close_func):
        Thread.__init__(self)
        self._pool = pool
        self._per_thread_init_func = per_thread_init_func
        self._per_thread_close_func = per_thread_close_func

    def run(self):
        if self._per_thread_init_func:
            thread_state = self._per_thread_init_func()
        else:
            thread_state = None
        queue = self._pool._queue
        queue_timeout = self._pool._queue_timeout
        while self._pool._running:
            try:
                (func, callback) = queue.get(True, queue_timeout)
                result = func(thread_state=thread_state)
                if callback:
                    self._pool._ioloop.add_callback(partial(callback, result))
            except Empty:
                pass
        if self._per_thread_close_func:
            self._per_thread_close_func(thread_state)
