import threading
import queue

import pymysql

from .connection import Connection

from mysql.connector import MySQLConnection
from mysql.connector.pooling import MySQLConnectionPool

CONNECTION_POOL_LOCK = threading.RLock()
CNX_POOL_MAXSIZE = 32


class PoolError(pymysql.Error):
    pass


class Pool(object):

    def __init__(self, size=5, pool_reset_session=True, **kwargs):
        self._pool_size = None
        self._reset_session = pool_reset_session
        self._set_pool_size(size)
        self._cnx_config = {}
        self._cnx_queue = queue.Queue(self._pool_size)

        if kwargs:
            self.set_config(**kwargs)
            cnt = 0
            while cnt < self._pool_size:
                self.add_connection()
                cnt += 1

    @property
    def size(self):
        """Return number of connections managed by the pool"""
        return self._pool_size

    @property
    def reset_session(self):
        """Return whether to reset session"""
        return self._reset_session

    def _set_pool_size(self, size):
        if size <= 0 or size > CNX_POOL_MAXSIZE:
            raise AttributeError(
                "Pool size should be higher than 0 and "
                "lower or equal to {0}".format(CNX_POOL_MAXSIZE))
        self._pool_size = size

    def set_config(self, **kwargs):
        """Set the connection configuration for Connection instances
        """
        if not kwargs:
            return

        with CONNECTION_POOL_LOCK:
            try:
                test_cnx = Connection(**kwargs)
                self._cnx_config = kwargs
            except AttributeError as err:
                raise PoolError("Connection configuration not valid: {0}".format(err))

    def _queue_connection(self, cnx):
        """Put connection back in the queue
        """
        if not isinstance(cnx, Connection):
            raise PoolError("Connection instance not subclass of Connection.")

        try:
            self._cnx_queue.put(cnx, block=False)
        except queue.Full:
            PoolError("Failed adding connection; queue is full")

    def add_connection(self, cnx=None):
        """Add a connection to the pool
        """
        with CONNECTION_POOL_LOCK:
            if not self._cnx_config:
                raise PoolError("Connection configuration not available")

            if self._cnx_queue.full():
                raise PoolError("Failed adding connection; queue is full")

            if not cnx:
                cnx = Connection(**self._cnx_config)
            else:
                if not isinstance(cnx, Connection):
                    raise PoolError("Connection instance not subclass of Connection.")

            self._queue_connection(cnx)

    def get_connection(self):
        """Get a connection from the pool
        """
        with CONNECTION_POOL_LOCK:
            try:
                cnx = self._cnx_queue.get(block=False)
            except queue.Empty:
                raise PoolError("Failed getting connection; pool exhausted")

            return cnx

    def _remove_connections(self):
        """Close all connections
        """
        with CONNECTION_POOL_LOCK:
            cnt = 0
            cnxq = self._cnx_queue
            while cnxq.qsize():
                try:
                    cnx = cnxq.get(block=False)
                    cnx.close()
                    cnt += 1
                except queue.Empty:
                    return cnt
                except PoolError:
                    raise
                except pymysql.Error:
                    pass

            return cnt

    def dispose(self):
        self._remove_connections()
