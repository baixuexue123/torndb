import time
from contextlib import contextmanager

import pymysql
import pymysql.cursors
from pymysql.connections import Connection


class PyMySQLConn(Connection):
    """ A lightweight wrapper around PyMySQL DB-API connections. """

    def __init__(self, host, db, user=None, password=None,
                 charset="utf8", time_zone="+8:00", sql_mode="TRADITIONAL",
                 health_check_interval=300, cursorclass=pymysql.cursors.DictCursor,
                 **kwargs):

        pair = host.split(":")
        if len(pair) == 2:
            kwargs["host"] = pair[0]
            kwargs["port"] = int(pair[1])
        else:
            kwargs["host"] = host
            kwargs["port"] = 3306

        self.health_check_interval = health_check_interval
        self.next_health_check = 0
        self.check_health()
        super(PyMySQLConn, self).__init__(db=db, user=user, passwd=password, charset=charset,
                                          init_command='SET time_zone = "%s"' % time_zone,
                                          sql_mode=sql_mode, cursorclass=cursorclass, **kwargs)

    def check_health(self):
        """"Check the health of the connection with a ping"""
        if self.health_check_interval and time.time() > self.next_health_check:
            self.ping(reconnect=True)
            self.next_health_check = time.time() + self.health_check_interval

    @contextmanager
    def transaction(self):
        """A context manager for executing a transaction on this Database."""
        self.begin()
        try:
            yield self
            self.commit()
        except Exception:
            self.rollback()
            raise

    def cursor(self, cursor=None):
        self.check_health()
        return super(PyMySQLConn, self).cursor(cursor=cursor)

    def iter(self, sql, args=None):
        """Returns an iterator for the given query and parameters."""
        cursor = self.cursor(pymysql.cursors.SSCursor)
        cursor.execute(sql, args=args)
        for row in cursor:
            yield row

    def query(self, query, args=None):
        """Returns a row list for the given query and parameters."""
        with self.cursor() as cursor:
            cursor.execute(query, args=args)
            return cursor.fetchall()

    def get(self, query, args):
        """Returns the (singular) row returned by the given query.

        If the query has no results, returns None.  If it has
        more than one result, raises an exception.
        """
        rows = self.query(query, args=args)
        if not rows:
            return None
        elif len(rows) > 1:
            raise Exception("Multiple rows returned for get() query")
        else:
            return rows[0]

    def execute_lastrowid(self, query, args=None):
        with self.cursor() as c:
            c.execute(query, args=args)
            lastrowid = c.lastrowid
            self.commit()
            return lastrowid

    def execute_rowcount(self, query, args=None):
        """Executes the given query, returning the rowcount from the query."""
        with self.cursor() as c:
            c.execute(query, args=args)
            rowcount = c.rowcount
            self.commit()
            return rowcount

    def executemany_rowcount(self, query, args):
        """Executes the given query against all the given param sequences.
        return the rowcount from the query.
        """
        with self.cursor() as cursor:
            cursor.executemany(query, args)
            rowcount = cursor.rowcount
            self.commit()
            return rowcount

    insert = execute_lastrowid
    update = delete = execute_rowcount
    updatemany = executemany_rowcount
    insertmany = executemany_rowcount
