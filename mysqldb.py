import copy
import logging
import time
import pprint
from contextlib import contextmanager

try:
    import MySQLdb
except ImportError:
    import pymysql
    pymysql.install_as_MySQLdb()

import MySQLdb.constants
import MySQLdb.converters
import MySQLdb.cursors

logger = logging.getLogger(__name__)


class Connection:
    """A lightweight wrapper around MySQLdb DB-API connections.
    """

    def __init__(
        self,
        host,
        database,
        user=None,
        password=None,
        max_idle_time=7 * 3600,
        connect_timeout=0,
        time_zone="+0:00",
        charset="utf8",
        sql_mode="TRADITIONAL",
        **kwargs
    ):
        self.host = host
        self.database = database
        self.max_idle_time = float(max_idle_time)

        args = dict(
            conv=CONVERSIONS,
            use_unicode=True,
            charset=charset,
            db=database,
            init_command=('SET time_zone = "%s"' % time_zone),
            connect_timeout=connect_timeout,
            sql_mode=sql_mode,
            **kwargs
        )
        if user is not None:
            args["user"] = user
        if password is not None:
            args["passwd"] = password

        pair = host.split(":")
        if len(pair) == 2:
            args["host"] = pair[0]
            args["port"] = int(pair[1])
        else:
            args["host"] = host
            args["port"] = 3306

        self._db = None
        self._db_args = args
        self._last_use_time = time.time()
        try:
            self.reconnect()
        except Exception:
            logger.error("Cannot connect to MySQL on %s", self.host, exc_info=True)

    def __del__(self):
        self.close()

    def close(self):
        """Closes this database connection."""
        if self._db is not None:
            self._db.close()
            self._db = None

    def ping(self, reconnect=True):
        """ Check if the server is alive.
        """
        if self._db is None:
            if reconnect:
                self.reconnect()
                reconnect = False
            else:
                raise MySQLdb.Error("Already closed")
        try:
            self._db.ping()
        except Exception:
            if reconnect:
                self.reconnect()
                self.ping(False)
            else:
                raise

    def reconnect(self):
        """Closes the existing database connection and re-opens it."""
        self.close()
        self._db = MySQLdb.connect(**self._db_args)

    def iter(self, query, *params, **kwparams):
        """Returns an iterator for the given query and parameters."""
        self._ensure_connected()
        cursor = MySQLdb.cursors.SSCursor(self._db)
        try:
            self._execute(cursor, query, params, kwparams)
            column_names = [d[0] for d in cursor.description]
            for row in cursor:
                yield Row(zip(column_names, row))
        finally:
            cursor.close()

    def query(self, query, *params, **kwparams):
        """Returns a row list for the given query and parameters."""
        cursor = self._cursor()
        try:
            self._execute(cursor, query, params, kwparams)
            column_names = [d[0] for d in cursor.description]
            return [Row(zip(column_names, row)) for row in cursor]
        finally:
            cursor.close()

    def get(self, query, *params, **kwparams):
        """Returns the (singular) row returned by the given query.
        If the query has no results, returns None.  If it has
        more than one result, raises an exception.
        """
        rows = self.query(query, *params, **kwparams)
        if not rows:
            return None
        elif len(rows) > 1:
            raise Exception("Multiple rows returned for Database.get() query")
        else:
            return rows[0]

    # rowcount is a more reasonable default return value than lastrowid,
    # but for historical compatibility execute() must return lastrowid.
    def execute(self, query, *params, **kwparams):
        """Executes the given query, returning the lastrowid from the query."""
        return self.execute_lastrowid(query, *params, **kwparams)

    def execute_lastrowid(self, query, *params, **kwparams):
        """Executes the given query, returning the lastrowid from the query."""
        cursor = self._cursor()
        try:
            self._execute(cursor, query, params, kwparams)
            return cursor.lastrowid
        finally:
            cursor.close()

    def execute_rowcount(self, query, *params, **kwparams):
        """Executes the given query, returning the rowcount from the query."""
        cursor = self._cursor()
        try:
            self._execute(cursor, query, params, kwparams)
            return cursor.rowcount
        finally:
            cursor.close()

    def executemany(self, query, params):
        """Executes the given query against all the given param sequences.
        We return the lastrowid from the query.
        """
        return self.executemany_lastrowid(query, params)

    def executemany_lastrowid(self, query, params):
        """Executes the given query against all the given param sequences.
        We return the lastrowid from the query.
        """
        cursor = self._cursor()
        try:
            cursor.executemany(query, params)
            return cursor.lastrowid
        finally:
            cursor.close()

    def executemany_rowcount(self, query, params):
        """Executes the given query against all the given param sequences.
        We return the rowcount from the query.
        """
        cursor = self._cursor()
        try:
            cursor.executemany(query, params)
            return cursor.rowcount
        finally:
            cursor.close()

    update = delete = execute_rowcount
    updatemany = executemany_rowcount

    insert = execute_lastrowid
    insertmany = executemany_lastrowid

    def _ensure_connected(self):
        # Mysql by default closes client connections that are idle for
        # 8 hours, but the client library does not report this fact until
        # you try to perform a query and it fails.  Protect against this
        # case by preemptively closing and reopening the connection
        # if it has been idle for too long (7 hours by default).
        if self._db is None or (time.time() - self._last_use_time > self.max_idle_time):
            self.reconnect()
        self._last_use_time = time.time()

    def _cursor(self):
        self._ensure_connected()
        return self._db.cursor()

    def _execute(self, cursor, query, params, kwparams):
        try:
            return cursor.execute(query, kwparams or params)
        except OperationalError:
            logging.error("Error connecting to MySQL on %s", self.host)
            self.close()
            raise

    @contextmanager
    def transaction(self):
        """A context manager for executing a transaction on this Database."""
        self._db.begin()
        try:
            yield self
            self._db.commit()
        except Exception:
            self._db.rollback()
            raise


class Row(dict):
    """A dict that allows for object-like property access syntax."""

    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            raise AttributeError(name)

    def __repr__(self):
        return "<Row({})>".format(pprint.pformat(self, indent=2))


if MySQLdb is not None:
    # Fix the access conversions to properly recognize unicode/binary
    FIELD_TYPE = MySQLdb.constants.FIELD_TYPE
    FLAG = MySQLdb.constants.FLAG
    CONVERSIONS = copy.copy(MySQLdb.converters.conversions)

    field_types = [FIELD_TYPE.BLOB, FIELD_TYPE.STRING, FIELD_TYPE.VAR_STRING]
    if "VARCHAR" in vars(FIELD_TYPE):
        field_types.append(FIELD_TYPE.VARCHAR)

    for field_type in field_types:
        CONVERSIONS[field_type] = [(FLAG.BINARY, str)] + CONVERSIONS[field_type]

    # Alias some common MySQL exceptions
    IntegrityError = MySQLdb.IntegrityError
    OperationalError = MySQLdb.OperationalError
