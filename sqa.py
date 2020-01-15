from contextlib import contextmanager

import sqlalchemy.engine
from sqlalchemy import create_engine, exc, inspect, text
from sqlalchemy.sql.expression import TextClause

from .records import Record, RecordCollection


class Database:
    """A Database. Encapsulates a url and an SQLAlchemy engine with a pool of
    connections.
    """

    def __init__(self, db_url, pool_size=5, max_overflow=10,
                 pool_recycle=3600, pool_pre_ping=False,
                 encoding='utf-8', echo=False, **kwargs):

        self.db_url = db_url
        if not self.db_url:
            raise ValueError('You must provide a db_url.')

        # Create an engine.
        self._engine = create_engine(
            self.db_url,
            pool_recycle=pool_recycle,
            pool_size=pool_size,
            max_overflow=max_overflow,
            pool_pre_ping=pool_pre_ping,
            encoding=encoding,
            echo=echo,
            **kwargs
        )
        self._engine.connect()
        self.open = True

    def close(self):
        """Closes the Database."""
        self._engine.dispose()
        self.open = False

    def __enter__(self):
        return self

    def __exit__(self, exc, val, traceback):
        self.close()

    def __repr__(self):
        return '<Database[] open={}>'.format(self.db_url, self.open)

    def get_table_names(self, internal=False):
        """Returns a list of table names for the connected database."""

        # Setup SQLAlchemy for Database inspection.
        return inspect(self._engine).get_table_names()

    def get_connection(self):
        """Get a connection to this Database. Connections are retrieved from a
        pool.
        """
        if not self.open:
            raise exc.ResourceClosedError('Database closed.')

        return Connection(self._engine.connect())

    def query(self, query, *multiparams, **params):
        """Executes the given SQL query against the Database. Parameters can,
        optionally, be provided. Returns a RecordCollection, which can be
        iterated over to get result rows as dictionaries.
        """
        with self.get_connection() as conn:
            return conn.query(query, *multiparams, **params)

    def bulk_query(self, query, *multiparams):
        """Bulk insert or update."""

        with self.get_connection() as conn:
            conn.bulk_query(query, *multiparams)

    insertmany = bulk_query

    def get(self, query, *multiparams, **params):
        with self.get_connection() as conn:
            return conn.get(query, *multiparams, **params)

    def insert(self, query, *multiparams, **params):
        with self.get_connection() as conn:
            return conn.insert(query, *multiparams, **params)

    def update(self, query, *multiparams, **params):
        with self.get_connection() as conn:
            return conn.update(query, *multiparams, **params)

    def delete(self, query, *multiparams, **params):
        with self.get_connection() as conn:
            return conn.delete(query, *multiparams, **params)

    @contextmanager
    def transaction(self):
        """A context manager for executing a transaction on this Database."""

        conn = self.get_connection()
        tx = conn.transaction()
        try:
            yield conn
            tx.commit()
        except:
            tx.rollback()
        finally:
            conn.close()


class Connection:
    """A Database connection."""

    def __init__(self, connection: sqlalchemy.engine.Connection):
        self._conn = connection
        self.open = not connection.closed

    def close(self):
        self._conn.close()
        self.open = False

    def __enter__(self):
        return self

    def __exit__(self, exc, val, traceback):
        self.close()

    def __repr__(self):
        return '<Connection open={}>'.format(self.open)

    def query(self, query, *multiparams, **params):
        """Executes the given SQL query against the connected Database.
        Parameters can, optionally, be provided. Returns a RecordCollection,
        which can be iterated over to get result rows as dictionaries.
        """

        # Execute the given query.
        result_proxy = self.execute(query, *multiparams, **params)
        # Row-by-row Record generator.
        row_gen = (Record(result_proxy.keys(), row) for row in result_proxy)
        # Convert psycopg2 results to RecordCollection.
        results = RecordCollection(row_gen)
        return results

    def bulk_query(self, query, *multiparams):
        """Bulk insert or update."""

        if multiparams and self._has_bind_params(query):
            query = text(query)

        self._conn.execute(query, *multiparams)

    def scalar(self, query, *multiparams, **params):
        if params:
            query = text(query)
        return self._conn.scalar(query, *multiparams, **params)

    def _has_bind_params(self, query):
        return bool(TextClause._bind_params_regex.search(query))

    def execute(self, query, *multiparams, **params):
        if (params or multiparams) and self._has_bind_params(query):
            query = text(query)
        return self._conn.execute(query, *multiparams, **params)

    def execute_lastrowid(self, query, *multiparams, **params):
        result_proxy = self.execute(query, *multiparams, **params)
        return result_proxy.lastrowid

    def execute_rowcount(self, query, *multiparams, **params):
        result_proxy = self.execute(query, *multiparams, **params)
        return result_proxy.rowcount

    insert = execute_lastrowid
    update = delete = execute_rowcount
    insertmany = bulk_query

    def get(self, query, *multiparams, **params):
        """Returns the (singular) row returned by the given query.
        If the query has no results, returns None.  If it has
        more than one result, raises an exception.
        """
        result_proxy = self.execute(query, *multiparams, **params)
        if result_proxy.rowcount > 1:
            raise ValueError('Result contained more than one row.')
        row = result_proxy.first()
        if row is None:
            return
        return Record(row.keys(), row.values())

    def transaction(self):
        """Returns a transaction object. Call ``commit`` or ``rollback``
        on the returned object as appropriate."""

        return self._conn.begin()
