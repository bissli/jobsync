import atexit
import logging
import time

from syncman import config

logger = logging.getLogger(__name__)

try:
    import psycopg
except ImportError:
    logger.warning('psycopg not available')

try:
    import sqlite3
except ImportError:
    logger.warning('sqlite not available')


class ConnectionWrapper:
    """Wraps a connection object so we can keep track of the
    calls and execution time of any cursors used by this connection.
    """

    def __init__(self, connection, cleanup=True):
        self.connection = connection
        self.calls = 0
        self.time = 0
        if cleanup:
            atexit.register(self.cleanup)

    def __getattr__(self, name):
        """Delegate any members to the underlying connection."""
        return getattr(self.connection, name)

    def cursor(self, *args, **kwargs):
        return CursorWrapper(self.connection.cursor(*args, **kwargs), self)

    def addcall(self, elapsed):
        self.time += elapsed
        self.calls += 1

    def cleanup(self):
        try:
            self.connection.close()
            logger.debug(f'Database connection lasted {self.time or 0} seconds, {self.calls or 0} queries')
        except:
            pass


class CursorWrapper:
    """Wraps a cursor object so we can keep track of the
    execute calls and time used by this cursor.
    """

    def __init__(self, cursor, connwrapper):
        self.cursor = cursor
        self.connwrapper = connwrapper

    def __getattr__(self, name):
        """Delegate any members to the underlying cursor."""
        return getattr(self.cursor, name)

    def execute(self, sql, *args, **kwargs):
        """Time the call and tell the connection wrapper that
        created this connection.
        """
        start = time.time()
        logger.debug('SQL:\n%s\nargs: %s\nkwargs: %s' % (sql, str(args), str(kwargs)))
        self.cursor.execute(sql, *args, **kwargs)
        end = time.time()
        self.connwrapper.addcall(end - start)
        logger.debug('Query time=%f' % (end - start))


def dumpsql(function):
    """This is a decorator for db module functions, for logging data flowing down to driver"""

    def wrapper(cn, sql, *args, **kwargs):
        try:
            return function(cn, sql, *args, **kwargs)
        except Exception as exc:
            logger.error('Error with query:\nSQL: {}\nARGS: {}\nKWARGS:{}'.format(sql, args, kwargs))
            logger.exception(exc)
            raise exc

    return wrapper


def connect(profile='postgres', **kw):
    timeout = kw.get('timeout') or 150
    if profile == 'postgres':
        _con = psycopg.connect(
            dbname=config.sql.database,
            host=config.sql.host,
            user=config.sql.user,
            port=config.sql.port,
            password=config.sql.passwd,
            )
    if profile == 'sqlite':
        _con = sqlite3.connect('database.db')

    return ConnectionWrapper(_con)


def _dict_cur(cn):
    if type(cn.connection) == psycopg.Connection:
        return cn.cursor(row_factory=psycopg.rows.dict_row)
    if type(cn.connection) == sqlite3.Connection:
        return cn.cursor()
    raise ValueError('Unknown connection type')


@dumpsql
def select(cn, sql, *args, **kwargs):
    cursor = _dict_cur(cn)
    cursor.execute(sql, args)
    return create_dataframe(cursor, **kwargs)


def create_dataframe(cursor, **kwargs):
    """Create a dataframe from the raw rows, column names and column types"""
    from pandas import DataFrame

    cols_typs = kwargs.pop('cols_typs', None)

    if type(cursor.connection) == psycopg.Connection:
        data = cursor.fetchall()
        if not data and cols_typs:
            data = [{k: v.__call__() for k, v in cols_typs.items()}]
        return DataFrame(data)
    if type(cursor.connection) == sqlite3.Connection:
        cols = [column[0] for column in cursor.description]
        data = cursor.fetchall()
        if not data and cols_typs:
            data = [{k: v.__call__() for k, v in cols_typs.items()}]
        return DataFrame.from_records(data=data, columns=cols)
    raise ValueError('Unknown connection type')


@dumpsql
def execute(cn, sql, *args):
    cursor = cn.cursor()
    cursor.execute(sql, args)
    rowcount = cursor.rowcount
    cn.commit()
    return rowcount


insert = update = delete = execute


class transaction:
    """Context manager for running multiple commands in a transaction.

    with db.transaction(cn) as tx:
        tx.execute('delete from ...', args)
        tx.execute('update from ...', args)
    """

    def __init__(self, cn):
        self.cn = cn

    def __enter__(self):
        self.cursor = _dict_cur(self.cn)
        return self

    def __exit__(self, exc_type, value, traceback):
        if exc_type is not None:
            self.cn.rollback()
            logger.warning('Rolling back the current transaction')
        else:
            self.cn.commit()
            logger.debug('Committed transaction.')

    @dumpsql
    def execute(self, sql, *args):
        self.cursor.execute(sql, args)
        return self.cursor.rowcount

    @dumpsql
    def select(self, sql, *args, **kwargs):
        cursor = self.cursor
        cursor.execute(sql, args)
        return create_dataframe(cursor, **kwargs)

    def select_scalar(self, cn, sql, *args):
        row = select_row(cn, sql, *args)
        if len(row) != 1:
            logger.error('fExpected one col, got {len(row)}')
            return 1
        return row[list(row.keys())[0]]


def select_row(cn, sql, *args):
    df = select(cn, sql, *args)
    assert len(df.index) == 1, 'Expected one row, got %d' % len(df.index)
    return df.iloc[0]


def select_scalar(cn, sql, *args):
    df = select(cn, sql, *args)
    assert len(df.index) == 1, 'Expected one row, got %d' % len(df.index)
    return df[df.columns[0]].iloc[0]


def select_scalar_or_none(cn, sql, *args):
    row = select_row_or_none(cn, sql, *args)
    if row:
        return row[list(row.keys())[0]]
    return None


class SQLiteAutoClose:
    """A context manager that automatically closes the cursor and the database.
    Return a cursor object upon entering.
    """

    def __init__(self, database):
        self.conn = sqlite3.connect(database)

    def __enter__(self):
        self.conn = self.conn.__enter__()
        self.cur = self.conn.cursor()
        return self.cur

    def __exit__(self, *exc_info):
        result = self.conn.__exit__(*exc_info)
        self.cur.close()
        self.conn.close()
        return result
