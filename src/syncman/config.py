import os
import pathlib
import tempfile

HERE = os.path.abspath(os.path.dirname(__file__))


class Setting(dict):
    """Dict where d['foo'] can also be accessed as d.foo
    but also automatically creates new sub-attributes of
    type Setting. This behavior can be locked to turn off
    later. Used in config.py.
    WARNING: not copy safe

    >>> cfg = Setting()
    >>> cfg.unlock() # locked after config.py load

    >>> cfg.foo.bar = 1
    >>> hasattr(cfg.foo, 'bar')
    True
    >>> cfg.foo.bar
    1
    >>> cfg.lock()
    >>> cfg.foo.bar = 2
    Traceback (most recent call last):
     ...
    ValueError: This Setting object is locked from editing
    >>> cfg.foo.baz = 3
    Traceback (most recent call last):
     ...
    ValueError: This Setting object is locked from editing
    >>> cfg.unlock()
    >>> cfg.foo.baz = 3
    >>> cfg.foo.baz
    3
    """

    _locked = False

    def __init__(self, *args, **kwargs):
        dict.__init__(self, *args, **kwargs)

    def __getattr__(self, name):
        """Create sub-setting fields on the fly"""
        if name not in self:
            if self._locked:
                raise ValueError('This Setting object is locked from editing')
            self[name] = Setting()
        return self[name]

    def __setattr__(self, name, val):
        if self._locked:
            raise ValueError('This Setting object is locked from editing')
        elif name not in self:
            self[name] = Setting()
        self[name] = val

    @staticmethod
    def lock():
        Setting._locked = True

    @staticmethod
    def unlock():
        Setting._locked = False


tmpdir = Setting()
tmpdir.dir = tempfile.gettempdir()
pathlib.Path(tmpdir.dir).mkdir(parents=True, exist_ok=True)

sql = Setting()
sql.type = 'psycopg2+psycopg'
sql.appname = os.getenv('SYNC_DB_APPNAME', 'sync_')
sql.timezone = os.getenv('SYNC_DB_TIMEZONE', 'US/Eastern')
sql.host = os.getenv('SYNC_DB_HOST', 'localhost')
sql.database = os.getenv('SYNC_DB_DBNAME', 'syncman')
sql.user = os.getenv('SYNC_PD_USERNAME', 'postgres')
sql.passwd = os.getenv('SYNC_PD_PASSWORD', 'postgres')
sql.port = os.getenv('SYNC_DB_PORT', 5432)
