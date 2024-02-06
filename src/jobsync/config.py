import os

from libb import Setting

HERE = os.path.abspath(os.path.dirname(__file__))

Setting.unlock()

sync = Setting()
sync.sql.appname = os.getenv('SYNC_SQL_APPNAME', 'sync_')
sync.sql.profile = os.getenv('SYNC_SQL_PROFILE', 'postgres')
sync.sql.timezone = os.getenv('SYNC_SQL_TIMEZONE', 'US/Eastern')
sync.sql.host = os.getenv('SYNC_SQL_HOST', 'localhost')
sync.sql.dbname = os.getenv('SYNC_SQL_DATABASE', 'jobsync')
sync.sql.user = os.getenv('SYNC_SQL_USERNAME', 'postgres')
sync.sql.passwd = os.getenv('SYNC_SQL_PASSWORD', 'postgres')
sync.sql.port = os.getenv('SYNC_SQL_PORT', 5432)

Setting.lock()

if __name__ == '__main__':
    __import__('doctest').testmod()
