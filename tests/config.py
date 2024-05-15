from libb import Setting

Setting.unlock()

postgres = Setting()
postgres.drivername = 'postgres'
postgres.database = 'jobsync'
postgres.hostname = 'localhost'
postgres.username = 'postgres'
postgres.password = 'postgres'
postgres.port = 5432
postgres.timeout = 30
postgres.check_connection = True
postgres.cleanup = True

sqlite = Setting()
sqlite.drivername = 'sqlite'
sqlite.database = 'database.db'
sqlite.check_connection = True
sqlite.cleanup = True

Setting.lock()
