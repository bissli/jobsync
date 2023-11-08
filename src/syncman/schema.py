from syncman import config, db

Node = f'{config.sql.appname}node'
Sync = f'{config.sql.appname}sync'
Audit = f'{config.sql.appname}audit'


def create_tables(cn):
    sqls = [f"""
create table if not exists {Node} (
    name varchar(255),
    created timestamp without time zone,
    primary key(name, created)
)
    """,f"""
create table if not exists {Sync} (
    node varchar(255) not null,
    item varchar(255) not null,
    primary key (item)
)
    """,f"""
create table if not exists {Audit} (
    created timestamp without time zone not null,
    node varchar(255) not null,
    item varchar(255) not null,
    date date not null
)
    """]
    for sql in sqls:
        db.execute(cn, sql)
