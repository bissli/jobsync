import logging

import sqlalchemy as sa

logger = logging.getLogger(__name__)


def create(db_connection: str, db_appname: str):
    engine = sa.create_engine(db_connection, pool_size=1)
    metadata = sa.MetaData()
    node, sync, audit = f'{db_appname}node', f'{db_appname}sync', f'{db_appname}audit'
    sa.Table(
        node,
        metadata,
        sa.Column('created', sa.TIMESTAMP(timezone=False), primary_key=True),
        sa.Column('name', sa.String(255), primary_key=True),
    )
    sa.Table(
        sync,
        metadata,
        sa.Column('node', sa.String(255), nullable=False),
        sa.Column('item', sa.String(255), nullable=False),
    )
    sa.Table(
        audit,
        metadata,
        sa.Column('created', sa.TIMESTAMP(timezone=False), nullable=False),
        sa.Column('node', sa.String(255), nullable=False),
        sa.Column('item', sa.String(255), nullable=False),
        sa.Column('date', sa.DATE(), nullable=False),
    )
    metadata.create_all(engine)
    metadata.reflect(engine, only=[node, sync, audit])
    return {
        'engine': engine,
        'node': metadata.tables[node],
        'sync': metadata.tables[sync],
        'audit': metadata.tables[audit],
    }
