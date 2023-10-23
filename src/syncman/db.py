import datetime
import logging
import os
import urllib.parse
from dataclasses import dataclass, fields

import pytz
import sqlalchemy as sa
from piny import StrictMatcher, YamlLoader

logger = logging.getLogger('syncman')


@dataclass
class Agent:
    name: str
    agent: str
    created: str


@dataclass
class Claim:
    name: str
    agent: str
    claim: str


@dataclass
class Audit:
    name: str
    created: str
    agent: str
    claim: str
    date: str


def from_dict(class_name, **kwargs):
    _fields = {f.name for f in fields(class_name) if f.init}
    return class_name(**{k: v for k, v in kwargs.items() if k in _fields})


def get_connection_url(config):
    _db = config['database']
    dbname = os.environ.get(_db['name'] or '', _db['name'])
    dbhost = os.environ.get(_db['host'] or '', _db['host'])
    dbuser = os.environ.get(_db['username'] or '', _db['username'])
    dbpass = urllib.parse.quote_plus(os.environ.get(_db['password'] or '', _db['password'] or ''))
    if _db['type'] == 'postgres':
        url = f'postgresql+psycopg2://{dbuser}:{dbpass}@{dbhost}:5432/{dbname}'
    else:
        url = f'sqlite:///{dbname}.db'
    return url


def create(config='config.yml'):
    """Config is either a yml file to load and process or a dictionary"""
    if isinstance(config, str):
        config = YamlLoader(path=config, matcher=StrictMatcher).load()
    agent = from_dict(Agent, **config['tables']['agent'])
    claim = from_dict(Claim, **config['tables']['claim'])
    audit = from_dict(Audit, **config['tables']['audit'])
    url = get_connection_url(config)
    engine = sa.create_engine(url, pool_size=1, echo=True)
    metadata = sa.MetaData()
    sa.Table(
        agent.name,
        metadata,
        sa.Column(agent.created, sa.TIMESTAMP(timezone=False), nullable=False, primary_key=True),
        sa.Column(agent.agent, sa.String(255), nullable=False, primary_key=True),
    )
    sa.Table(
        claim.name,
        metadata,
        sa.Column(claim.agent, sa.String(255), nullable=False),
        sa.Column(claim.claim, sa.String(255), nullable=False),
    )
    sa.Table(
        audit.name,
        metadata,
        sa.Column(audit.created, sa.TIMESTAMP(timezone=False), nullable=False, default=datetime.datetime.now()),
        sa.Column(audit.agent, sa.String(255), nullable=False),
        sa.Column(audit.claim, sa.String(255), nullable=False),
        sa.Column(audit.date, sa.DATE(), nullable=False),
    )
    metadata.create_all(engine)
    metadata.reflect(engine, only=[agent.name, claim.name, audit.name])
    return {
        'engine': engine,
        'timezone': pytz.timezone(config['database']['timezone'] or 'UTC'),
        'agent': (agent, metadata.tables[agent.name]),
        'claim': (claim, metadata.tables[claim.name]),
        'audit': (audit, metadata.tables[audit.name]),
    }
