import logging
import time
from contextlib import contextmanager

import sqlalchemy as sa
from sqlalchemy.orm import scoped_session, sessionmaker
from syncman import schema

logger = logging.getLogger(__name__)

__engine = None
__session_factory = None


def create_engine(url, **kw):
    global __engine
    if __engine is None:
        __engine = sa.create_engine(url, **kw)
    return __engine


def create_session_factory(url):
    global __session_factory
    engine = create_engine(url)
    if __session_factory is None:
        schema.Base.metadata.create_all(__engine, checkfirst=True)
        logger.info('Init base application tables')
        __session_factory = sessionmaker(bind=__engine)
    return __session_factory


@contextmanager
def ManagedSession(url):
    """Creates a context with an open SQLAlchemy session."""
    session = create_session_factory(url)()
    try:
        yield session
        session.commit()
        session.flush()
    except:
        session.rollback()
        raise
    finally:
        __session_factory.remove()


class NonBlockingDelay:
    """Non blocking delay class"""

    def __init__(self):
        self._timestamp = 0
        self._delay = 0

    def _seconds(self):
        return int(time.time())

    def timeout(self):
        """Check if time is up"""
        return (self._seconds() - self._timestamp) > self._delay

    def delay(self, delay):
        """Non blocking delay in seconds"""
        self._timestamp = self._seconds()
        self._delay = delay
