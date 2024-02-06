"""See test cases for implemenation example
"""
import contextlib
import datetime
import logging
from typing import List

import dataset
import pandas as pd
import pytz
from jobsync import config
from jobsync.delay import delay
from jobsync.schema import Audit, Check, Node, create_tables

logger = logging.getLogger(__name__)


def now(tz):
    return datetime.datetime.now().astimezone(tz)


def today(tz):
    return datetime.date(now(tz).year, now(tz).month, now(tz).day)


class Job:
    """Job sync manager
    """
    def __init__(self, node_name, db_url, wait_on_enter=60, wait_on_exit=5,
                 skip_sync=False, create_tables=True):
        self.node_name = node_name
        self._wait_on_enter = int(abs(wait_on_enter)) or 60
        self._wait_on_exit = int(abs(wait_on_exit)) or 5
        self._skip_sync = skip_sync
        self._create_tables = create_tables
        self._steps = []
        self._node_count = 1
        self._tz = pytz.timezone(config.sql.timezone)
        self.db = dataset.connect(db_url)  # should be in SQLAlchemy format

    def __enter__(self):
        if self._create_tables:
            with contextlib.suppress(Exception):
                create_tables(self.db)
        self.__cleanup__()
        if not self._skip_sync:
            self.tell_ready()
            logger.info(f'Sleeping {self._wait_on_enter} seconds...')
            delay(self._wait_on_enter)
            self._node_count = len(self.poll_ready())
        return self

    def __exit__(self, exc_ty, exc_val, tb):
        logger.info(f'Exiting {self.node_name} context')
        if exc_ty:
            logger.exception(exc_val)
        self.__cleanup__()
        with contextlib.suppress(Exception):
            self.db.close()

    def tell_ready(self):
        """Notify other nodes that current node is ready"""
        self.db[Node].insert({'name': self.node_name, 'created': now(self._tz)})
        logger.debug(f'{self.node_name} told ready status')

    def poll_ready(self, use_cached=False) -> List:
        if self._skip_sync:
            return [self.node_name]
        return list(self.db[Node].distinct('name'))

    def tell_done(self):
        if self._skip_sync:
            return
        self.db[Check].insert({'node': self.node_name, 'created': now(self._tz)})

    def poll_done(self):
        if self._skip_sync:
            return pd.DataFrame(data={'node': [self.node_name], 'count': [1]})
        return list(
            self.db.query(f'select node, count(1) as count from {Check} group by node'))

    def all_done(self):
        """We want both all nodes to have completed and the checkpoint count to be identical."""
        data = self.poll_done()
        return len([x['node'] for x in data]) % self._node_count == 0 \
            and len({x['count'] for x in data}) == 1

    def add_step(self, step):
        self._steps.append((step, now(self._tz)))

    def remove_step(self, step):
        self._steps = [(_step, _) for _step, _ in self._steps if _step != step]

    def write_audit(self):
        """The application can chose to flush to audit the steps early, but generally it's fine to wait
        and flush on context manager exit (to avoid database trips).
        """
        if not self._steps:
            return
        thedate = today(self._tz)
        i = self.db[Audit].insert_many([{
            'date': thedate,
            'node': self.node_name,
            'item': step,
            'created': created,} for step, created in self._steps])
        logger.debug(f'Flushed {i} step to {Audit}')
        self._steps.clear()

    def _clean_node(self):
        self.db[Node].delete(name=self.node_name)
        logger.debug(f'Cleared {self.node_name} from {Node}')

    def _clean_check(self):
        self.db[Check].delete(node=self.node_name)
        logger.debug(f'Cleaned {self.node_name} from {Check}')

    def __cleanup__(self):
        if not self._skip_sync:
            self._clean_node()
            self._clean_check()
        self.write_audit()  # always log, even when not syncing
