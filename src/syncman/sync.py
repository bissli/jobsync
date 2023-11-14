"""See test cases for implemenation example
"""
import contextlib
import datetime
import logging
from typing import List

import pandas as pd
import pytz
from more_itertools import flatten
from syncman import config, db
from syncman.delay import delay
from syncman.schema import Audit, Check, Node, create_tables

logger = logging.getLogger(__name__)


def now(tz):
    return datetime.datetime.now().astimezone(tz)


def today(tz):
    return datetime.date(now(tz).year, now(tz).month, now(tz).day)


class SyncManager:
    """Task manager with sync synchronization
    """
    def __init__(self, name, cn, wait_on_enter=60, wait_on_exit=5, run_without_sync=False, create_tables=True):
        self.name = name
        self._wait_on_enter = int(abs(wait_on_enter)) or 60
        self._wait_on_exit = int(abs(wait_on_exit)) or 5
        self._run_without_sync = run_without_sync
        self._create_tables = create_tables
        self._tasks = []
        self._node_count = 1
        self._tz = pytz.timezone(config.sql.timezone)
        self.cn = cn

    def __enter__(self):
        if self._create_tables:
            with contextlib.suppress(Exception):
                create_tables(self.cn)
        self.__cleanup__()
        if not self._run_without_sync:
            self.publish_online()
            logger.info(f'Sleeping {self._wait_on_enter} seconds...')
            delay(self._wait_on_enter)
            self._node_count = self.count_online()
        return self

    def __exit__(self, exc_ty, exc_val, tb):
        logger.info(f'Exiting {self.name} context')
        if exc_ty:
            logger.exception(exc_val)
        self.__cleanup__()

    def publish_online(self):
        """Notify other nodes that current node is online"""
        db.execute(self.cn, f'insert into {Node} (name, created) values (%s, %s)', self.name, now(self._tz))
        logger.debug(f'{self.name} published ready status')

    def get_online(self, use_cached=False) -> List:
        if self._run_without_sync:
            return [self.name]
        return list(db.select_column(self.cn, f'select distinct(name) from {Node}'))

    def count_online(self, use_cached=False) -> int:
        """Query database for currently online nodes"""
        if self._run_without_sync:
            return 1
        i = db.select_scalar(self.cn, f'select count(distinct(name)) from {Node}')
        logger.debug(f'Found {i} participating nodes')
        return i

    def publish_checkpoint(self):
        if self._run_without_sync:
            return
        db.execute(self.cn, f'insert into {Check} (node, created) values (%s, %s)', self.name, now(self._tz))

    def count_checkpoints(self):
        if self._run_without_sync:
            return pd.DataFrame(data={'node': [self.name], 'count': [1]})
        return db.select(self.cn, f'select node, count(1) as count from {Check} group by node')

    def all_nodes_published_checkpoints(self):
        """We want both all nodes to have published and the checkpoint count to be identical."""
        df = self.count_checkpoints()
        return len(df['node']) % self._node_count == 0 and len(set(df['count'])) == 1

    def add_local_task(self, task):
        self._tasks.append((task, now(self._tz)))

    def remove_local_task(self, task):
        self._tasks = [(_task, _) for _task, _ in self._tasks if _task != task]

    def write_audit(self):
        """The application can chose to flush to audit the tasks early, but generally it's fine to wait
        and flush on context manager exit (to avoid database trips).
        """
        if not self._tasks:
            return
        thedate = today(self._tz)
        args = ','.join(['(%s, %s, %s, %s)'] * len(self._tasks))
        vals = list(flatten([(thedate, self.name, task, created) for task, created in self._tasks]))
        sql = f'insert into {Audit} (date, node, item, created) values {args}'
        i = db.execute(self.cn, sql, *vals)
        logger.debug(f'Flushed {i} tasks to {Audit}')
        self._tasks.clear()

    def _clean_node(self):
        db.execute(self.cn, f'delete from {Node} where name = %s', self.name)
        logger.debug(f'Cleared {self.name} from {Node}')

    def _clean_check(self):
        db.execute(self.cn, f'delete from {Check} where node = %s', self.name)
        logger.debug(f'Cleaned {self.name} from {Check}')

    def __cleanup__(self):
        if not self._run_without_sync:
            self._clean_node()
            self._clean_check()
        self.write_audit()  # always log, even when not syncing
