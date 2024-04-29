"""See test cases for implemenation example
"""
import contextlib
import logging
from functools import total_ordering

from jobsync.schema import Audit, Check, Node

import db
from date import now, today
from libb import attrdict, delay

logger = logging.getLogger(__name__)


@total_ordering
class BaseStep:

    def __init__(self, id, name=None):
        self.id = id
        self.name = name

    def __repr__(self):
        return f'id:{self.id},name:{self.name}'

    def __eq__(self, other):
        return self.id == other.id

    def __lt__(self, other):
        return self.id < other.id

    def __gt__(self, other):
        return not self.__lt__(other)


class Job:
    """Job sync manager"""

    def __init__(
        self,
        node_name,
        site,
        config,
        wait_on_enter=60,
        wait_on_exit=5,
        skip_sync=False,
    ):
        self.node_name = node_name
        self._wait_on_enter = int(abs(wait_on_enter)) or 60
        self._wait_on_exit = int(abs(wait_on_exit)) or 5
        self._skip_sync = skip_sync
        self._steps = []
        self._node_count = 1
        self.cn = db.connect(site, config)

    def __enter__(self):
        self.__cleanup__()
        if not self._skip_sync:
            self.set_idle()
            logger.info(f'Sleeping {self._wait_on_enter} seconds...')
            delay(self._wait_on_enter)
            self._node_count = len(self.get_idle())
        return self

    def __exit__(self, exc_ty, exc_val, tb):
        logger.info(f'Exiting {self.node_name} context')
        if exc_ty:
            logger.exception(exc_val)
        self.__cleanup__()
        with contextlib.suppress(Exception):
            self.cn.close()

    def get_idle(self) -> list[dict]:
        if self._skip_sync:
            return [attrdict(node=self.node_name)]
        sql = f'select distinct(name) from {Node}'
        return [attrdict(node=row['name']) for row in
                db.select(self.cn, sql).to_dict('records')]

    def get_done(self) -> list[dict]:
        """Return mapping of nodes to current completed checkpoints
        """
        if self._skip_sync:
            return [attrdict(node=self.node_name, count=1)]
        sql = f'select node, count(1) as count from {Check} group by node'
        return [attrdict(node=row['node'], count=row['count']) for row in
                db.select(self.cn, sql).to_dict('records')]

    def set_idle(self):
        """Notify other nodes that current node is ready
        """
        sql = f'insert into {Node} (name, created) values (%s, %s)'
        db.execute(self.cn, sql, self.node_name, now())
        logger.debug(f'{self.node_name} told ready status')

    def set_done(self):
        if self._skip_sync:
            return
        sql = f'insert into {Check} (node, created) values (%s, %s)'
        db.execute(self.cn, sql, self.node_name, now())

    def others_done(self) -> bool:
        """We want both all nodes to have completed and the checkpoint count
        to be identical.
        Return if the
        """
        data = self.get_done()
        len_node = len([x['node'] for x in data])
        len_count = len({x['count'] for x in data})
        return len_count == 1 and len_node % self._node_count == 0

    def add_step(self, step: BaseStep):
        self._steps.append((step, now()))

    def remove_step(self, step: BaseStep):
        self._steps = [(_step, _) for _step, _ in self._steps if _step != step]

    def write_audit(self):
        """The application can chose to flush to audit the steps early, but
        generally it's fine to wait and flush on context manager exit
        (to avoid database trips).
        """
        if not self._steps:
            return
        thedate = today()
        rows = [{'date': thedate, 'node': self.node_name, 'item': step.id,
                 'created': created}
                for step,created in self._steps]
        i = db.insert_rows(self.cn, Audit, rows)
        logger.debug(f'Flushed {i} step to {Audit}')
        self._steps.clear()

    def _clean_node(self):
        sql = f'delete from {Node} where name = %s'
        db.execute(self.cn, sql, self.node_name)
        logger.debug(f'Cleared {self.node_name} from {Node}')

    def _clean_check(self):
        sql = f'delete from {Check} where node = %s'
        db.execute(self.cn, sql, self.node_name)
        logger.debug(f'Cleaned {self.node_name} from {Check}')

    def __cleanup__(self):
        """Always log, even when not syncing
        """
        if not self._skip_sync:
            self._clean_node()
            self._clean_check()
        self.write_audit()
