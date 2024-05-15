"""See test cases for implemenation example
"""
import contextlib
import logging
from collections.abc import Hashable
from copy import deepcopy
from functools import total_ordering

import database as db

from date import now, today
from jobsync.schema import Audit, Check, Node, init_database
from libb import attrdict, delay

logger = logging.getLogger(__name__)

__all__ = ['Job', 'Task']


@total_ordering
class Task:

    def __init__(self, id: Hashable, name: str = None):
        assert isinstance(id, Hashable), f'Id {id} must be hashable'
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
    """Job sync manager

    General routine:

    with Job(...) as job:
        # on enter we have written to the `Node` table to
        # notify other nodes that we are idle. Then we wait
        # and perform a query of idle nodes.

        call job.`add_task` and add Task

        # run some actions

        call job.`set_done` to indicate job completed.

        # wait some interval

        call job.`others_done` to check that other nodes have flushed.

        # if others done finished.

        # on exit we flush audit log and cleanup tables
    """

    def __init__(
        self,
        node_name,
        site,
        config,
        wait_on_enter=60,
        wait_on_exit=5,
        skip_sync=False,
        skip_db_init=False,
    ):
        self.node_name = node_name
        self._wait_on_enter = int(abs(wait_on_enter)) or 60
        self._wait_on_exit = int(abs(wait_on_exit)) or 5
        self._skip_sync = skip_sync
        self._tasks = []
        self._nodes = [self.node_name]
        self.cn = db.connect(site, config)
        if not skip_db_init:
            init_database(self.cn)

    def __enter__(self):
        """On enter (assuming sync)

        (1) Clear self from audit tables (in case previous run failed).
        (2) Notify nodes that we are ready to sync.
        (3) Wait `self._wait_on_enter` seconds.
        (4) Count number of nodes registered for sync.

        """
        self.__cleanup__()
        if not self._skip_sync:
            self.set_idle()
            logger.debug(f'Sleeping {self._wait_on_enter} seconds...')
            delay(self._wait_on_enter)
            self._nodes = self.get_idle()
        return self

    def __exit__(self, exc_ty, exc_val, tb):
        """On exit (assuming sync)

        (1) Clear self from audit tables.
        (2) Close DB connection.

        """
        logger.debug(f'Exiting {self.node_name} context')
        if exc_ty:
            logger.exception(exc_val)
        self.__cleanup__()
        with contextlib.suppress(Exception):
            self.cn.close()

    @property
    def nodes(self):
        """Safely expose internal nodes (for name matching, for example).
        """
        return deepcopy(self._nodes)

    def get_idle(self) -> list[dict]:
        """Get idle nodes.

        (1) Get nodes from `Node` table (membership indicates idle node).

        """
        if self._skip_sync:
            return [attrdict(node=self.node_name)]
        sql = f'select distinct(name) from {Node}'
        return [attrdict(node=row['name']) for row in
                db.select(self.cn, sql).to_dict('records')]

    def get_done(self) -> list[dict]:
        """Return mapping of nodes to current completed checkpoints

        (1) Query nodes in `Check` table (membership indicates node done).

        """
        if self._skip_sync:
            return [attrdict(node=self.node_name, count=1)]
        sql = f'select node, count(1) as count from {Check} group by node'
        return [attrdict(node=row['node'], count=row['count']) for row in
                db.select(self.cn, sql).to_dict('records')]

    def set_idle(self):
        """Notify other nodes that current node is ready

        (1) Add node to `Node` table (membership indicates idle node).

        """
        sql = f'insert into {Node} (name, created_on) values (%s, %s)'
        db.execute(self.cn, sql, self.node_name, now())
        logger.debug(f'{self.node_name} told ready status')

    def set_done(self):
        """Notify other nodes that current node is done

        (1) Add node to `Check` table (membership indicates node done).

        """
        if self._skip_sync:
            return
        sql = f'insert into {Check} (node, created_on) values (%s, %s)'
        db.execute(self.cn, sql, self.node_name, now())

    def others_done(self) -> bool:
        """We want both all nodes to have completed and the checkpoint count
        to be identical.
        """
        data = self.get_done()
        len_node = len([x['node'] for x in data])
        len_count = len({x['count'] for x in data})
        return len_count == 1 and len_node % len(self._nodes) == 0

    def add_task(self, task: Task):
        self._tasks.append((task, now()))

    def remove_task(self, task: Task):
        self._tasks = [(_task, _) for _task, _ in self._tasks if _task != task]

    def write_audit(self):
        """The application can chose to flush to audit the tasks early, but
        generally it's fine to wait and flush on context manager exit
        (to avoid database round trips).
        """
        if not self._tasks:
            return
        thedate = today()
        rows = [{
            'date': thedate,
            'node': self.node_name,
            'item': task.id,
            'created_on': created
            } for task, created in self._tasks]
        i = db.insert_rows(self.cn, Audit, rows)
        logger.debug(f'Flushed {i} task to {Audit}')
        self._tasks.clear()

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
