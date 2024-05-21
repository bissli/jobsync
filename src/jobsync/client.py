"""See test cases for implemenation example
"""
import contextlib
import logging
from collections.abc import Hashable
from copy import deepcopy
from functools import total_ordering
from types import ModuleType

import database as db

from date import Date, now, today
from jobsync.schema import Audit, Check, Claim, Node, init_database
from libb import attrdict, delay, isiterable

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
        node_name: str,
        site: str,
        config: ModuleType,
        wait_on_enter: int = 60,
        wait_on_exit: int = 0,
        skip_sync: bool = False,
        skip_db_init: bool = False,
        date: Date = None,
    ):
        self.node_name = node_name
        self._wait_on_enter = int(abs(wait_on_enter))
        self._wait_on_exit = int(abs(wait_on_exit))
        self._skip_sync = skip_sync
        self._tasks = []
        self._nodes = [attrdict(node=self.node_name)]
        self._date = date or today()
        self._cn = db.connect(site, config)
        if not skip_db_init:
            init_database(self._cn)

    def __enter__(self):
        """On enter (assuming sync)

        (1) Clear self from audit tables (in case previous run failed).
        (2) Notify nodes that we are ready to sync.
        (3) Wait `self._wait_on_enter` seconds.
        (4) Count number of nodes registered for sync.

        """
        self.__cleanup()
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
        if not self._skip_sync and self._wait_on_exit:
            logger.debug(f'Sleeping {self._wait_on_exit} seconds...')
            delay(self._wait_on_exit)
        self.__cleanup()
        with contextlib.suppress(Exception):
            self._cn.close()

    @property
    def nodes(self):
        """Safely expose internal nodes (for name matching, for example).
        """
        return deepcopy(self._nodes)

    def get_idle(self) -> list[attrdict]:
        """Get idle nodes.

        (1) Get nodes from `Node` table (membership indicates idle node).

        """
        if self._skip_sync:
            return [attrdict(node=self.node_name)]
        sql = f'select distinct(name) from {Node}'
        return [attrdict(node=row['name']) for row in
                db.select(self._cn, sql).to_dict('records')]

    def get_done(self) -> list[attrdict]:
        """Return mapping of nodes to current completed checkpoints

        (1) Query nodes in `Check` table (membership indicates node done).

        """
        if self._skip_sync:
            return [attrdict(node=self.node_name, count=1)]
        sql = f'select node, count(1) as count from {Check} group by node'
        return [attrdict(node=row['node'], count=row['count']) for row in
                db.select(self._cn, sql).to_dict('records')]

    def set_idle(self):
        """Notify other nodes that current node is ready

        (1) Add node to `Node` table (membership indicates idle node).

        """
        sql = f'insert into {Node} (name, created_on) values (%s, %s)'
        db.execute(self._cn, sql, self.node_name, now())
        logger.debug(f'{self.node_name} told ready status')

    def set_done(self):
        """Notify other nodes that current node is done

        (1) Add node to `Check` table (membership indicates node done).

        """
        if self._skip_sync:
            return
        sql = f'insert into {Check} (node, created_on) values (%s, %s)'
        db.execute(self._cn, sql, self.node_name, now())

    def set_claim(self, item):
        """Claim and item or items. Publish to database to
        inform other nodes.
        """
        if isiterable(item):
            this = [{'node': self.node_name, 'created_on': now(), 'item': i} for i in item]
            db.insert_rows(self._cn, Claim, *this)
        else:
            sql = f'insert into {Claim} (node, item, created_on) values (%s, %s, %s) on conflict do nothing'
            db.execute(self._cn, sql, self.node_name, str(item), now())

    def release_claim(self, item):
        """Release claim on item or items. Publish to database
        to inform other nodes.
        """
        if isiterable(item):
            sql = f'delete from {Claim} where node = %s and item in ({",".join(["%s"]*len(item))})'
            db.execute(self._cn, sql, self.node_name, *[str(i) for i in item])
        else:
            sql = f'delete from {Claim} where node = %s and item = %s'
            db.execute(self._cn, sql, self.node_name, str(item))

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
        rows = [{
            'date': self._date,
            'node': self.node_name,
            'item': task.id,
            'created_on': created
            } for task, created in self._tasks]
        i = db.insert_rows(self._cn, Audit, rows)
        logger.debug(f'Flushed {i} task to {Audit}')
        self._tasks.clear()

    def get_audit(self) -> list[attrdict]:
        sql = f'select node, item from {Audit} where date = %s'
        return [attrdict(node=row['node'], item=row['item']) for row in
                db.select(self._cn, sql, self._date).to_dict('records')]

    def __cleanup_node_table(self):
        sql = f'delete from {Node} where name = %s'
        db.execute(self._cn, sql, self.node_name)
        logger.debug(f'Cleared {self.node_name} from {Node}')

    def __cleanup_check_table(self):
        sql = f'delete from {Check} where node = %s'
        db.execute(self._cn, sql, self.node_name)
        logger.debug(f'Cleaned {self.node_name} from {Check}')

    def __cleanup_claim_table(self):
        sql = f'delete from {Claim} where node = %s'
        db.execute(self._cn, sql, self.node_name)
        logger.debug(f'Cleaned {self.node_name} from {Claim}')

    def __cleanup(self):
        """Always log, even when not syncing
        """
        if not self._skip_sync:
            self.__cleanup_node_table()
            self.__cleanup_check_table()
            self.__cleanup_claim_table()
        self.write_audit()
