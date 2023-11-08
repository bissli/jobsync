import datetime
import logging
from contextlib import contextmanager

import more_itertools
import pytz
from dateutil import relativedelta
from syncman import db
from syncman.delay import delay
from syncman.schema import Audit, Node, Sync

logger = logging.getLogger(__name__)


def now(tz):
    return datetime.datetime.now().astimezone(tz)


class SyncTaskManager:
    """Task manager with sync synchronization
    NOT local thread safe
    We only publish completed syncs. Let the client dictate how to split tasks.
    """

    def __init__(
        self,
        name,
        cn,
        sync_nodes=True,
        node_heartbeat_window=20,
        wait_on_enter=60,
        wait_on_exit=5,
        db_timezone='US/Eastern',
        ):
        self.name = name
        self._node_hearbeat_window = int(abs(node_heartbeat_window)) or 20
        self._wait_on_enter = int(abs(wait_on_enter)) or 60
        self._wait_on_exit = int(abs(wait_on_exit)) or 5
        self._sync_nodes = sync_nodes
        self._items = set()
        self._nodecount = 1
        self._tz = pytz.timezone(db_timezone)
        self._thedate = datetime.date(now(self._tz).year, now(self._tz).month, now(self._tz).day)
        self.cn = cn

    #
    # public methods
    #

    @contextmanager
    def synchronize(self, wait=120):
        yield  # during this time `add_sync` will be called to add syncs
        self.__publish_completed_items()
        for i in range(int(wait / 2)):
            if not self._sync_nodes:
                break
            if self._nodecount == self.__count_synchronized_nodes():
                delay(wait / 5)  # extra buffer
                break
            logger.debug(f'Waiting for all nodes to flush ({i+1}:{int(wait/2)})')
            delay(wait / 60)
        delay(self._wait_on_exit)
        self.__flush_audit()
        self._items.clear()

    def add_sync(self, item):
        if isinstance(item, (tuple, list)):
            self._items.update(item)
        else:
            self._items.add(item)

    def remove_sync(self, item):
        self._items.remove(item)

    @property
    def nodecount(self):
        return self._nodecount

    #
    # private methods
    #

    def __enter__(self):
        if self._sync_nodes:
            self.__publish_running()
            self._nodecount = self.__count_participating_nodes()
        return self

    def __exit__(self, exc_ty, exc_val, tb):
        print(f'Exiting {self.name}')
        if exc_ty:
            logger.exception(exc_val)
        if self._sync_nodes:
            self.__publish_stopped()

    def __publish_running(self):
        db.execute(self.cn, f'insert into {Node} (name, created) values (%s, %s)', self.name, now(self._tz))
        logger.debug(f'{self.name} published ready status')
        logger.info(f'Sleeping {self._wait_on_enter} seconds...')
        delay(self._wait_on_enter)

    def __publish_stopped(self):
        db.execute(self.cn, f'delete from {Sync} where node = %s', self.name)
        logger.debug(f'Cleared {self.name} from {Sync}')

    def __flush_audit(self):
        sql = f"""
insert into {Audit} (
    date,
    node,
    item,
    created
)
select %s as "date", node, item, %s as "created"
from
    {Sync}
where
    node = %s
"""
        with db.transaction(self.cn) as tx:
            tx.execute(sql, self._thedate, now(self._tz), self.name)
            i = tx.execute(f'delete from {Sync} where node = %s', self.name)
            logger.debug(f'Wrote {i} seen instruments to {Audit}')

    def __publish_completed_items(self):
        """We explicitly do this only after completing task in order to avoid needless database round-trips.
        Obviously a real-time sync would be ideal, but this is impractical over a network connection.
        """
        if not self._items:
            return
        args = ','.join(['(%s, %s)'] * len(self._items))
        vals = list(more_itertools.flatten([(self.name, i) for i in self._items]))
        sql = f'insert into {Sync} (node, item) values {args} on conflict do nothing'
        i = db.execute(self.cn, sql, *vals)
        logger.debug(f'Wrote {i} instruments to {Sync}')

    def __count_synchronized_nodes(self):
        if not self._sync_nodes:
            return 1
        i = db.select_scalar(self.cn, f'select count(distinct(node)) as count from {Sync}')
        logger.debug(f'Found {i} nodes with published status')
        return i

    def __count_participating_nodes(self):
        now_minus_window = now(self._tz) - relativedelta.relativedelta(minutes=self._node_hearbeat_window)
        i = db.select_scalar(self.cn, f'select count(distinct(name)) from {Node} where created > %s', now_minus_window)
        logger.debug(f'Found {i} participating nodes')
        return i
