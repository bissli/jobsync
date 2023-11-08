import datetime
import logging
import time
from contextlib import contextmanager

import pytz
import sqlalchemy as sa
from dateutil import relativedelta
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
        db_session,
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
        self.session = db_session

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
        with self.session() as conn:
            conn.execute(sa.insert(Node), {'name': self._name, 'created': self.now})
        logger.info(f'Sleeping {self._pre_wait} seconds...')
        delay(self._wait_on_enter)

    def __publish_stopped(self):
        with self.session() as conn:
            conn.execute(sa.delete(self.Sync), {'node': self._name})
        logger.debug(f'Cleared {self._name} from {Sync.__tablename__}')

    def __flush_audit(self):
        stmt = sa.insert(Audit).from_select(['date', 'node', 'item', 'created'],
            sa.select(
            sa.text(f'{str(sa.bindparam("thedate"))} as "date"'),
            'node',
            'item',
            sa.text(f'{str(sa.bindparam("created"))} as "created"'))\
                .where('node' == sa.bindparam('node')))
        with self.session() as conn:
            param = {'node': self._name, 'thedate': self._thedate, 'created': now(self._tz)}
            conn.execute(stmt, param)
            stmt = sa.delete(Sync).where('node' == sa.bindparam('node'))
            i = conn.execute(stmt, param).rowcount
        logger.debug(f'Wrote {i} seen instruments to {Audit.__tablename__}')

    def __publish_completed_items(self):
        """We explicitly do this only after completing task in order to avoid needless database round-trips.
        Obviously a real-time sync would be ideal, but this is impractical over a network connection.
        """
        if not self._items:
            return
        rows = [{'node': self._name, 'item': i} for i in self._items]
        with self.session() as conn:
            i = conn.execute(sa.insert(self.Sync), rows).on_conflict_do_nothing().rowcount
        logger.debug(f'Wrote {i} saved instruments to {Sync.__tablename__}')

    def __count_synchronized_nodes(self):
        if not self._sync_nodes:
            return 1
        with self.session() as conn:
            i = conn.execute(sa.select(sa.func.count(sa.distinct('node')))).scalar()
        logger.debug(f'Found {i} nodes with published status')
        return i

    def __count_participating_nodes(self):
        now_minus_window = self.now - relativedelta.relativedelta(minutes=self._node_hearbeat_window)
        stmt =  sa.select(sa.func.count(sa.distinct('name')))\
            .where('created' > sa.bindparam('created_on'))
        with self.session() as conn:
            i = conn.execute(stmt, {'created_on': now_minus_window}).scalar()
        logger.debug(f'Found {i} participating nodes')
        return i


def delay(seconds):
    delay = NonBlockingDelay()
    delay.delay(seconds)
    while not delay.timeout():
        continue


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
