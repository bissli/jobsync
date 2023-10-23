import copy
import datetime
import logging
import time
from contextlib import contextmanager

import pytz
import sqlalchemy as sa
from dateutil import relativedelta

from . import model

logger = logging.getLogger(__name__)


class SyncTaskManager:
    """Task manager with sync synchronization
    NOT local thread safe
    We only publish completed syncs. Let the client dictate how to split tasks.
    """

    def __init__(
        self,
        name,
        synchronize_nodes=True,
        node_heartbeat_window=20,
        pre_wait=60,
        post_wait=5,
        db_appname='syncman_',
        db_connection='sqlite:///syncman.db',
        db_timezone='US/Eastern',
    ):
        self._name = name
        self._node_hearbeat_window = int(abs(node_heartbeat_window)) or 20
        self._pre_wait = int(abs(pre_wait)) or 60
        self._post_wait = int(abs(post_wait)) or 5
        self._synchronize_nodes = synchronize_nodes
        self._items = set()
        self._nodes = []
        self._tz = pytz.timezone(db_timezone)
        self._thedate = datetime.date(self.now.year, self.now.month, self.now.day)
        _ = model.create(db_connection, db_appname)
        self.engine, self.Node, self.Sync, self.Audit = _.values()

    #
    # public methods
    #

    @contextmanager
    def register_task(self, wait=120):
        self.__get_participating_nodes()
        yield  # during this time `add_sync` will be called to add syncs
        self.__publish_completed_syncs()
        for i in range(int(wait / 2)):
            if not self._synchronize_nodes:
                break
            if len(self._nodes) == self.__count_nodes_with_published_status():
                time.sleep(wait / 5)  # extra buffer
                break
            logger.debug(f'Waiting for all nodes to flush ({i+1}:{int(wait/2)})')
            time.sleep(wait / 60)
        time.sleep(self._post_wait)
        self.__clear_sync_and_audit()
        self._items.clear()

    def add_sync(self, item):
        self._items.add(item)

    def remove_sync(self, sync_id):
        self._items.remove(sync_id)

    @property
    def nodes(self):
        return copy.copy(self._nodes)

    #
    # private methods
    #

    @property
    def now(self):
        return datetime.datetime.now().astimezone(self._tz)

    def __enter__(self):
        if self._synchronize_nodes:
            self.__publish_running_status()
        else:
            logger.warning('Skipping node notification on enter.')
        return self

    def __exit__(self, exc_ty, exc_val, tb):
        if exc_ty:
            logger.exception(exc_val)
        if self._synchronize_nodes:
            self.__publish_stopped_status()
        else:
            logger.warning('Skipping node notification on exit.')

    def __publish_running_status(self):
        stmt = sa.insert(self.Node)
        with self.engine.begin() as conn:
            conn.execute(stmt, {'name': self._name, 'created': self.now})
        logger.info(f'Sleeping {self._pre_wait} seconds...')
        time.sleep(self._pre_wait)

    def __publish_stopped_status(self):
        stmt = sa.delete(self.Sync)
        with self.engine.begin() as conn:
            conn.execute(stmt, {'node': self._name})
        logger.debug(f'Cleared {self._name} from {self.Sync.name}')

    def __clear_sync_and_audit(self):
        stmt = sa.insert(self.Audit).from_select(
            [self.Audit.c.date, self.Audit.c.node, self.Audit.c.item, self.Audit.c.created],
            sa.select(
                sa.text(f'{str(sa.bindparam("thedate"))} as "date"'),
                self.Sync.c.node,
                self.Sync.c.item,
                sa.text(f'{str(sa.bindparam("created"))} as "created"'),
            ).where(self.Sync.c.node == sa.bindparam('node')),
        )
        with self.engine.begin() as conn:
            param = {'node': self._name, 'thedate': self._thedate, 'created': self.now}
            conn.execute(stmt, param)
            stmt = sa.delete(self.Sync).where(self.Sync.c.node == sa.bindparam('node'))
            i = conn.execute(stmt, param).rowcount
        logger.debug(f'Wrote {i} seen instruments to {self.Audit.name}')

    def __publish_completed_syncs(self):
        """We explicitly do this only after completing task in order to avoid needless database round-trips.
        Obviously a real-time sync would be ideal, but this is impractical over a network connection.
        """
        if not self._items:
            return
        rows = [{'node': self._name, 'item': i} for i in self._items]
        stmt = sa.insert(self.Sync)
        with self.engine.begin() as conn:
            i = conn.execute(stmt, rows).rowcount
        logger.debug(f'Wrote {i} saved instruments to {self.Sync.name}')

    def __count_nodes_with_published_status(self):
        if not self._synchronize_nodes:
            return 1
        stmt = sa.select(sa.func.count(sa.distinct(self.Sync.c.node)))
        with self.engine.begin() as conn:
            i = conn.execute(stmt).scalar()
        logger.debug(f'Found {i} nodes with published status')
        return i

    def __get_participating_nodes(self):
        if not self._synchronize_nodes:
            self._nodes = [self._name]
        else:
            stmt = (
                sa.select(sa.distinct(self.Node.c.name))
                .where(self.Node.c.created > sa.bindparam('created_on'))
                .order_by(self.Node.c.name)
            )
            with self.engine.begin() as conn:
                now_minus_window = self.now - relativedelta.relativedelta(minutes=self._node_hearbeat_window)
                res = conn.execute(stmt, {'created_on': now_minus_window}).fetchall()
            self._nodes = res[0] if res else []
