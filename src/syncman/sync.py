import copy
import datetime
import logging
import time
from contextlib import contextmanager, suppress

import sqlalchemy as sa
from dateutil import relativedelta

from . import db

logging.basicConfig()
logging.getLogger('sqlalchemy.engine').setLevel(logging.WARNING)
logging.getLogger('syncman').setLevel(logging.WARNING)
logger = logging.getLogger('syncman')


class SyncTaskManager:
    """Task manager with claim synchronization
    NOT local thread safe
    We only publish completed claims. Let the client dictate how to split tasks.
    """

    def __init__(
        self,
        name,
        config='config.yml',
        synchronize_agents=True,
        agent_heartbeat_window=20,
        pre_wait=60,
        post_wait=5,
    ):
        self.name = name
        self._agent_hearbeat_window = int(abs(agent_heartbeat_window)) or 20
        self._pre_wait = int(abs(pre_wait)) or 60
        self._post_wait = int(abs(post_wait)) or 5
        self._synchronize_agents = synchronize_agents
        self._local_claims = set()
        self._publishing_agents = []
        _ = db.create(config)
        self.tz = _['timezone']
        self.engine = _['engine']
        self.agent, self.Agent = _['agent']
        self.claim, self.Claim = _['claim']
        self.audit, self.Audit = _['audit']
        _ = datetime.datetime.now().astimezone(self.tz)
        self._today = datetime.date(_.year, _.month, _.day)

    #
    # public methods
    #

    @contextmanager
    def register_task(self, wait=120):
        self.__get_participating_agents()
        yield  # during this time `add_claim` will be called to add claims
        self.__publish_completed_claims()
        for i in range(int(wait / 2)):
            if len(self._publishing_agents) <= 1:
                break
            if len(self._publishing_agents) == self.__count_agents_with_status_published():
                time.sleep(wait / 5)  # extra buffer
                break  # done
            logger.debug(f'Waiting for all agents to flush ({i+1}:{int(wait/2)})')
            time.sleep(wait / 60)
        time.sleep(self._post_wait)
        self.__clear_published_claims()
        self._local_claims.clear()

    def add_claim(self, claim_id):
        self._local_claims.add(claim_id)

    def remove_claim(self, claim_id):
        self._local_claims.remove(claim_id)

    @property
    def agents(self):
        return copy.copy(self._publishing_agents)

    #
    # private methods
    #

    def __enter__(self):
        if self._synchronize_agents:
            self.__publish_status(ready=True)
        else:
            logger.warning('Skipping agent notification on enter.')
        return self

    def __exit__(self, exc_ty, exc_val, tb):
        if exc_ty:
            logger.exception(exc_val)
        if self._synchronize_agents:
            self.__publish_status(ready=False)
        else:
            logger.warning('Skipping agent notification on exit.')

    def __publish_status(self, ready=True):
        if ready:
            stmt = sa.insert(self.Agent)
            now = datetime.datetime.now().astimezone(self.tz)
            param = {self.agent.agent: self.name, self.agent.created: now}
            with self.engine.begin() as conn:
                conn.execute(stmt, param)
            logger.info(f'Sleeping {self._pre_wait} seconds...')
            time.sleep(self._pre_wait)
        else:
            with suppress(Exception):
                stmt = sa.delete(self.Claim)
                param = {self.claim.agent: self.name}
                with self.engine.begin() as conn:
                    conn.execute(stmt, param)
                logger.debug(f'Cleared {self.name} from {self.agent.name}')

    def __clear_published_claims(self):
        stmt = sa.insert(self.Audit).from_select(
            [self.audit.date, self.audit.agent, self.audit.claim],
            sa.select(
                sa.text(f':current_date as "{self.audit.date}"'),
                self.Claim.c[self.claim.agent].label(self.audit.agent),
                self.Claim.c[self.claim.claim].label(self.audit.claim),
            ).where(self.Claim.c[self.claim.agent] == sa.bindparam(self.claim.agent)),
        )
        with self.engine.begin() as conn:
            param = {self.claim.agent: self.name, 'current_date': self._today}
            conn.execute(stmt, param)
            stmt = sa.delete(self.Claim).where(self.Claim.c[self.claim.agent] == sa.bindparam(self.claim.agent))
            i = conn.execute(stmt, param).rowcount
        logger.debug(f'Wrote {i} seen instruments to {self.audit.name}')

    def __publish_completed_claims(self):
        """We explicitly do this only after completing task in order to avoid needless database round-trips.
        Obviously a real-time sync would be ideal, but this is impractical over a network connection.
        """
        if not self._local_claims:
            return
        rows = [{self.claim.agent: self.name, self.claim.claim: i} for i in self._local_claims]
        stmt = sa.insert(self.Claim)
        with self.engine.begin() as conn:
            i = conn.execute(stmt, rows).rowcount
        logger.debug(f'Wrote {i} saved instruments to {self.claim.name}')

    def __count_agents_with_status_published(self):
        stmt = sa.select(sa.distinct(self.Claim.c[self.claim.agent]))
        with self.engine.begin() as conn:
            res = conn.execute(stmt).fetchall()
        return len(res[0]) if res else 0

    def __get_participating_agents(self):
        if not self._synchronize_agents:
            self._publishing_agents = [self.name]
        else:
            stmt = (
                sa.select(sa.distinct(self.Agent.c[self.agent.agent].label('agent')))
                .where(self.Agent.c[self.agent.created] > sa.bindparam(self.agent.created))
                .order_by('agent')
            )
            with self.engine.begin() as conn:
                now_minus_window = datetime.datetime.now().astimezone(self.tz) - relativedelta.relativedelta(
                    minutes=self._agent_hearbeat_window
                )
                param = {self.agent.created: now_minus_window}
                res = conn.execute(stmt, param).fetchall()
            self._publishing_agents = res[0] if res else []
