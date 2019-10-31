import collections
import datetime
import logging
import sqlite3
from contextlib import contextmanager

import numpy

log = logging.getLogger('smalltsdb')


class QuantileAggregate:

    # TODO: mention subclassing list/array.array lack of significant improvements

    """quantile() sqlite3 aggregate function.

    Usage:

        -- p90, p99
        select
            key,
            quantile(value, .90),
            quantile(value, .99)
        from table
        group by key;

    **Very** slow; if it's used N times, we append and sort
    exactly the same values N times.

    """

    def __init__(self):
        self.values = []
        self.q = None

    def step(self, value, q):
        if self.q is None:
            self.q = q
        else:
            assert self.q == q, "q changed"
        self.values.append(value)

    def finalize(self):
        assert self.q is not None, "no q"
        return numpy.percentile(self.values, self.q * 100)


PERIODS = collections.OrderedDict(
    [
        ('onesecond', 1),
        ('tensecond', 10),
        ('oneminute', 60),
        ('fiveminute', 300),
        ('onehour', 3600),
        ('oneday', 86400),
    ]
)

STATS = 'n min max avg sum p50 p90 p99'.split()


def epoch_from_datetime(dt):
    return (dt - datetime.datetime(1970, 1, 1)) / datetime.timedelta(seconds=1)


class BaseTSDB:
    def __init__(self):
        self._db = None

    # private

    def _open_db(self):
        raise NotImplementedError

    # public - lifecycle

    @property
    def db(self):
        if not self._db:
            self._db = self._open_db()
        return self._db

    def close(self):
        self.db.close()
        self._db = None

    def sync(self):
        raise NotImplementedError

    # public - convenience methods

    def insert(self, tuples):
        with self.db:
            for t in tuples:
                self.db.execute("insert into incoming values (?, ?, ?);", t)

    def get_metric(self, path, period, stat, interval):
        # TODO: these should be ValueError
        assert period in PERIODS
        assert stat in STATS

        start, end = interval
        if isinstance(start, datetime.datetime):
            start = epoch_from_datetime(start)
        if isinstance(end, datetime.datetime):
            end = epoch_from_datetime(end)

        rows = self.db.execute(
            f"""
            select timestamp, {stat}
            from {period}
            where path = :path
                and timestamp between :start and :end
            order by timestamp;
            """,
            {'path': path, 'start': start, 'end': end},
        )
        return list(rows)

    def list_metrics(self):
        # TODO: find a better name
        # TODO: what period should we be looking at? all! but make it faster please

        parts = [f"select distinct path from {period}" for period in PERIODS]
        query = "\nunion\n".join(parts) + ";"

        for row in self.db.execute(query):
            yield row[0]


def sql_create_incoming(schema='main'):
    return f"""
        create table if not exists {schema}.incoming (
            path text not null,
            timestamp real not null,
            value real not null
        );
    """


def sql_create_agg(name):
    return f"""
        create table if not exists {name} (
            path text not null,
            timestamp real not null,
            n real not null,
            min real not null,
            max real not null,
            avg real not null,
            sum real not null,
            p50 real not null,
            p90 real not null,
            p99 real not null
        );
    """


def sql_select_agg(seconds):
    return f"""
        select
            path,
            cast(timestamp as integer) / {seconds} * {seconds} as agg_ts,
            count(value),
            min(value),
            max(value),
            avg(value),
            sum(value),
            quantile(value, .5),
            quantile(value, .9),
            quantile(value, .99)
        from incoming
        group by path, agg_ts
    """


class ViewTSDB(BaseTSDB):
    def __init__(self, path):
        super().__init__()
        self.path = path

    def _open_db(self):
        db = sqlite3.connect(self.path)
        db.create_aggregate('quantile', 2, QuantileAggregate)

        db.execute(sql_create_incoming())

        for name, seconds in PERIODS.items():
            db.execute(
                f"""
                -- temporary because it's not gonna work on a connection
                -- that doesn't have quantile
                create temp view if not exists {name} (
                    path, timestamp, n, min, max, avg, sum, p50, p90, p99
                ) as
                {sql_select_agg(seconds)};
                """
            )

        return db

    def sync(self):
        # synced by default!
        pass


class TablesTSDB(BaseTSDB):
    def __init__(self, path):
        super().__init__()
        self.path = path

    def _open_db(self):
        db = sqlite3.connect(self.path)
        db.create_aggregate('quantile', 2, QuantileAggregate)

        db.execute(sql_create_incoming())

        for name, seconds in PERIODS.items():
            db.execute(sql_create_agg(name))

        return db

    def sync(self):
        # TODO: improve performance by not using an aggregate function at all;
        # pull the whole dataset (sorted) into memory, instead

        # TODO: retention policies
        # TODO: only sync data within a certain window and drop incoming data outside of it

        with self.db as db:
            for name, seconds in PERIODS.items():
                start = datetime.datetime.now()
                db.execute(
                    f"""
                    insert or replace into {name} (
                        path, timestamp, n, min, max, avg, sum, p50, p90, p99
                    )
                    {sql_select_agg(seconds)};
                    """
                )
                end = datetime.datetime.now()
                log.debug("synced %s in %s", name, end - start)


class TwoDatabasesTSDB(TablesTSDB):
    def __init__(self, path, incoming_path=None):
        super().__init__(path)
        self.incoming_path = incoming_path or path + '.incoming'

    def _open_db(self):
        db = sqlite3.connect(self.path)
        db.create_aggregate('quantile', 2, QuantileAggregate)

        db.execute("attach database ? as aux;", (self.incoming_path,))

        db.execute(sql_create_incoming('aux'))

        for name, seconds in PERIODS.items():
            db.execute(sql_create_agg(name))

        return db


TSDB = ViewTSDB
