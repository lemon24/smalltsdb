import collections
import datetime
import logging
import sqlite3
import time

import numpy

from .timing import Timing
from .utils import utcnow

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


class BaseTSDB:
    def __init__(self, with_incoming, with_aggregate):
        self._db = None
        self._with_incoming = with_incoming
        self._with_aggregate = with_aggregate
        self._timing = Timing()

    # private

    def _open_db(self):
        raise NotImplementedError

    _now = staticmethod(utcnow)

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
        assert self._with_incoming
        with self.db as db:
            db.executemany("insert into incoming values (?, ?, ?);", tuples)

    def get_metric(self, path, period, stat, interval):
        assert self._with_aggregate

        # TODO: these should be ValueError
        assert period in PERIODS
        assert stat in STATS

        start, end = interval
        if isinstance(start, datetime.datetime):
            start = epoch_from_datetime(start)
        if isinstance(end, datetime.datetime):
            end = epoch_from_datetime(end)

        with self._timing("get_metric"):
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
        assert self._with_aggregate
        # TODO: find a better name
        # TODO: what period should we be looking at? all! but make it faster please

        parts = [f"select distinct path from {period}" for period in PERIODS]
        query = "\nunion\n".join(parts) + ";"

        # TODO: can exhaust memory, paginate
        with self._timing("list_metrics"):
            return [row[0] for row in self.db.execute(query)]


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
            p99 real not null,
            primary key (path, timestamp)
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
    def __init__(self, path, *, with_incoming=True, with_aggregate=True):
        super().__init__(with_incoming, with_aggregate)
        self.path = path

    def _open_db(self):
        db = sqlite3.connect(self.path)

        db.execute(sql_create_incoming())

        if not self._with_aggregate:
            return db

        db.create_aggregate('quantile', 2, QuantileAggregate)

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
        log.debug("ViewTSDB is synced by default, not doing anything!")


def intervals(period, tail, now, last_final):
    """For a specific period and time, return what needs to be updated
    as a (final interval, partial interval).

    period is the datapoint period in seconds: 1 (onesecond), 10 (tensecond) etc.

    tail is the number of seconds back from now for which we don't
    consider consider the datapoints final.

    now is now in seconds.

    last_final is the last final datapoint.

    The intevals are [start, end) tuples.

    For example:

        intervals(10, 30, 102, 30) == ((40, 70), (70, 110))

    That is:

        intervals(10, 30, 1:42, 0:30)
        == [0:40, 1:10), [1:10, 1:50)
        == [0:40, 0:50, 1:00], [1:10, 1:20, 1:30, 1:40]

    """
    if last_final is None:
        last_final = -period

    final_start = last_final + period
    final_end = (now - tail) // period * period
    partial_start = final_end
    partial_end = (now // period + 1) * period

    return (final_start, final_end), (partial_start, partial_end)


class TablesTSDB(BaseTSDB):

    # Turns out using attached databases to achieve table-level locking is a known thing:
    # http://sqlite.1065341.n5.nabble.com/Locking-td5121.html#a5122
    # https://www.sqlite.org/version3.html ("Improved concurrency")

    def __init__(
        self, path, *, with_incoming=True, with_aggregate=True, self_metric_prefix=None
    ):
        super().__init__(with_incoming, with_aggregate)
        self.path = path
        self.self_metric_prefix = self_metric_prefix

    def _open_db(self):
        db = sqlite3.connect(self.path)

        if self._with_incoming and self._with_aggregate:
            db.create_aggregate('quantile', 2, QuantileAggregate)

        if self._with_incoming:
            db.execute(sql_create_incoming())
            db.execute(
                f"create index if not exists incoming_index on incoming(path, timestamp);"
            )

        if self._with_aggregate:
            for name, seconds in PERIODS.items():
                db.execute(sql_create_agg(name))
                db.execute(
                    f"create index if not exists {name}_index on {name}(path, timestamp);"
                )

        return db

    # FIXME
    _tail = 60

    def sync(self):
        assert self._with_aggregate and self._with_incoming

        with self._timing('sync.all') as timings:
            self._sync()

        if self.self_metric_prefix:
            # TODO: the target database for debug metrics should be configurable
            #
            # at the moment, with 1 sync/minute, this emits
            #
            # metrics = (
            #   2           # sync.all and sync.delete_incoming_query
            #   + 6 * (     # len(PERIODS)
            #       2       # sync.$PERIOD.all and sync.$PERIOD.finals_query
            #       + metrics)  # 1 sync.$PERIOD.sync_query per metric
            #     )
            #  ) * 7        # len(values per timing invocation)
            #
            # metrics = (2 + 6 * (2 + metrics)) * 7
            # metrics = 14 + 84 + 42 * metrics
            # 1 = 98 * metrics + 42 * metrics**2
            # 42 * metrics^2 + 98 * metrics - 1 = 0
            #
            # metrics = (-98 +- sqrt(98**2 - 4*42*(-1))) / (2 * 42)
            #   = (-98 +- sqrt(9722)) / 84
            #   = ... TBD
            #
            self.insert((f'{self.self_metric_prefix}.{t[0]}',) + t[1:] for t in timings)

    def _sync(self):
        # TODO: improve performance by not using an aggregate function at all;
        # pull the whole dataset (sorted) into memory, instead

        # TODO: retention policies

        # TODO: long-running queries suppress KeyboardInterrupt until they are done
        # TODO: run pragma optimize at the end
        # TODO: maybe vacuum at the end
        # TODO: cap the time the queries run via interrupt()

        now = self._now()

        timing_results = []

        for name, seconds in PERIODS.items():

            with self.db as db, self._timing(f'sync.{name}.all'):
                with self._timing(f'sync.{name}.finals_query'):

                    last_finals = db.execute(
                        f"""
                        with
                        paths(path) as (
                            select distinct path from incoming
                        )
                        select paths.path, max({name}.timestamp)
                        from paths left join {name} on paths.path = {name}.path
                        group by paths.path;
                        """
                    )

                    # exhaust the cursor so we don't get any weird "database is locked" errors (?)
                    # https://github.com/lemon24/smalltsdb/issues/2#issuecomment-549119926
                    # TODO: this can fill up the memory if there are a lot of metrics, either paginate it or shove it into a temp table
                    last_finals = list(last_finals)

                for path, last_final in last_finals:
                    (final_start, final_end), _ = intervals(
                        seconds, self._tail, now, last_final
                    )

                    log.debug(
                        "sync path: %s %s for [%s, %s)",
                        name,
                        path,
                        datetime.datetime.utcfromtimestamp(final_start),
                        datetime.datetime.utcfromtimestamp(final_end),
                    )
                    with self._timing(f'sync.{name}.sync_query'):
                        # TODO: set zeroes on the things without incoming values to mark them as final
                        # TODO: maybe log the number of datapoints synced
                        # TODO: sort the datapoints before group by (it may speed quantile() up); do this after gathering sync metrics

                        rows = db.execute(
                            f"""
                            insert or replace into {name} (
                                path, timestamp, n, min, max, avg, sum, p50, p90, p99
                            )
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
                            where path = :path
                                and timestamp between :start and :end
                            group by path, agg_ts
                            """,
                            {'path': path, 'start': final_start, 'end': final_end},
                        )

        delete_end = now - self._tail - max(PERIODS.values())

        log.debug(
            "delete incoming: older than %s",
            datetime.datetime.utcfromtimestamp(delete_end),
        )
        with self.db as db, self._timing('sync.delete_incoming_query'):
            db.execute("delete from incoming where timestamp < ?;", (delete_end,))


class TwoDatabasesTSDB(TablesTSDB):
    def __init__(
        self,
        path,
        incoming_path=None,
        *,
        with_incoming=True,
        with_aggregate=True,
        self_metric_prefix=None,
    ):
        super().__init__(
            path,
            with_incoming=with_incoming,
            with_aggregate=with_aggregate,
            self_metric_prefix=self_metric_prefix,
        )
        if incoming_path is None:
            incoming_path = path
            if path not in (':memory:', ''):
                incoming_path += '.incoming'
        self.incoming_path = incoming_path

    def _open_db(self):
        # TODO: this looks almost the same as TablesTSDB._open_db; dedupe
        db = sqlite3.connect(self.path)

        if self._with_incoming and self._with_aggregate:
            db.create_aggregate('quantile', 2, QuantileAggregate)

        if self._with_incoming:
            db.execute("attach database ? as aux;", (self.incoming_path,))
            db.execute(sql_create_incoming('aux'))
            db.execute(
                f"create index if not exists aux.incoming_index on incoming(path, timestamp);"
            )

        if self._with_aggregate:
            for name, seconds in PERIODS.items():
                db.execute(sql_create_agg(name))
                db.execute(
                    f"create index if not exists {name}_index on {name}(path, timestamp);"
                )

        return db


TSDB = TablesTSDB
