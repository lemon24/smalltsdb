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

    def _now(self):
        return epoch_from_datetime(datetime.datetime.utcnow())

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
    def __init__(self, path):
        super().__init__()
        self.path = path

    def _open_db(self):
        db = sqlite3.connect(self.path)
        db.create_aggregate('quantile', 2, QuantileAggregate)

        db.execute(sql_create_incoming())

        # TODO: create index incoming_index on incoming(path, timestamp)?

        for name, seconds in PERIODS.items():
            db.execute(sql_create_agg(name))

        return db

    # FIXME
    _tail = 60

    def sync(self):
        # TODO: improve performance by not using an aggregate function at all;
        # pull the whole dataset (sorted) into memory, instead

        # TODO: retention policies

        # TODO: long-running queries suppress KeyboardInterrupt until they are done
        # TODO: run pragma optimize at the end

        now = self._now()

        for name, seconds in PERIODS.items():

            with self.db as db:
                start = datetime.datetime.now()

                log.debug("getting %s last finals", name)
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

                # exhaust the cursor so we don't get any weird "database is locked" errors
                # https://github.com/lemon24/smalltsdb/issues/2#issuecomment-549119926
                # TODO: this can fill up the memory if there are a lot of metrics, either paginate it or shove it into a temp table
                last_finals = list(last_finals)

                for path, last_final in last_finals:
                    (final_start, final_end), _ = intervals(
                        seconds, self._tail, now, last_final
                    )
                    log.debug(
                        "syncing %s %s for [%s, %s)",
                        name,
                        path,
                        datetime.datetime.utcfromtimestamp(final_start),
                        datetime.datetime.utcfromtimestamp(final_end),
                    )

                    # TODO: set zeroes on the things without incoming values to mark them as final
                    # TODO: maybe log the number of datapoints synced

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

                end = datetime.datetime.now()
                log.debug("synced %s in %s", name, end - start)

        start = datetime.datetime.now()

        delete_end = now - self._tail - max(PERIODS.values())
        log.debug(
            "deleting incoming datapoints older than %s",
            datetime.datetime.utcfromtimestamp(delete_end),
        )

        with db:
            db.execute("delete from incoming where timestamp < ?;", (delete_end,))

        end = datetime.datetime.now()
        log.debug("deleted old incoming in %s", end - start)


class TwoDatabasesTSDB(TablesTSDB):
    def __init__(self, path, incoming_path=None):
        super().__init__(path)
        if incoming_path is None:
            incoming_path = path
            if path not in (':memory:', ''):
                incoming_path += '.incoming'
        self.incoming_path = incoming_path

    def _open_db(self):
        db = sqlite3.connect(self.path)
        db.create_aggregate('quantile', 2, QuantileAggregate)

        db.execute("attach database ? as aux;", (self.incoming_path,))

        db.execute(sql_create_incoming('aux'))

        for name, seconds in PERIODS.items():
            db.execute(sql_create_agg(name))

        return db


TSDB = TablesTSDB
