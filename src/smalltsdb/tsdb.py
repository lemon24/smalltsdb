import collections
import datetime
import sqlite3

import numpy


class QuantileAggregate:

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
        return numpy.quantile(self.values, self.q)


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


def open_db(path, periods):
    """Return a configured database connection with all the required tables.

    Arguments:
        path (str): Path to the database.
        periods (dict(str, int)): Aggregation views to create,
            as (view_name, aggregation_seconds) tuples.

    Returns:
        sqlite3.Connection

    """
    db = sqlite3.connect(path)
    db.create_aggregate('quantile', 2, QuantileAggregate)

    db.execute(
        """
        create table if not exists incoming (
            path text not null,
            timestamp real not null,
            value real not null
        );
    """
    )

    for name, seconds in periods.items():
        db.execute(
            f"""
            -- temporary because it's not gonna work on a connection that doesn't have quantile
            create temp view if not exists
            {name} (path, timestamp, n, min, max, avg, sum, p50, p90, p99) as
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
            group by path, agg_ts;
        """
        )

    return db


def epoch_from_datetime(dt):
    return (dt - datetime.datetime(1970, 1, 1)) / datetime.timedelta(seconds=1)


class TSDB:
    def __init__(self, path):
        self.db = open_db(path, PERIODS)

    def insert(self, tuples):
        with self.db as db:
            for t in tuples:
                db.execute("insert into incoming values (?, ?, ?);", t)

    def get_metric(self, path, period, stat, interval):
        assert period in PERIODS
        assert stat in STATS

        start, end = interval
        if isinstance(start, datetime.datetime):
            start = epoch_from_datetime(start)
        if isinstance(end, datetime.datetime):
            end = epoch_from_datetime()

        rows = self.db.execute(
            f"""
            select timestamp, {stat}
            from {period}
            where path = :path
            order by timestamp;
        """,
            {'path': path},
        )

        return list(rows)
