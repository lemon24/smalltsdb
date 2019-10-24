"""
Example of using sqlite3 to ingest and aggregate metrics, mainly to show off
a plausible schema.

Uses views for the aggregated metrics.

Obviously, this is slow: ~50 seconds to go through all 6 aggregations for 1M
datapoints; there's no difference between :memory: and disk at this size, at
least on the same connection that created the data (caching may be involved).

"""
import datetime
import random
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
        return numpy.percentile(self.values, self.q * 100)


def open_db(path, aggregations):
    """Return a configured database connection with all the required tables.

    Arguments:
        path (str): Path to the database.
        view_seconds (list(tuple(str, int))): Aggregation views to create,
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

    for name, seconds in aggregations:
        db.execute(
            f"""
            create view if not exists
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


def generate_random_data(db, count):
    for i in range(count):
        db.execute(
            "insert into incoming values (?, ?, ?);",
            (
                random.choice(('one', 'two')),
                (
                    datetime.datetime.utcnow()
                    + datetime.timedelta(
                        microseconds=random.randrange(60 * 60 * 10 ** 6)
                    )
                ).timestamp(),
                random.randrange(100),
            ),
        )


def pretty_print_table(db, table):
    print('---', table)
    rows = db.execute(f"select * from {table} order by path, timestamp;")
    values_str = ''.join(f" {d[0]:>7}" for d in rows.description[2:])
    print(f"{rows.description[0][0]:<7} {rows.description[1][0]:<27}{values_str}")
    for path, timestamp, *values in rows:
        values_str = ''.join(f" {value:7.1f}" for value in values)
        print(
            f"{path:<7} {datetime.datetime.utcfromtimestamp(timestamp)!s:<27}{values_str}"
        )
    print()


AGGREGATIONS = [
    ('onesecond', 1),
    ('tensecond', 10),
    ('oneminute', 60),
    ('fiveminute', 300),
    ('onehour', 3600),
    ('oneday', 86400),
]

TABLES = ['incoming'] + [v for v, _ in AGGREGATIONS]


if __name__ == '__main__':
    import sys

    if len(sys.argv) < 2:
        path = ':memory:'
    else:
        path = sys.argv[1]
    if len(sys.argv) < 3:
        count = 10
    else:
        count = int(sys.argv[2])

    db = open_db(path, AGGREGATIONS)
    generate_random_data(db, count=count)
    for table in TABLES:
        pretty_print_table(db, table)
