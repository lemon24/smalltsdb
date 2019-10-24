"""
Example of using sqlite3 to ingest and aggregate metrics, mainly to show off
a plausible schema.

Slightly faster than views.py:

$ time python prototypes/views.py :memory: 1000000 >/dev/null
real	1m14.439s
user	1m13.011s
sys	0m0.748s

$ time python prototypes/views_agg_list.py :memory: 1000000 >/dev/null
real	1m5.492s
user	1m4.185s
sys	0m0.852s

"""
import datetime
import random
import sqlite3

import numpy


class QuantileAggregate(list):

    q = None

    step = list.append

    def finalize(self):
        assert self.q is not None, "no q"
        return numpy.percentile(self, self.q * 100)


class P50Aggregate(QuantileAggregate):
    q = 0.5


class P90Aggregate(QuantileAggregate):
    q = 0.9


class P99Aggregate(QuantileAggregate):
    q = 0.99


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
    db.create_aggregate('p50', 1, P50Aggregate)
    db.create_aggregate('p90', 1, P90Aggregate)
    db.create_aggregate('p99', 1, P99Aggregate)

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
                p50(value),
                p90(value),
                p99(value)
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
