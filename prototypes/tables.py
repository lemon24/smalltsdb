"""
Example of using sqlite3 to ingest and aggregate metrics, mainly to show off
a plausible schema.

Like views.py, but using real tables instead of views.

Takes slightly less than views.py to run (I assume most of it is in
aggregate() instead of pretty_print_table(), though).

"""

import sqlite3

from views import QuantileAggregate, generate_random_data, pretty_print_table
from views import AGGREGATIONS, TABLES


def open_db(path, aggregations):
    """Return a configured database connection with all the required tables.

    Arguments:
        path (str): Path to the database.
        aggregations (list(tuple(str, int))): Aggregations to create,
            as (aggregation_name, aggregation_seconds) tuples.

    Returns:
        sqlite3.Connection

    """
    db = sqlite3.connect(path)
    db.create_aggregate('quantile', 2, QuantileAggregate)

    db.execute("""
        create table if not exists incoming (
            path text not null,
            timestamp real not null,
            value real not null
        );
    """)

    for name, _ in aggregations:
        db.execute(f"""
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
        """)

    return db


def aggregate(db, aggregations):
    for name, seconds in aggregations:
        db.execute(f"""
            insert into {name}
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
        """)


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
    aggregate(db, AGGREGATIONS)
    for table in TABLES:
        pretty_print_table(db, table)

