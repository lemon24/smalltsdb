import datetime
import logging
import signal
import threading

from . import run_daemon
from . import SmallTSDB


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


if __name__ == '__main__':

    logging.basicConfig()
    logging.getLogger('smalltsdb').setLevel(logging.DEBUG)

    done = threading.Event()

    # an example of using done

    def signal_done(_, __):
        done.set()

    signal.signal(signal.SIGTERM, signal_done)

    tsdb = SmallTSDB(':memory:')
    try:
        run_daemon(tsdb, ('localhost', 1111), done)
    finally:
        pretty_print_table(tsdb.db, 'tensecond')
