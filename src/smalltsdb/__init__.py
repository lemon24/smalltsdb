import contextlib
import datetime
import logging
import queue
import socketserver
import sqlite3
import threading

import numpy

__version__ = '0.1.dev0'


log = logging.getLogger('smalltsdb')


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


AGGREGATIONS = [
    ('onesecond', 1),
    ('tensecond', 10),
    ('oneminute', 60),
    ('fiveminute', 300),
    ('onehour', 3600),
    ('oneday', 86400),
]


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


class SmallTSDB:
    def __init__(self, path):
        self.db = open_db(path, AGGREGATIONS)

    def insert(self, tuples):
        with self.db as db:
            for t in tuples:
                db.execute("insert into incoming values (?, ?, ?);", t)


def parse_line(line):
    parts = line.split()
    if len(parts) != 3:
        raise ValueError(f"invalid line: {line!r}")

    path, value, timestamp = parts
    value = float(value)
    timestamp = float(timestamp)

    return path, timestamp, value


def parse_lines(lines):
    for line in lines:
        yield parse_line(line)


class QueueMixin:
    def __init__(self, *args, queue, **kwargs):
        super().__init__(*args, **kwargs)
        self.queue = queue


class UDPServer(QueueMixin, socketserver.UDPServer):
    pass


class DatagramHandler(socketserver.BaseRequestHandler):
    def handle(self):
        data, socket = self.request
        try:
            tuples = list(parse_lines(data.decode('utf-8').splitlines()))
            log.debug("got %s tuples", len(tuples))
        except Exception as e:
            log.exception("error while parsing tuples: %s", e)
            return
        self.server.queue.put(tuples)


@contextlib.contextmanager
def run_socketservers(things, server_kwargs=None):
    server_kwargs = server_kwargs or {}
    """Run `socketserver`s in background threads.

    Arguments:
        things (list(tuple(type, type, object))):
            An iterable of (server_cls, handler_cls, server_address) tuples.
        server_kwargs (dict(str, object) or None)

    Returns:
        A context manager; starts the servers on enter, shuts them down on exit.

    """
    servers = []
    threads = []

    def serve_forever(server):
        log.debug(
            "starting %s with %s on %s",
            type(server).__name__,
            server.RequestHandlerClass.__name__,
            server.server_address,
        )
        with server:
            server.serve_forever()

    for server_cls, handler_cls, server_address in things:
        server = server_cls(server_address, handler_cls, **server_kwargs)
        thread = threading.Thread(target=serve_forever, args=(server,))
        servers.append(server)
        threads.append(thread)

    try:
        for thread in threads:
            thread.start()
        yield

    finally:
        for server in servers:
            server.shutdown()
        for thread in threads:
            try:
                thread.join()
            except RuntimeError as e:
                if "cannot join thread before it is started" in str(e):
                    pass
                # FIXME: If this gets raised, some threads may not get joined.
                raise


def run_daemon(tsdb, server_address, done):
    q = queue.SimpleQueue()

    socketservers = run_socketservers(
        [(UDPServer, DatagramHandler, server_address)], {'queue': q}
    )

    with socketservers:
        while not done.is_set():
            try:
                tsdb.insert(q.get(timeout=0.1))
            except queue.Empty:
                pass
