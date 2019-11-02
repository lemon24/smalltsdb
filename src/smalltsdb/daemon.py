import contextlib
import datetime
import logging
import os
import queue
import signal
import socketserver
import threading

from . import TSDB

log = logging.getLogger('smalltsdb')


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


class HandlerMixin:
    def handle(self):
        try:
            tuples = list(parse_lines(l.decode('utf-8') for l in self.rfile))
            log.debug("got %s tuples", len(tuples))
        except Exception as e:
            log.exception("error while parsing tuples: %s", e)
            return
        self.server.queue.put(tuples)


class DatagramHandler(HandlerMixin, socketserver.DatagramRequestHandler):
    pass


class UDPServer(QueueMixin, socketserver.UDPServer):
    pass


class StreamHandler(HandlerMixin, socketserver.StreamRequestHandler):
    pass


class TCPServer(QueueMixin, socketserver.TCPServer):
    pass

    # We could make it threaded, but we'd have to have a way of limiting
    # the number of threads somehow; see this for how:
    # https://stackoverflow.com/a/11783132
    #
    # Could also use a semaphore, probably:
    # https://docs.python.org/3/library/threading.html#semaphore-example


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


def run_daemon(
    tsdb, server_address, queue, started_callback=None, processed_callback=None
):
    socketservers = run_socketservers(
        [
            (UDPServer, DatagramHandler, server_address),
            (TCPServer, StreamHandler, server_address),
        ],
        {'queue': queue},
    )
    with socketservers:
        if started_callback:
            started_callback()
        while True:
            t = queue.get()
            if t is None:
                break
            tsdb.insert(t)
            if processed_callback:
                processed_callback()
            # TODO: emit metrics here


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


def main(db_path):
    # TODO: refactor so logging setup and other stuff can go to the CLI
    logging.basicConfig()
    logging.getLogger('smalltsdb').setLevel(logging.DEBUG)

    q = queue.Queue()

    # telling it nicely to stop;
    # a priority queue could be used to make sure "control" messages get there first

    def signal_done(_, __):
        q.put(None)

    signal.signal(signal.SIGTERM, signal_done)

    logging.getLogger('smalltsdb').info("using db: %r", db_path)

    with contextlib.closing(TSDB(db_path)) as tsdb:
        # try:
        run_daemon(tsdb, ('localhost', 1111), q)
    # finally:
    # pretty_print_table(tsdb.db, 'tensecond')
