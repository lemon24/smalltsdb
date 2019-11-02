import queue
import socket
import threading

import pytest

from smalltsdb.daemon import run_daemon
from smalltsdb.tsdb import intervals
from smalltsdb.tsdb import TablesTSDB
from smalltsdb.tsdb import TwoDatabasesTSDB
from smalltsdb.tsdb import ViewTSDB


def get_free_port(type):
    with socket.socket(socket.AF_INET, type) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


@pytest.fixture(params=[socket.SOCK_DGRAM, socket.SOCK_STREAM])
def socket_type_and_port(request):
    return request.param, get_free_port(request.param)


@pytest.fixture
def socket_type(socket_type_and_port):
    return socket_type_and_port[0]


@pytest.fixture
def port(socket_type_and_port):
    return socket_type_and_port[1]


@pytest.mark.parametrize('TSDB', [ViewTSDB, TablesTSDB, TwoDatabasesTSDB])
def test_integration(tmp_path, TSDB, socket_type, port):
    server_address = ('127.0.0.1', port)
    db_path = str(tmp_path / 'db.sqlite')

    q = queue.Queue()

    started = threading.Event()
    started_callback = started.set
    received_queue = queue.Queue()
    received_callback = lambda: received_queue.put(None)

    def run():
        tsdb = TSDB(db_path)
        run_daemon(tsdb, server_address, q, started_callback, received_callback)

    t = threading.Thread(target=run)
    t.start()

    started.wait()

    messages = [b"one 1 1", b"one 5 2\ntwo 2 5", b"one 1 12\n"]

    for message in messages:
        with socket.socket(socket.AF_INET, socket_type) as sock:
            sock.connect(server_address)
            sock.send(message)

    # we're waiting for 3 things to be received before killing the server
    for e in range(3):
        received_queue.get()

    q.put(None)
    t.join()

    tsdb = TSDB(db_path)
    tsdb.sync()

    rows = list(tsdb.db.execute('select * from tensecond order by path, timestamp;'))
    assert rows == [
        ('one', 0, 2, 1.0, 5.0, 3.0, 6.0, 3.0, 4.6, 4.96),
        ('one', 10, 1, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0),
        ('two', 0, 1, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0),
    ]


INTERVALS = """\
intervals(10, 30, 1:42, 0:30) -> (0:40, 1:10), (1:10, 1:50)
intervals(10, 30, 1:42, 0:50) -> (1:00, 1:10), (1:10, 1:50)
intervals(10, 30, 1:42, 1:00) -> (1:10, 1:10), (1:10, 1:50)
intervals(10, 30, 1:49, 1:00) -> (1:10, 1:10), (1:10, 1:50)
intervals(10, 30, 1:50, 1:00) -> (1:10, 1:20), (1:20, 2:00)

intervals(60, 30, 1:42, 0:00) -> (1:00, 1:00), (1:00, 2:00)
intervals(60, 30, 1:59, 0:00) -> (1:00, 1:00), (1:00, 2:00)
intervals(60, 30, 2:00, 0:00) -> (1:00, 1:00), (1:00, 3:00)
intervals(60, 30, 2:29, 0:00) -> (1:00, 1:00), (1:00, 3:00)
intervals(60, 30, 2:30, 0:00) -> (1:00, 2:00), (2:00, 3:00)
intervals(60, 30, 2:30, 1:00) -> (2:00, 2:00), (2:00, 3:00)

intervals(5*60, 60, 24:59, 0:00) -> (5:00, 20:00), (20:00, 25:00)
intervals(5*60, 60, 24:59, 15:00) -> (20:00, 20:00), (20:00, 25:00)
intervals(5*60, 60, 25:00, 15:00) -> (20:00, 20:00), (20:00, 30:00)
intervals(5*60, 60, 25:59, 15:00) -> (20:00, 20:00), (20:00, 30:00)
intervals(5*60, 60, 26:00, 15:00) -> (20:00, 25:00), (25:00, 30:00)

"""

INTERVALS_DATA = [
    eval(l.replace('intervals', '').replace('->', ',').replace(':', ' * 60 + '))
    for l in INTERVALS.splitlines()
    if l.strip()
]

INTERVALS_IDS = [l for l in INTERVALS.splitlines() if l.strip()]


@pytest.mark.parametrize('args, final, partial', INTERVALS_DATA, ids=INTERVALS_IDS)
def test_intervals(args, final, partial):
    assert intervals(*args) == (final, partial)


@pytest.mark.parametrize('TSDB', [TablesTSDB, TwoDatabasesTSDB])
def test_sync(TSDB):
    tsdb = TSDB(':memory:')
    tsdb._tail = 60

    def tensecond():
        return list(
            tsdb.db.execute(
                'select path, timestamp, n from tensecond order by path, timestamp;'
            )
        )

    tsdb.insert([('one', 1, 1), ('two', 5, 2)])

    tsdb._now = lambda: 69
    tsdb.sync()
    assert tensecond() == []

    tsdb._now = lambda: 70
    tsdb.sync()
    assert tensecond() == [('one', 0, 1), ('two', 0, 1)]

    tsdb.insert([('one', 2, 5), ('one', 12, 1)])

    tsdb._now = lambda: 79
    tsdb.sync()
    assert tensecond() == [('one', 0, 1), ('two', 0, 1)]

    tsdb._now = lambda: 80
    tsdb.sync()
    assert tensecond() == [('one', 0, 1), ('one', 10, 1), ('two', 0, 1)]

    tsdb._now = lambda: 90
    tsdb.sync()
    assert tensecond() == [('one', 0, 1), ('one', 10, 1), ('two', 0, 1)]
