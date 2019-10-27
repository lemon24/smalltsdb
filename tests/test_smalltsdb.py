import queue
import socket
import threading

import pytest

from smalltsdb.daemon import run_daemon
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
    processed = threading.Event()

    def run():
        tsdb = TSDB(db_path)
        run_daemon(tsdb, server_address, q, started.set, processed.set)

    t = threading.Thread(target=run)
    t.start()

    started.wait()

    messages = [b"one 1 1", b"one 5 2\ntwo 2 5", b"one 1 12\n"]

    for message in messages:
        with socket.socket(socket.AF_INET, socket_type) as sock:
            sock.connect(server_address)
            sock.send(message)

    # we're waiting for 3 things to be processed before killing the server
    for _ in range(3):
        processed.wait()

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
