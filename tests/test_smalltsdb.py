import queue
import socket
import threading
import time

import pytest

from smalltsdb.daemon import run_daemon
from smalltsdb.tsdb import TablesTSDB
from smalltsdb.tsdb import TwoDatabasesTSDB
from smalltsdb.tsdb import ViewTSDB


@pytest.fixture
def a_free_udp_port():
    # based on https://gist.github.com/bertjwregeer/0be94ced48383a42e70c3d9fff1f4ad0

    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(('127.0.0.1', 0))
    portnum = s.getsockname()[1]
    s.close()

    return portnum


@pytest.mark.parametrize('TSDB', [ViewTSDB, TablesTSDB, TwoDatabasesTSDB])
def test_integration(tmp_path, TSDB, a_free_udp_port):
    server_address = ('127.0.0.1', a_free_udp_port)
    db_path = str(tmp_path / 'db.sqlite')

    q = queue.Queue()

    def run():
        tsdb = TSDB(db_path)
        run_daemon(tsdb, server_address, q)

    t = threading.Thread(target=run)
    t.start()

    # give the thread time to start
    time.sleep(1)

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.sendto(b"one 1 1", server_address)
    sock.sendto(b"one 5 2\ntwo 2 5", server_address)
    sock.sendto(b"one 1 12\n", server_address)

    # also give it time to consume stuff
    time.sleep(1)

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


# TODO: threadless integration test
