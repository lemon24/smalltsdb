import math
import os
import random
import sys
import threading
import time
import webbrowser
from subprocess import run

import smalltsdb

DB = sys.argv[1]
APP_HOST = '0.0.0.0'
APP_PORT = '8000'

os.environ.update(
    SMALLTSDB_DB=DB,
    FLASK_DEBUG='1',
    FLASK_TRAP_BAD_REQUEST_ERRORS='1',
    FLASK_APP='src/smalltsdb/app/wsgi.py',
)


def bg_run(*args, **kwargs):
    threading.Thread(target=run, args=args, kwargs=kwargs).start()


bg_run(['flask', 'run', '-h', APP_HOST, '-p', APP_PORT])
bg_run(['python', '-m', 'smalltsdb.cli', 'daemon', '--interval', '1'])


time.sleep(0.5)


now = smalltsdb.TSDB._now()

for i in range(3600 // 5):
    if random.randrange(10) == 0:
        continue
    ts = math.floor(now) - 3600 + i * 5 + random.random() / 10
    value = random.randint(1, 6)
    run(
        ['nc', '127.0.0.1', '1111'],
        input=f"local.random.diceroll {value} {ts}\n",
        text=True,
    )

time.sleep(0.5)

run(['python', '-m', 'smalltsdb.cli', 'sync'])
webbrowser.open(f"http://{APP_HOST}:{APP_PORT}")

while True:
    run(['python', '-m', 'smalltsdb.cli', 'sync'])
    time.sleep(5)
