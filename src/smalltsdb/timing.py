# TODO: rename to timer?
import logging
import time
from contextlib import contextmanager

from .utils import utcnow

try:
    import psutil
except ImportError:
    psutil = None


log = logging.getLogger('smalltsdb')


class Timing:
    def __init__(self, callbacks=()):
        self.callbacks = list(callbacks)
        self._timings = None

    @contextmanager
    def __call__(self, name):
        first = self._timings is None
        if first:
            self._timings = []

        def call_callbacks():
            for callback in self.callbacks:
                yield from callback()

        log.debug("timing start: %s", name)
        start_utc = utcnow()

        starts = dict(call_callbacks())
        try:
            yield self._timings
        finally:
            ends = list(call_callbacks())
            times = {tname: end - starts[tname] for tname, end in ends}

            log.debug(
                "timing end: %s: %s",
                name,
                ' '.join(
                    '%s %.2f' % (tname, tduration) for tname, tduration in times.items()
                ),
            )
            for tname, tduration in times.items():
                self._timings.append((f'{name}.{tname}', start_utc, tduration))

            if first:
                self._timings = None

    def add_default_callbacks(self):
        if psutil and get_psutil_timings not in self.callbacks:
            self.callbacks.append(get_psutil_timings)
        if get_time_timings not in self.callbacks:
            self.callbacks.append(get_time_timings)


def get_psutil_timings():
    p = psutil.Process()
    with p.oneshot():
        cpu_times = p.cpu_times()
        for name in ['user', 'system']:
            yield name, getattr(cpu_times, name)
        try:
            io_counters = p.io_counters()
            for name in ['read_count', 'write_count', 'read_bytes', 'write_bytes']:
                yield name, getattr(io_counters, name)
        except AttributeError:
            pass


def get_time_timings():
    return [('time', time.monotonic())]
