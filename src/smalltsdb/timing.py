import collections
import contextlib
import logging
import time

from .utils import utcnow


try:
    import psutil
except ImportError:
    psutil = None

log = logging.getLogger('smalltsdb')


class Timing:
    def __init__(self, callbacks=()):
        self.callbacks = list(callbacks)
        self.timings = collections.deque()

    @contextlib.contextmanager
    def __call__(self, name):
        log.debug("timing start: %s", name)
        start_utc = utcnow()

        times = collections.OrderedDict()
        for callback in self.callbacks:
            times.update(callback())

        try:
            yield
        finally:
            for callback in self.callbacks:
                for tname, tvalue in callback():
                    end = tvalue
                    duration = end - times[tname]
                    times[tname] = duration
            log.debug(
                "timing end: %s: %s",
                name,
                ' '.join(
                    '%s %.2f' % (tname, tduration) for tname, tduration in times.items()
                ),
            )
            for tname, tduration in times.items():
                self.timings.append((f'{name}.{tname}', start_utc, tduration))

    def enable(self):
        if get_time_timings not in self.callbacks:
            self.callbacks.append(get_time_timings)
        if psutil and get_psutil_timings not in self.callbacks:
            self.callbacks.append(get_psutil_timings)


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
