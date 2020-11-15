import logging
import time
from contextlib import contextmanager

from .utils import utcnow

try:
    import psutil
except ImportError:
    psutil = None


log = logging.getLogger('smalltsdb')


class Timer:

    """Measure blocks of code using arbitrary clocks.

    >>> def clock():
    ...     return [('time', time.monotonic())]
    ...
    >>> timer = Timer([clock])
    >>>
    >>> with timer('outer') as timings:
    ...     time.sleep(.1)
    ...     with timer('inner'):
    ...         time.sleep(.2)
    ...
    >>> for name, start, duration in timings:
    ...     print(name, round(start, 1), round(duration, 1))
    ...
    inner.time 1605456798.8 0.2
    outer.time 1605456798.7 0.3
    >>>
    >>> with timer('prefix', 'all') as timings:
    ...     with timer('one'):
    ...         time.sleep(.1)
    ...     with timer('two'):
    ...         time.sleep(.2)
    ...
    >>> for name, start, duration in timings:
    ...     print(name, round(start, 1), round(duration, 1))
    ...
    prefix.one.time 1605456799.0 0.1
    prefix.two.time 1605456799.1 0.2
    prefix.all.time 1605456799.0 0.3

    """

    def __init__(self, callbacks=(), prefix=None):
        self.callbacks = list(callbacks)
        self.prefixes = [prefix] if prefix else []
        self.separator = '.'
        self._timings = None

    @contextmanager
    def __call__(self, name_or_prefix, maybe_name=None):
        if maybe_name is None:
            name = name_or_prefix
        else:
            name = maybe_name
            self.prefixes.append(name_or_prefix)

        first = self._timings is None
        if first:
            self._timings = []

        name = self.separator.join(self.prefixes + [name])

        log.debug("timing start: %s", name)
        start_utc = utcnow()

        starts = dict(pair for callback in self.callbacks for pair in callback())
        try:
            yield self._timings
        finally:
            ends = list(
                pair for callback in reversed(self.callbacks) for pair in callback()
            )
            times = {tname: end - starts[tname] for tname, end in ends}

            log.debug(
                "timing end: %s: %s",
                name,
                ' '.join('%s %.2f' % t for t in times.items()),
            )
            for tname, tduration in times.items():
                self._timings.append(
                    (f'{name}{self.separator}{tname}', start_utc, tduration)
                )

            if first:
                self._timings = None

            if maybe_name is not None:
                self.prefixes.pop()

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
    return [('time', time.perf_counter())]
