"""
Example of using socketserver to build a statsd/Graphite-compatible daemon.

"""
import socketserver
import threading
import contextlib


class UDPHandler(socketserver.BaseRequestHandler):

    def handle(self):
        data, socket = self.request
        print('--- on', '{}:{}'.format(*socket.getsockname()))
        print(data.decode('utf-8'))


@contextlib.contextmanager
def run_socketservers(things):
    """Run `socketserver`s in background threads.

    Arguments:
        things(list(tuple(type, type, object))):
            An iterable of (server_cls, handler_cls, server_address) tuples.

    Returns:
        A context manager; starts the servers on enter, shuts them down on exit.

    """
    servers = []
    threads = []

    def serve_forever(server):
        with server:
            server.serve_forever()

    for server_cls, handler_cls, server_address in things:
        server = server_cls(server_address, handler_cls)
        thread = threading.Thread(target=serve_forever, args=(server, ))
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


def demo_carbon_client(address):
    """
    pip install carbon-client

    https://github.com/mosquito/carbon-client

    Carbon itself supports UDP and TCP as transports.

    This client supports only UDP; there are others.

    """
    from time import sleep

    from carbon.client import UDPClient
    from carbon.client.extras import SimpleCounter, SimpleTimer, SimpleCollector

    client = UDPClient(address, "carbon")

    with SimpleCounter("counter", client):
        sleep(.2)

    with SimpleTimer("timer", client):
        sleep(.3)

    with SimpleCollector("collector", client) as collector:
        collector.add(123)

    client.send()


def demo_statsd(address):
    """
    pip install statsd

    https://statsd.readthedocs.io/

    Also supports TCP and Unix sockets.

    """
    from time import sleep

    from statsd import StatsClient

    hostname, _, port = address.partition(':')
    port = int(port)

    statsd = StatsClient(hostname, port)
    statsd.incr('some.event')
    statsd.incr('some.other.event', 10)
    statsd.incr('some.third.event', rate=0.1)

    with statsd.timer('foo'):
        sleep(.1)

    statsd.gauge('foo', 70)
    statsd.gauge('foo', 1, delta=True)
    statsd.gauge('foo', -3, delta=True)

    statsd.set('users', 'self')


if __name__ == "__main__":

    socketservers = run_socketservers([
        (socketserver.UDPServer, UDPHandler, ('localhost', 1111)),
        (socketserver.UDPServer, UDPHandler, ('localhost', 2222)),
    ])

    with socketservers:
        demo_carbon_client('localhost:1111')
        demo_statsd('localhost:2222')

