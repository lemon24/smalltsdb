import logging

import click

import smalltsdb.daemon


@click.group()
@click.option(
    '--db',
    type=click.Path(dir_okay=False),
    # TODO: don't harcode this
    envvar='SMALLTSDB_DB',
    required=True,
)
@click.pass_context
def cli(ctx, db):
    ctx.obj = {'path': db}


def setup_logging(level):
    log = logging.getLogger('smalltsdb')
    log.setLevel(level)
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        '%(asctime)s %(process)d %(levelname)-7s %(message)s', '%Y-%m-%dT%H:%M:%S'
    )
    handler.setFormatter(formatter)
    log.addHandler(handler)


@cli.command()
@click.pass_obj
def daemon(kwargs):
    setup_logging(logging.DEBUG)
    smalltsdb.daemon.main(kwargs['path'])


@cli.command()
@click.option('--lock-file', type=click.Path(dir_okay=False, resolve_path=True))
@click.pass_obj
def sync(kwargs, lock_file):
    setup_logging(logging.DEBUG)

    # TODO: maybe move this into the main thing?
    if lock_file:
        from fasteners.process_lock import InterProcessLock

        lock = InterProcessLock(lock_file)
        if not lock.acquire(blocking=False):
            raise click.ClickException("could not acquire lock: {}".format(lock_file))

    smalltsdb.TSDB(kwargs['path']).sync()


if __name__ == '__main__':
    cli()
