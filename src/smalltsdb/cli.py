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
        '%(asctime)s %(levelname)-7s %(message)s', '%Y-%m-%dT%H:%M:%S'
    )
    handler.setFormatter(formatter)
    log.addHandler(handler)


@cli.command()
@click.pass_obj
def daemon(kwargs):
    setup_logging(logging.DEBUG)
    smalltsdb.daemon.main(kwargs['path'])


@cli.command()
@click.pass_obj
def sync(kwargs):
    setup_logging(logging.DEBUG)
    smalltsdb.TSDB(kwargs['path']).sync()


if __name__ == '__main__':
    cli()
