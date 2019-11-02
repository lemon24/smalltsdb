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


@cli.command()
@click.pass_obj
def daemon(kwargs):
    smalltsdb.daemon.main(kwargs['path'])


if __name__ == '__main__':
    cli()
