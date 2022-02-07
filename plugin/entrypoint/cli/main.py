import click
from plugin.entrypoint.cli import apply


@click.group()
def cli():
    pass # We just need a click.group to create our command


cli.add_command(apply)


if __name__ == '__main__':
    cli()
