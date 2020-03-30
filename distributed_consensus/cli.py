"""Console script for distributed_consensus."""
import sys
import click


@click.command()
def main(args=None):
    """Console script for distributed_consensus."""
    click.echo("Replace this message by putting your code into "
               "distributed_consensus.cli.main")
    click.echo("See click documentation at https://click.palletsprojects.com/")
    return 0


if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover
