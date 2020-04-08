import contextvars
import logging

import click

import coloredlogs

from ..bootstrap import bootstrap
from ..config import Config, config
from .root import root

L = logging.getLogger(__name__)


@root.command()
@click.option(
    '-c',
    '--config-file',
    help='path to configuration file',
    type=click.File(mode='rb'),
    default='./config.yaml',
)
@click.argument('LOCAL_NODE_ID', required=True, nargs=1, type=int)
def run(config_file, local_node_id):
    coloredlogs.install(level=logging.INFO)
    L.info(f'loading config {config_file.name}')

    config_obj = Config.from_yaml(local_node_id, config_file)
    coloredlogs.install(level=config_obj.log_level, reconfigure=True)

    config.set(config_obj)
    ctx = contextvars.copy_context()

    ctx.run(bootstrap)
