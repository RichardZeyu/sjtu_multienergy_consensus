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
    # 加载配置
    config_obj = Config.from_yaml(local_node_id, config_file)
    coloredlogs.install(level=config_obj.log_level, reconfigure=True)
    # 设置上下文变量
    config.set(config_obj)
    # 拷贝上下文
    ctx = contextvars.copy_context()
    # 以获取到的上下文来执行方法
    ctx.run(bootstrap)
