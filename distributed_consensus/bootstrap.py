from .config import Config, config
from .transport.tcp.conn import TCPConnectionHandler, TCPProtocolV1
from .queue import QueueManager
from .sync_adapter import QueueManagerAdapter
from .scene.consensus.nodeUpdate import NodeUpdate
from distributed_consensus.demand import Demand

import logging
import asyncio
import threading
import struct

L = logging.getLogger(__name__)


async def async_main():
    e = '000000010000001e0200000001404cae4cf2ed435dc07d27d134aa656940608b0ede814a97004817d8f6afd1643ebfe115dd5a1310c78002e62f04a2ef79041d8ffb6629b0b6f135fe52f8ed9af0b3ec2016270a0b26f8de30f022a2677dbe9569090a3fa13fa1689435f40296693bc76b363e2779ca7d85defd822ed2b3d68bd6d4d0e07d0915c331a85d975e056cbdc0d6a51def6776cd383e5f961d9c04c8a574b4090a6cbc5dc02273a41e0400db52842122d10a9358a61d8b29703444e8b46c56c000b0294837dcb68720eb53ff9312a4922754dec597c7e9e06ffa8c1ae450b0951650d1841ace4c4d9f7682f0d8a6a5d686f5c7b963f725848179dd1f1241e2f478588db667360f2e339420c1c43b01361e10ae05a4ac7f5633fc8ea212575982405f4b9910e6905cea03df2d7cc3083a6ed91a5c323fb42cf7926f7625844cc5fabaa318e613f27c05fc246a95da16832b4ddba25d0f26d1aaa07c72ab1a977ca66b360604ae60b91a9b4ff331eba03848493db9fdef556950eaec6fed9de597470ccfc210abfc5fe514daf8d6e960dad187ad554ec14cbf534d0742bd37552bfdf288b9e905a92bd4db38c84842d4e063bddf03439f6d05d04db0a3d241752d0e05e44a1e8c7eace1b344c5186d475659f00f8b1c029258704c98138c2b3f00d995319e0d9d6b3c313b10e56d34af35ea72eb4959c6e52bff3ea4441b51b9981b8302d5940bcfe9cdf5c104c0930a603f1c63e78e8f3e1fc6a367137ef2fc7f510b' == '000000010000001e0200000001404cae4cf2ed435dc07d27d134aa656940608b0ede814a97004817d8f6afd1643ebfe115dd5a1310c78002e62f04a2ef79041d8ffb6629b0b6f135fe52f8ed9af0b3ec2016270a0b26f8de30f022a2677dbe9569090a3fa13fa1689435f40296693bc76b363e2779ca7d85defd822ed2b3d68bd6d4d0e07d0915c331a85d975e056cbdc0d6a51def6776cd383e5f961d9c04c8a574b4090a6cbc5dc02273a41e0400db52842122d10a9358a61d8b29703444e8b46c56c000b0294837dcb68720eb53ff9312a4922754dec597c7e9e06ffa8c1ae450b0951650d1841ace4c4d9f7682f0d8a6a5d686f5c7b963f725848179dd1f1241e2f478588db667360f2e339420c1c43b01361e10ae05a4ac7f5633fc8ea212575982405f4b9910e6905cea03df2d7cc3083a6ed91a5c323fb42cf7926f7625844cc5fabaa318e613f27c05fc246a95da16832b4ddba25d0f26d1aaa07c72ab1a977ca66b360604ae60b91a9b4ff331eba03848493db9fdef556950eaec6fed9de597470ccfc210abfc5fe514daf8d6e960dad187ad554ec14cbf534d0742bd37552bfdf288b9e905a92bd4db38c84842d4e063bddf03439f6d05d04db0a3d241752d0e05e44a1e8c7eace1b344c5186d475659f00f8b1c029258704c98138c2b3f00d995319e0d9d6b3c313b10e56d34af35ea72eb4959c6e52bff3ea4441b51b9981b8302d5940bcfe9cdf5c104c0930a603f1c63e78e8f3e1fc6a367137ef2fc7f510b'
    # 测试
    """data_type = 1
    round_id = 1
    values = [3,4,5]
    is_end = 0
    fmt = '>BL'+'L'*len(values)+'B'
    pck = struct.pack(fmt,data_type,round_id,*values,is_end)
    t = struct.unpack(fmt,pck)
    data_type = t[0]
    round_id = t[1]
    values = t[2:5]
    is_end = t[5]"""

    # 获取配置
    c: Config = config.get()
    L.debug('using config: %r', c)
    L.info(f'local node is normal: {c.local_node.is_normal}')
    L.info(f'local node is delegate: {c.local_node.is_delegate}')

    queue = QueueManager(c.local_node, c.node_manager)

    loop = asyncio.get_running_loop()
    adapter = QueueManagerAdapter(queue, loop)
    # demandtest = Demand()
    # demandtest.test_demand(c,**c.scene_parameters)
    conn = TCPConnectionHandler(
        c.local_node, c.node_manager, queue, TCPProtocolV1,loop
    )
     # 异步方法 获取EventLoop
    
    # asyncio.Event() 用来协同工作
    done = asyncio.Event()
    # 启动代表 启动监听（代表）、连接其他代表，目前貌似不论是否是代表，都会启动监听。
    if await conn.setup_and_wait_micronet(
        timeout=c.transport_parameters['tcpv1']['micronet_init_sec']
    ):
    #if True :
        # 普通节点发送数据
        L.info('micronet established')
        scene = c.scene_class(
            local=c.local_node,
            node_manager=c.node_manager,
            adapter=adapter,
            done_cb=lambda: loop.call_soon_threadsafe(done.set),
            **c.scene_parameters
        )
        # 开启新线程，设置为守护线程，当主线程退出时，子线程也会退出
        t = threading.Thread(target=scene.run, daemon=True)
        t.start()
        # asyncio.Event() 用来协同工作，让多个协程同步执行
        # done.wait()会一直等待，直到done.set为止，在另外的线程里使用done.set时，需要使用loop.call_soon_threadsafe来调用
        # 如上面的scene 的done_cb回调函数
        await done.wait()


def bootstrap():
    asyncio.run(async_main())
