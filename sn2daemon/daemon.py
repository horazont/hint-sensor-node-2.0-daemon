import asyncio
import logging

from . import protocol


class SensorNode2Daemon:
    def __init__(self, args, config, loop):
        super().__init__()
        self.logger = logging.getLogger("sn2d")
        self.__loop = loop
        self.__args = args
        self.__config = config

        self._protocol = None

    def _make_protocol(self):
        if self._protocol is not None:
            self.logger.warning("protocol already initialised!")
        self._protocol = protocol.SensorNode2Protocol()
        return self._protocol

    async def run(self):
        _, protocol = await self.__loop.create_datagram_endpoint(
            self._make_protocol,
            remote_addr=(
                self.__config['net']['remote_address'],
                self.__config['net'].get('port', 7284),
            ),
            local_addr=(
                "0.0.0.0",
                self.__config['net'].get('port', 7284),
            ),
        )

        while True:
            await asyncio.sleep(1)
