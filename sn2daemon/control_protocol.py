import asyncio
import functools
import logging
import random
import socket
import struct

from enum import Enum

from _sn2d_comm import lib


rng = random.SystemRandom()


class MsgType(Enum):
    PING = lib.ESP_PING
    PONG = lib.ESP_PONG
    SETUP = lib.ESP_SETUP


SetupPacket = struct.Struct(
    ">"
    "BLB16s16s"
)


def un_C_str(b):
    try:
        b = b[:b.index(b"\0")]
    except ValueError:
        pass
    return b.decode("ascii")


class ControlProtocol(asyncio.DatagramProtocol):
    def __init__(self, logger=None):
        self.logger = logger or logging.getLogger(
            ".".join([
                __name__, type(self).__qualname__,
            ])
        )
        self.__transport = None
        self.__disconnect_exc = ConnectionError("not connected")
        self.__waiters = {}

    def _require_connection(self):
        if self.__transport is None:
            raise self.__disconnect_exc

    def _timeout(self, key):
        try:
            fut = self.__waiters.pop(key)
        except KeyError:
            return

        fut.set_exception(TimeoutError)

    def _fut_done(self, key, fut):
        try:
            fut_in_mapping = self.__waiters.pop(key)
        except KeyError:
            return

        if fut_in_mapping is not fut:
            self.__waiters[key] = fut_in_mapping

    def _make_fut(self, key, timeout):
        loop = asyncio.get_event_loop()
        fut = asyncio.Future(loop=loop)
        self.__waiters[key] = fut
        fut.add_done_callback(functools.partial(self._fut_done, key))
        loop.call_later(timeout, self._timeout, key)
        return fut

    def datagram_received(self, buf, addr):
        header = buf[:5]
        if len(header) < 5:
            self.logger.debug("received corrupted frame: %r", buf)
            return

        try:
            fut = self.__waiters.pop(header)
        except KeyError:
            self.logger.debug("received unexpected frame: %r", buf)
            return

        if not fut.done():
            fut.set_result((addr, buf))

    def error_received(self, exc):
        pass

    def connection_made(self, transport):
        self.logger.debug("connected via %r", transport)
        self.__transport = transport
        sock = self.__transport.get_extra_info("socket")
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

    def connection_lost(self, exc):
        self.__transport = None
        self.__disconnect_exc = exc or ConnectionError("not connected")

    async def configure(
            self,
            remote_address,
            dest_addr,
            sntp_addr,
            timeout=5):
        self._require_connection()

        dest_addr = dest_addr.encode("ascii")
        assert len(dest_addr) < 16
        sntp_addr = sntp_addr.encode("ascii")
        assert len(sntp_addr) < 16

        dest_addr += b"\0" * (16 - len(dest_addr))
        sntp_addr += b"\0" * (16 - len(sntp_addr))

        msg_id = rng.getrandbits(32)
        pkt = SetupPacket.pack(
            MsgType.SETUP.value,
            msg_id,
            0,
            dest_addr,
            sntp_addr,
        )

        fut = self._make_fut(pkt[:5], timeout)
        addr = (remote_address, 7284)

        self.__transport.sendto(pkt, addr=addr)

        await fut

    async def detect(
            self,
            remote_address,
            timeout=5):
        self._require_connection()

        msg_id = rng.getrandbits(32)
        pkt = SetupPacket.pack(
            MsgType.SETUP.value,
            msg_id,
            0,
            b"\x00"*16,
            b"\x00"*16,
        )

        fut = self._make_fut(pkt[:5], timeout)
        addr = (remote_address, 7284)
        self.__transport.sendto(pkt, addr=addr)

        addr, response = await fut

        _, _, version, dest_addr, sntp_addr = SetupPacket.unpack(
            response
        )
        dest_addr = un_C_str(dest_addr)
        sntp_addr = un_C_str(sntp_addr)

        return addr[0], (dest_addr, sntp_addr)
