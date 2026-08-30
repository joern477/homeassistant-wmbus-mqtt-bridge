#!/usr/bin/env python3

import argparse
import asyncio
import os
import pty
import signal
import sys
import termios

import aioesphomeapi


class ESPHomePTY:
    def __init__(self, host, port, key, proxy_name, link_path):
        self.host = host
        self.port = port
        self.key = key
        self.proxy_name = proxy_name
        self.link_path = link_path
        self.api = None
        self.instance = None
        self.master_fd = None
        self.slave_fd = None
        self.unsubscribe_data = None
        self.stop_event = asyncio.Event()

    async def connect(self):
        self.api = aioesphomeapi.APIClient(
            self.host, self.port, noise_psk=self.key
        )
        await self.api.connect(login=True)
        info = await self.api.device_info()
        proxies = info.serial_proxies
        if not proxies:
            raise RuntimeError("ESPHome device has no serial_proxy instances")

        for index, proxy in enumerate(proxies):
            print(
                f"serial_proxy[{index}]: {proxy.name} type={proxy.port_type}",
                flush=True,
            )
            if proxy.name == self.proxy_name:
                self.instance = index

        if self.instance is None:
            available = ", ".join(repr(p.name) for p in proxies)
            raise RuntimeError(
                f"serial_proxy {self.proxy_name!r} not found; "
                f"available: {available}"
            )

    def create_pty(self):
        self.master_fd, self.slave_fd = pty.openpty()
        slave_name = os.ttyname(self.slave_fd)

        attrs = termios.tcgetattr(self.slave_fd)
        attrs[0] = 0
        attrs[1] = 0
        attrs[2] &= ~(
            termios.PARENB
            | termios.PARODD
            | termios.CSTOPB
            | termios.CSIZE
        )
        attrs[2] |= termios.CS8
        attrs[3] = 0
        termios.tcsetattr(self.slave_fd, termios.TCSANOW, attrs)

        os.makedirs(os.path.dirname(self.link_path), exist_ok=True)
        try:
            os.unlink(self.link_path)
        except FileNotFoundError:
            pass
        os.symlink(slave_name, self.link_path)

        print(f"PTY created: {slave_name}", flush=True)
        print(f"PTY link:    {self.link_path}", flush=True)

    def on_esphome_data(self, message):
        if message.instance != self.instance:
            return
        if not message.data or self.master_fd is None:
            return
        try:
            os.write(self.master_fd, message.data)
        except OSError as err:
            print(f"PTY write failed: {err}", file=sys.stderr, flush=True)
            self.stop_event.set()

    async def pty_to_esphome(self):
        if self.master_fd is None or self.api is None or self.instance is None:
            raise RuntimeError("PTY/API not initialized")

        loop = asyncio.get_running_loop()
        while not self.stop_event.is_set():
            data = await loop.run_in_executor(
                None, os.read, self.master_fd, 4096
            )
            if not data:
                break
            self.api.serial_proxy_write(self.instance, data)

    async def run(self):
        await self.connect()
        self.create_pty()

        self.unsubscribe_data = self.api.subscribe_serial_proxy_data(
            self.on_esphome_data
        )
        self.api.serial_proxy_subscribe(self.instance)

        tx_task = asyncio.create_task(self.pty_to_esphome())
        stop_task = asyncio.create_task(self.stop_event.wait())
        try:
            await asyncio.wait(
                (tx_task, stop_task),
                return_when=asyncio.FIRST_COMPLETED,
            )
        finally:
            tx_task.cancel()
            stop_task.cancel()
            for task in (tx_task, stop_task):
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                except Exception:
                    pass

            if self.api is not None and self.instance is not None:
                try:
                    self.api.serial_proxy_unsubscribe(self.instance)
                except Exception:
                    pass
                try:
                    await self.api.disconnect()
                except Exception:
                    pass

            if self.unsubscribe_data is not None:
                try:
                    self.unsubscribe_data()
                except Exception:
                    pass

            for fd_name in ("master_fd", "slave_fd"):
                fd = getattr(self, fd_name)
                if fd is not None:
                    try:
                        os.close(fd)
                    except OSError:
                        pass
                    setattr(self, fd_name, None)

            try:
                os.unlink(self.link_path)
            except FileNotFoundError:
                pass


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", required=True)
    parser.add_argument("--port", type=int, default=6053)
    parser.add_argument("--key", required=True)
    parser.add_argument("--proxy", required=True)
    parser.add_argument("--link", required=True)
    args = parser.parse_args()

    bridge = ESPHomePTY(
        args.host, args.port, args.key, args.proxy, args.link
    )

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, bridge.stop_event.set)
        except NotImplementedError:
            pass

    await bridge.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
