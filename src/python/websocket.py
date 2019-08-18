import json
import time
import asyncio
import threading
from autobahn.asyncio.websocket import (
    WebSocketServerProtocol, WebSocketServerFactory
)

class WebSocketProtocol(WebSocketServerProtocol):
    async def onConnect(self, request):
        print("Client connecting: {}".format(request))

    async def onOpen(self):
        print("WebSocket connection open.")
        packet = dict()
        packet["packetType"] = "message"
        packet["message"] = "Welcome to the Autobahn server."
        data = json.dumps(packet).encode("UTF-8")
        self.sendMessage(data)
        packet = dict()
        # while True:
        #     await asyncio.sleep(.05)
        #     self.sendMessage(json.dumps(packet).encode("UTF-8"))

    async def onMessage(self, payload, isBinary):
        print(payload)

    async def onClose(self, wasClean, code, reason):
        print("WebSocket connection closed: {}".format(reason))

class WebSocketHandler():
    def __init__(self, host="0.0.0.0", port=5678):
        self.factory = WebSocketServerFactory()
        self.factory.protocol = WebSocketProtocol
        self.host = host
        self.port = port
        self._thread = None
        self._is_running = threading.Event()

    async def _wakeup(self):
        while True:
            await asyncio.sleep(1)
            print("wakeup")

    def _thread_target(self):
        self.loop.run_forever()
        print("Thread finished")

    def loop_start(self):
        self.loop = asyncio.get_event_loop()
        # self.hack = self.loop.create_task(self._wakeup()) # HACK!
        self.server_coroutine = self.loop.create_server(
            self.factory,
            self.host,
            self.port)
        self.server = self.loop.run_until_complete(self.server_coroutine)
        self._is_running.set()
        self._thread = threading.Thread(
            target=self._thread_target,
            name="WebSocketHandlerThread")
        self._thread.start()

    def loop_stop(self):
        if not self._thread:
            raise Exception("You need to start start the loop before stopping "
                            "it")
        # self.hack.cancel()
        self.server.close()
        self.loop.call_soon_threadsafe(self.loop.stop)
        print("Wating for WebSocket Handler Thread to finish...")
        self._thread.join()
        self._is_running.clear()
        self._thread = None
        print("Done!")


def main_test():
    ws_handler = WebSocketHandler()
    ws_handler.loop_start()
    while True:
        try:
            time.sleep(1)
        except KeyboardInterrupt:
            print("KeyboardInterrupt")
            break
    ws_handler.loop_stop()

if __name__ == '__main__':
    main_test()
