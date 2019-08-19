import json
import time
import asyncio
import threading
from autobahn.asyncio.websocket import (
    WebSocketServerProtocol, WebSocketServerFactory
)
from autobahn.websocket.types import (
    ConnectionDeny
)


class BrewServWebSocketProtocol(WebSocketServerProtocol):
    """ This class handles all connection specific requests and
    messages."""

    # def onConnect(self, request):
        # print("Client connecting: {}".format(request))
        # pass
        # raise ConnectionDeny(ConnectionDeny.BAD_REQUEST, "shut up!")

    def onOpen(self):
        # print("WebSocket connection open.")
        self.factory.register(self)
        packet = dict()
        packet["packetType"] = "message"
        packet["message"] = "Welcome to the Autobahn server."
        data = json.dumps(packet).encode("UTF-8")
        self.sendMessage(data)

    def onMessage(self, payload, isBinary):
        print(payload)

    def onClose(self, wasClean, code, reason):
        # print("WebSocket connection closed: {}".format(reason))
        self.factory.unregister(self)


class BrewServWebSocketServerFactory(WebSocketServerFactory):
    """This class manages all connections and broadcasting messages."""

    protocol = BrewServWebSocketProtocol

    def __init__(self, daq, **kwargs):
        WebSocketServerFactory.__init__(self, **kwargs)
        self.daq = daq
        self.clients = list()

    def xxx(self, t):
        print("xxxxx " + t + " -------------")

    def register(self, client):
        if client not in self.clients:
            self.clients.append(client)

    def unregister(self, client):
        if client in self.clients:
            self.clients.remove(client)


class WebSocketHandler(object):
    def __init__(self, host, port, daq=None):
        self.host = host
        self.port = port
        self._thread = None
        self._is_running = threading.Event()
        self.factory = BrewServWebSocketServerFactory(daq=daq)

    async def _wakeup(self):
        """ HACK to bypass a bug that misses KeyboardInterrupts."""
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
    ws_handler = WebSocketHandler("0.0.0.0", 5678)
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
