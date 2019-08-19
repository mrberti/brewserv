import time
from acquisition import Acquisition
from websocket import WebSocketHandler


class BrewServ(object):
    def __init__(self, ws_host="", ws_port=5678):
        self.daq = Acquisition()
        self.ws_handler = WebSocketHandler(ws_host, ws_port, self.daq)


def main_test():
    brewserv = BrewServ()
    brewserv.daq.new_connection("192.168.1.80", user="simon", password="supipass")
    brewserv.daq.new_variable("test/esp32-001/temp")
    brewserv.daq.new_variable("test/esp32-001/hall")
    brewserv.daq.new_variable("test/esp32-001/status")
    brewserv.daq.connect()

    brewserv.daq.loop_start()
    brewserv.ws_handler.loop_start()
    while True:
        try:
            time.sleep(1)
            # for var in brewserv.daq._variables.values():
                # print(var.get_data())
        except KeyboardInterrupt:
            print("KeyboardInterrupt")
            break
    brewserv.daq.loop_stop()
    brewserv.ws_handler.loop_stop()

if __name__ == "__main__":
    main_test()
