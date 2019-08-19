import time
import threading
from mqttconnection import MQTTConnection
from variable import Variable

class Acquisition(object):
    def __init__(self, **kwargs):
        self._subscriptions = list()
        self._connections = list()
        self._variables = dict()
        self._thread = None
        self._is_running = threading.Event()

    def new_connection(self, host, type="mqtt", port=1883, **kwargs):
        while self._connections:
            # Remove all connections (currently only one connection supported)
            connection = self._connections.pop()
            connection.disconnect()
        if type.lower() == "mqtt":
            new_connection = MQTTConnection(host, port, **kwargs)
        else:
            raise NotImplementedError("Only MQTT supported")
        new_connection.variables = self._variables
        self._connections.append(new_connection)

    def new_variable(self, topic, name="", description="", unit="", **kwargs):
        variable = Variable(topic, name, description, unit, **kwargs)
        self._variables[topic] = variable

    def connect(self):
        for connection in self._connections:
            # connection.variables = self._variables
            connection.connect()

    def acquire_forever(self):
        for connection in self._connections:
            connection.start()
        self._is_running.set()
        while self._is_running.is_set():
            try:
                time.sleep(1)
            except KeyboardInterrupt:
                break

    def loop_start(self):
        if self._thread:
            raise Exception("Acquisition Thread already running.")
        self._thread = threading.Thread(
            target=self.acquire_forever,
            name="AcquisitionThread")
        self._thread.start()

    def loop_stop(self):
        if not self._thread:
            raise Exception("Acquisition not running.")
        self._is_running.clear()
        print("Waiting for Acquisition Thread to stop.")
        self._thread.join()
        self._thread = None

        # Saving all data
        for var in self._variables.values():
            var.save()

    def get_variables(self):
        return list(self._variables.values())

def main_test():
    daq = Acquisition()
    daq.new_connection("192.168.1.80", user="simon", password="supipass")
    daq.new_variable("test/esp32-001/temp")
    daq.connect()
    # daq.acquire_forever()
    daq.loop_start()
    while True:
        try:
            time.sleep(1)
        except KeyboardInterrupt:
            print("KeyboardInterrupt")
            break
    daq.loop_stop()

if __name__ == "__main__":
    main_test()