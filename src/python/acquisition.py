# Standard imports
import time
import threading

# BrewServ
from mqttconnection import MQTTConnection
from variable import Variable

class Acquisition(object):
    def __init__(self, **kwargs):
        self._connections = list()
        self._variables = dict()
        self._thread = None
        self._is_running = threading.Event()
        self._last_new_connection = None

    def on_notify(self, notification):
        # print(notification.connection)
        for var in notification.connection.pop_recent_variable():
            print(var._var["topic"], ":", var.get_last_datapoint())

    def new_connection(self, host, type="mqtt", port=1883, **kwargs):
        if type.lower() == "mqtt":
            new_connection = MQTTConnection(host, port, **kwargs)
        else:
            raise NotImplementedError("Only MQTT supported")
        self._connections.append(new_connection)
        new_connection.set_notify_cb(self.on_notify)
        self._last_new_connection = new_connection

    def new_variable(self, topic, connection=None, **kwargs):
        if not connection:
            connection = self._last_new_connection
        var = connection.add_variable(topic, **kwargs)
        self._variables[topic] = var
        return var

    def connect(self):
        for connection in self._connections:
            try:
                connection.connect()
            except ConnectionError as exc:
                print("WARNING. Could not connect to '{}'. Removing from "
                      "connection list. {}"
                      .format(connection, exc))
                self._connections.remove(connection)

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

    def get_variables(self):
        return list(self._variables.values())

    def load_variables(self):
        print("Loading variables")
        for var in self._variables.values():
            var.load()

    def save_variables(self):
        print("Saving variables")
        for var in self._variables.values():
            var.save()
            var.save(type="csv")

def main_test():
    daq = Acquisition()
    daq.new_connection("192.168.1.80", user="simon", password="supipass")
    daq.new_variable("test/esp32-001/temp")
    daq.new_connection("test.mosquitto.org")
    daq.new_variable("room/bme280")
    daq.load_variables()
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
    daq.save_variables()

if __name__ == "__main__":
    main_test()
