import time
from mqtthandler import MQTTHandler
from variable import Variable

class Acquisition(object):
    def __init__(self, **kwargs):
        self._subscriptions = list()
        self._connections = list()
        self._variables = dict()

    def new_connection(self, host, type="mqtt", port=1883, **kwargs):
        while self._connections:
            # Remove all connections (currently only one connection supported)
            connection = self._connections.pop()
            connection.disconnect()
        if type.lower() == "mqtt":
            new_connection = MQTTHandler(host, port, **kwargs)
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
        while True:
            try:
                time.sleep(1)
            except KeyboardInterrupt:
                break


def main_test():
    daq = Acquisition()
    daq.new_connection("192.168.1.80", user="simon", password="supipass")
    daq.new_variable("test/esp32-001/temp")
    daq.connect()
    daq.acquire_forever()

if __name__ == "__main__":
    main_test()