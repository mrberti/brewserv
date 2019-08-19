# Standard includes
import uuid
import time
import threading

# Dependencies
import paho.mqtt.client as mqtt

# BrewServ
from variable import Variable


class MQTTConnection(object):
    def __init__(self, server, port=1883, **kwargs):
        self.server = server
        self.port = port
        self.user = kwargs.get("user")
        self.password = kwargs.get("password")
        self.id = kwargs.get("id", uuid.uuid4)
        self.variables = dict()
        self._client = mqtt.Client()
        self._client.on_connect = self.on_connect
        self._client.on_message = self.on_message
        self._has_new_data_event = threading.Event()
        self._recent_write_variables = list()

    def subscribe_to_variables(self):
        for variable in self.variables:
            self.subscribe(variable)

    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            print("{}: Connected to {}:{}. ({})."
                  .format(self, self.server, self.port, rc))
            self.subscribe_to_variables()
        if rc == 4:
            print("{}: Could not connect to {}:{}. Wrong user name or "
                  "password ({})."
                  .format(self, self.server, self.port, rc))
        else:
            raise Exception("Could not connect")

    def on_message(self, client, userdata, msg):
        if msg.topic in self.variables:
            vvv = self.variables[msg.topic]
            vvv.push_data(msg.payload.decode("utf-8"))
            self._has_new_data_event.set()
            self._recent_write_variables.append(vvv)

    def has_new_data(self):
        return self._has_new_data_event.is_set()

    def wait_for_new_data(self, timeout=None):
        self._has_new_data_event.wait(timeout)

    def pop_recent_variable(self):
        if self._recent_write_variables:
            var = self._recent_write_variables.pop(0)
            if not self._recent_write_variables:
                self._has_new_data_event.clear()
            yield var
        else:
            self._has_new_data_event.clear()

    def connect(self):
        if self.user:
            self._client.username_pw_set(self.user, self.password)
        self._client.connect(self.server)

    def start(self):
        self._client.loop_start()

    def stop(self):
        self._client.loop_stop()

    def disconnect(self):
        self._client.disconnect()

    def subscribe(self, topic, qos=0):
        self._client.subscribe(topic, qos)

def main_test():
    server = MQTTConnection("192.168.1.80", user="simon", password="supipass")
    server.variables["test/esp32-001/temp"] = Variable("test/esp32-001/temp")
    server.variables["test/esp32-001/hall"] = Variable("test/esp32-001/hall")
    server.connect()
    server.subscribe("test/#")
    server.start()
    while True:
        try:
            server.wait_for_new_data()
            for var in server.pop_recent_variable():
                pass
                print(var._var["topic"])
        except KeyboardInterrupt:
            break
    server.stop()
    server.disconnect()

if __name__ == "__main__":
    main_test()
