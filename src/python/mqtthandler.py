import paho.mqtt.client as mqtt
import uuid
import time

from variable import Variable


class MQTTHandler(object):
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
    server = MQTTHandler("192.168.1.80", user="simon", password="supipass")
    server.variables["test/esp32-001/temp"] = Variable("test/esp32-001/temp")
    server.connect()
    server.subscribe("test/#")
    server.start()
    while True:
        try:
            time.sleep(.1)
        except KeyboardInterrupt:
            break
    server.stop()
    server.disconnect()

if __name__ == "__main__":
    main_test()
