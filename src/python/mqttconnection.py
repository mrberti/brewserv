# Standard includes
import uuid
import time
import threading

# Dependencies
import paho.mqtt.client as mqtt

# BrewServ
from connection import MetaConnection
from variable import Variable


class MQTTConnection(MetaConnection):
    def __init__(
            self,
            server,
            port=1883,
            user=None,
            password=None,
            client_id=None,
            **kwargs):
        MetaConnection.__init__(self, **kwargs)
        self.server = server
        self.port = port
        self.user = user
        self.password = password
        self.client_id = client_id if client_id else uuid.uuid4
        self._mqtt_client = mqtt.Client()
        self._mqtt_client.on_connect = self._on_connect
        self._mqtt_client.on_message = self._on_message

    # MQTT specific functions
    def _on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            print("{}: Connected to {}:{}. ({})."
                  .format(self, self.server, self.port, rc))
            self.subscribe_to_variables()
        if rc == 4:
            print("{}: Could not connect to {}:{}. Wrong user name or "
                  "password ({})."
                  .format(self, self.server, self.port, rc))
        else:
            raise ConnectionError("Could not connect")

    def _on_message(self, client, userdata, msg):
        topic = msg.topic
        data = msg.payload.decode("utf-8")
        self.push_data(topic, data)

    def __str__(self):
        # d = dict()
        # d["server"] = self.server
        # d["port"] = self.port
        # d["user"] = self.user
        # d["password"] = self.password
        # d["client_id"] = self.client_id
        # return str(d)
        return "{!r} {}:{}".format(self, self.server, self.port)

    # Implementing the abstract methods
    def connect(self):
        if self.user:
            self._mqtt_client.username_pw_set(self.user, self.password)
        try:
            self._mqtt_client.connect(self.server)
        except TimeoutError as exc:
            raise ConnectionError(exc)

    def start(self):
        self._mqtt_client.loop_start()
    
    def subscribe(self, topic, qos=0):
        self._mqtt_client.subscribe(topic, qos)
    
    def stop(self):
        self._mqtt_client.loop_stop()

    def disconnect(self):
        self._mqtt_client.disconnect()


def got_notified(notification):
    # connection.wait_for_new_data(1.)
    print(notification)
    for var in notification.connection.pop_recent_variable():
        print(var._var["topic"], ":", var.get_last_datapoint())
        pass

def main_test():
    server = MQTTConnection("192.168.1.80", user="simon", password="supipass")
    server.add_variable("test/esp32-001/temp")
    # server.add_variable("test/esp32-001/hall")
    server.set_notify_cb(got_notified)
    server.connect()
    server.start()
    while True:
        try:
            time.sleep(1)
        except KeyboardInterrupt:
            break
    server.stop()
    server.disconnect()

if __name__ == "__main__":
    main_test()
