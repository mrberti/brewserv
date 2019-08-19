import utime as time
import ujson as json
from umqtt.simple import MQTTClient
import esp
import esp32
from machine import Timer, ADC, Pin

def mqtt_callback(topic, msg):
    global led
    print("{} {}".format(topic, msg))
    op = topic.decode().split("/")[-1].lower()
    msg = msg.decode().lower()
    if "led" == op:
        try:
            state = int(msg)
        except ValueError:
            if msg == "on":
                state = True
            elif msg == "off":
                state = False
            else:
                print("Received wrong format: '{}'".format(msg))
                return
        if state:
            led.on()
        else:
            led.off()

def mqtt_publish(t):
    client.publish(
        "test/{}/alive".format(mqtt_client_settings["client_id"]),
        str(alive))
    client.publish(
        "test/{}/hall".format(mqtt_client_settings["client_id"]),
        str(esp32.hall_sensor()))
    client.publish(
        "test/{}/temp".format(mqtt_client_settings["client_id"]),
        str((esp32.raw_temperature() - 32) / 1.8))

def loop():
    global alive
    client.wait_msg()
    # time.sleep_ms(1000)
    alive = time.time()

# Read user data
mqttfile_name = "mqttuser.json"
with open(mqttfile_name, "r") as f:
    user_credentials = json.load(f)

mqtt_client_settings = dict()
mqtt_client_settings["client_id"] = "esp32-{}".format("001")
mqtt_client_settings["server"] = "192.168.1.80"
# mqtt_client_settings["port"] = 1883
mqtt_client_settings["user"] = user_credentials.get("user")
mqtt_client_settings["password"] = user_credentials.get("password")
# mqtt_client_settings["keepalive"] = 60
# mqtt_client_settings["ssl"] = False
# mqtt_client_settings["ssl_params"] = {}

print(mqtt_client_settings)

client = MQTTClient(**mqtt_client_settings)
client.set_callback(mqtt_callback)
client.connect()
client.subscribe("test/{}/led".format(mqtt_client_settings["client_id"]))

led = Pin(2, Pin.OUT)
tim = Timer(-1)
tim.init(period=678, mode=Timer.PERIODIC, callback=mqtt_publish)

alive = 0
while True:
    try:
        loop()
    except KeyboardInterrupt:
        break
client.disconnect()
tim.deinit()