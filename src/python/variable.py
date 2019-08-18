import time
import json
import collections
import threading


class Variable(object):
    def __init__(self, topic, name="", description="", unit="", **kwargs):
        self._var = dict()
        self._var["topic"] = topic
        self._var["name"] = ""
        self._var["description"] = ""
        self._var["unit"] = ""
        self._var["qos"] = 0
        self._var["origin"] = ""
        self._var["last_update"] = 0
        self._var["data"] = collections.OrderedDict()
        self.lock = threading.Lock()

    def push_data(self, data, timestamp=None, **kwargs):
        if not timestamp:
            timestamp = int(time.time() * 1e9)
        # try to parse with json, if failed, just store the raw string
        try:
            data_entry = json.loads(data)
        except json.decoder.JSONDecodeError:
            data_entry = data
        with self.lock:
            self._var["data"][timestamp] = data_entry
            self._var["last_update"] = timestamp

    def get_data(self, add_timestamp_offset=False):
        with self.lock:
            return self._var["data"]

    def __str__(self):
        with self.lock:
            return json.dumps(self._var)

def ts_to_str(ts):
    ts = float(ts)/1e9
    ns = str(ts).split(".")[1]
    format_str = "%Y-%m-%d %H:%M:%S.{}".format(ns)
    res = time.strftime(format_str, time.localtime(ts))
    return res


def main():
    var = Variable("test/muh")
    s = time.time()
    for i in range(10):
        var.push_data(str(i))
        time.sleep(.001)
    delta = round(time.time() - s, 3)
    print(str(delta) + " s")
    ts = var._var["last_update"]
    print(ts/1000000000)
    print(ts_to_str(ts))
    print(var)
    print(var.get_data())

if __name__ == "__main__":
    main()