import time
import json
import collections
import threading
from pathlib import Path


DEFAULT_DATA_FOLDER = Path.home() / ".brewserv/data"


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
        self._lock = threading.Lock()
        self._has_new_data = threading.Event()

    def _create_filename(self, data_folder=None):
        if not data_folder:
            data_folder = DEFAULT_DATA_FOLDER
        data_folder = Path(data_folder)

        filename = self._var["topic"]
        if not filename.endswith(".json"):
            filename += ".json"
        filename = Path(filename)

        filepath = Path(data_folder / filename)
        return filepath

    def load(self, data_folder=None):
        filename = self._create_filename(data_folder)
        if filename.exists():
            with self._lock:
                with open(filename, "r") as f:
                    self._var = json.load(f)
        else:
            print("WARNING: File '{}' does not exist.".format(filename))

    def save(self, data_folder=None):
        filename = self._create_filename(data_folder)
        filename.parent.mkdir(parents=True, exist_ok=True)
        print("saving to: '{}'".format(filename))
        with self._lock:
            with open(filename, "w") as f:
                json.dump(self._var, f, indent=2)

    def push_data(self, data, timestamp=None, **kwargs):
        """
        TODO
        - Currently, the time stamp will be created with system
        accuracy. This might lead to doubled entries when the data comes
        in too fast
        - There should be a check, that the timestamps are actually
        strictly monotonically increasing
        """
        if not timestamp:
            timestamp = int(time.time() * 1e9)
        # try to parse with json, if failed, just store the raw string
        try:
            data_entry = json.loads(data)
        except (json.decoder.JSONDecodeError, TypeError):
            data_entry = data
        with self._lock:
            self._var["data"][timestamp] = data_entry
            self._var["last_update"] = timestamp
            self._has_new_data.set()

    def has_new_data(self):
        return self._has_new_data.is_set()

    def set_as_read(self):
        self._has_new_data.clear()

    def get_data(self):
        with self._lock:
            self._has_new_data.clear()
            return self._var["data"]

    def get_last_datapoint(self):
        with self._lock:
            key = list(self._var["data"].keys())[-1]
            self._has_new_data.clear()
            return {key: self._var["data"].get(key)}

    def __str__(self):
        with self._lock:
            return json.dumps(self._var)

def ts_to_str(ts):
    ts = float(ts)/1e9
    ns = str(ts).split(".")[1]
    format_str = "%Y-%m-%d %H:%M:%S.{}".format(ns)
    res = time.strftime(format_str, time.localtime(ts))
    return res


def main():
    var = Variable("test/muh")
    var.load()
    s = time.time()
    for i in range(10):
        var.push_data(str(i))
        a =  time.time()
        while time.time() == a:
            pass
        # time.sleep(.000000000000001)
    delta = round(time.time() - s, 3)
    print("delta = " + str(delta) + " s")
    # ts = var._var["last_update"]
    # print(ts/1000000000)
    # print(ts_to_str(ts))
    # print(var)
    # print(var.get_data())
    s = time.time()
    print(var.get_last_datapoint())
    delta = round(time.time() - s, 3)
    print("delta = " + str(delta) + " s")
    print(len(var.get_data()))
    var.save()

if __name__ == "__main__":
    main()