# Standard imports
import abc
import threading

# BrewServ
from variable import Variable


class MetaConnection(abc.ABC):
    """Base class for handling a connection to a data service."""
    def __init__(self, **kwargs):
        self._variables = dict()
        self._has_new_data_event = threading.Event()
        self._recent_write_variables = list()

    def set_notify_cb(self, callback):
        self._notify_cb = callback

    def add_variable(self, topic, **kwargs):
        """Create a new variable and start subscribing to it. If the
        specific data service requires subscriptions after connection,
        use `subscribe_to_variables()` after establishing a connection.
        TODO
        - Add load and store parameters
        - Add origin
        """
        if topic not in self._variables:
            var = Variable(topic, **kwargs)
            self._variables[topic] = var
            self.subscribe(topic)
        else:
            var = self._variables[topic]
        return var

    def subscribe_to_variables(self):
        """This convenience function subscribes to all variables which
        were previously added. It might be necessary for the data
        service to subscribe after the connection was established. So
        this function comes in handy."""
        for topic in self._variables.keys():
            self.subscribe(topic)

    def push_data(self, topic, data):
        """`data` must be a string."""
        if topic in self._variables:
            var = self._variables[topic]
            var.push_data(data)
            self._has_new_data_event.set()
            self._recent_write_variables.append(var)
            self.notify({"message": "new data"})

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

    def notify(self, message):
        notification = ConnectionNotification(self, message)
        if self._notify_cb:
            self._notify_cb(notification)

    def get_variables(self):
        """Return a list with references to all variables."""
        return [self._variables.values()]

    # The following abstract methods need to be implemented in the specific 
    # data service handler.
    @abc.abstractmethod
    def connect(self):
        pass

    @abc.abstractmethod
    def start(self):
        pass

    @abc.abstractmethod
    def subscribe(self, topic):
        pass

    @abc.abstractmethod
    def stop(self):
        pass

    @abc.abstractmethod
    def disconnect(self):
        pass

class ConnectionNotification(object):
    def __init__(self, connection, message):
        self.connection = connection
        self.message = message

    def __str__(self):
        return str(self.message)
        # return str(self) + " " + str(self.message)
