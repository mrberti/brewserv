from acquisition import Acquisition

class BrewServ(object):
    def __init__(self):
        self.daq = Acquisition()

def main_test():
    brewserv = BrewServ()
    brewserv.daq.new_connection("192.168.1.80", user="simon", password="supipass")
    brewserv.daq.new_variable("test/esp32-001/temp")
    brewserv.daq.connect()
    brewserv.daq.acquire_forever()

if __name__ == "__main__":
    main_test()