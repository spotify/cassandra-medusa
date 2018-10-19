import socket
import json


# MedusaTransport is very similar to the original UDPTransport
# The difference is that here we call .endcode() on the string before putting it to socket
# It's a python3 thing :(
class MedusaTransport:
    def __init__(self, **kw):
        self._host = kw.get('host', '127.0.0.1')
        self._port = kw.get('port', 19000)
        self._t = (self._host, self._port)
        self._s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    def send_json(self, message):
        self._s.sendto(json.dumps(message).encode(), self._t)
