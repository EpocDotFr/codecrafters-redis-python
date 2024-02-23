from typing import Tuple, Optional, BinaryIO
from app.serializer import Serializer
import socket


class RedisClient:
    server_address: Tuple[str, int]
    serializer: Serializer
    socket: socket.socket
    rfile: BinaryIO
    wfile: BinaryIO

    def __init__(self, server_address: Optional[Tuple[str, int]] = None) -> None:
        self.server_address = server_address or ('127.0.0.1', 6379)

        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def connect(self):
        self.socket.connect(self.server_address)

        self.rfile = self.socket.makefile('rb', -1)
        self.wfile = self.socket.makefile('wb', 0)

        self.serializer = Serializer(self.rfile, self.wfile)

    def disconnect(self):
        self.socket.shutdown(2)
        self.socket.close()

    def __enter__(self):
        self.connect()

        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.disconnect()
