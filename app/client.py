from app.custom_types import SimpleStringType, BulkStringType
from app.serializer import Serializer
from typing import Tuple, BinaryIO
import socket


class RedisClient:
    server_address: Tuple[str, int]

    socket: socket.socket
    rfile: BinaryIO
    wfile: BinaryIO
    serializer: Serializer

    def __init__(self, server_address: Tuple[str, int]) -> None:
        self.server_address = server_address

        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.rfile = self.socket.makefile('rb', -1)
        self.wfile = self.socket.makefile('wb', 0)
        self.serializer = Serializer(self.rfile, self.wfile)

    def connect(self):
        self.socket.connect(self.server_address)

    def disconnect(self):
        self.socket.shutdown(2)
        self.socket.close()

    def ping(self) -> bool:
        self.serializer.send([BulkStringType('PING')])

        response = self.serializer.receive()

        return isinstance(response, SimpleStringType) and response == 'PONG'

    def replconf(self, param: str, value: str) -> bool:
        self.serializer.send([BulkStringType('REPLCONF'), BulkStringType(param), BulkStringType(value)])

        response = self.serializer.receive()

        return isinstance(response, SimpleStringType) and response == 'OK'

    def psync(self, replicationid: str, offset: str):
        self.serializer.send([BulkStringType('PSYNC'), BulkStringType(replicationid), BulkStringType(offset)])

        response = self.serializer.receive()

    def __enter__(self):
        self.connect()

        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.disconnect()
