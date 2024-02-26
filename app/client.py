from app.custom_types import SimpleStringType, BulkStringType
from typing import Tuple, BinaryIO, Union
from app.serializer import Serializer
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

    def send(self, *args):
        self.serializer.send([BulkStringType(arg) for arg in args])

        frame = self.serializer.receive()

        if not frame:
            return False

        return self.serializer.unserialize(frame)

    def ping(self) -> bool:
        response = self.send('PING')

        return isinstance(response, SimpleStringType) and response == 'PONG'

    def replconf(self, parameter: str, value: Union[int, str]) -> bool:
        response = self.send('REPLCONF', parameter, value)

        return isinstance(response, SimpleStringType) and response == 'OK'

    def psync(self, replicationid: str, offset: int):
        response = self.send('PSYNC', replicationid, offset)

    def __enter__(self):
        self.connect()

        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.disconnect()
