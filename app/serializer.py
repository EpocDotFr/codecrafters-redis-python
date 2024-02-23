from app.custom_types import SimpleStringType, BulkStringType, ErrorType
from typing import BinaryIO, Optional, Union, List


class Serializer:
    rfile: BinaryIO
    wfile: BinaryIO

    def __init__(self, rfile: BinaryIO, wfile: BinaryIO):
        self.rfile = rfile
        self.wfile = wfile

    def receive(self, length: Optional[int] = None) -> str:
        if length:
            frame = self.rfile.read(length + 2).decode() # Also read \r\n termination chars
        else:
            frame = self.rfile.readline().decode()

        if not frame:
            return ''

        return frame[:-2] # Remove \r\n termination chars

    def send(self, data: Optional[Union[List, BulkStringType, SimpleStringType, ErrorType, int]]) -> int:
        return self.wfile.write(self.serialize(data).encode())

    def unserialize(self, frame: str) -> Optional[Union[List, BulkStringType, SimpleStringType, ErrorType, int]]:
        frame_type, body = frame[0], frame[1:]

        if frame_type == '*': # Array
            length = int(body)

            return [
                self.unserialize(self.receive()) for _ in range(length)
            ]
        elif frame_type == '$': # Bulk string
            length = int(body)

            if length == -1: # Null bulk string
                return None

            return BulkStringType(self.receive(length))
        elif frame_type == ':': # Integer
            return int(body)
        elif frame_type == '-': # Error
            return ErrorType(body)
        elif frame_type == '+': # Simple string
            return SimpleStringType(body)

        raise ValueError(f'Unknown frame type: {frame_type}')

    def serialize(self, data: Optional[Union[List, BulkStringType, SimpleStringType, ErrorType, int]]) -> str:
        frame = ''

        if data is None: # Null bulk string
            frame = '$-1'
        elif isinstance(data, BulkStringType): # Bulk string
            frame = f'${len(data)}\r\n{data}'
        elif isinstance(data, SimpleStringType): # Simple string
            frame = f'+{data}'
        elif isinstance(data, ErrorType): # Error
            frame = f'-{data}'
        elif isinstance(data, int): # Integer
            frame = f':{data}'
        elif isinstance(data, list): # Array
            frame = '*{}\r\n{}'.format(
                len(data),
                ''.join([
                    self.serialize(item) for item in data
                ])
            )

        if not frame:
            raise ValueError('Unhandled data type: {}'.format(type(data)))

        return frame + '\r\n'
