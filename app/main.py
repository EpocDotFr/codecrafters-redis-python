from typing import Union, List, Optional, Dict
from argparse import Namespace
from time import time
import socketserver


class SimpleStringType(str):
    pass


class BulkStringType(str):
    pass


class ErrorType(str):
    pass


class RedisServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True
    store: Dict

    def __init__(self, *args, **kvargs):
        super().__init__(*args, **kvargs)

        self.store = {}


class RESPHandler(socketserver.StreamRequestHandler):
    server: RedisServer

    def receive(self, length: Optional[int] = None) -> str:
        if length:
            frame = self.rfile.read(length + 2).decode() # Also read \r\n termination chars
        else:
            frame = self.rfile.readline().decode()

        if not frame:
            return ''

        return frame[:-2] # Remove \r\n termination chars

    def send(self, data: Optional[Union[List, BulkStringType, SimpleStringType, ErrorType, int]]) -> int:
        return self.wfile.write(self.pack(data).encode())

    def parse_args(self, request: List[BulkStringType], args: Optional[List[str]] = None, kvargs: Optional[List[str]] = None) -> Namespace:
        all_args = request[1:]
        args = args if args is not None else []
        kvargs = kvargs if kvargs is not None else []

        ret = Namespace()

        for i, arg in enumerate(args):
            try:
                value = all_args[i]
            except IndexError:
                value = None

            setattr(ret, arg, value)

        for kvarg in kvargs:
            try:
                kvarg_index = all_args.index(BulkStringType(kvarg)) # Locate the arg name

                value = all_args[kvarg_index + 1] # Arg value is found just after the named arg
            except (ValueError, IndexError):
                value = None

            setattr(ret, kvarg, value)

        return ret

    def handle(self) -> None:
        while True:
            frame = self.receive()

            if not frame:
                break

            request = self.unpack(frame)

            if not request or not isinstance(request, list):
                raise ValueError('Expected non-empty array from client')

            for part in request:
                if not isinstance(part, BulkStringType):
                    raise ValueError('Expected non-empty array of bulk strings from client')

            command = request[0]

            if command == 'COMMAND':
                break # Ignore COMMAND connection
            elif command == 'PING':
                self.send(SimpleStringType('PONG'))
            elif command == 'ECHO':
                args = self.parse_args(request, args=['message'])

                self.send(args.message)
            elif command == 'SET':
                args = self.parse_args(request, args=['key', 'value'], kvargs=['PX'])

                self.server.store[args.key] = (
                    args.value,
                    (int(time()) + int(int(args.PX) / 1000)) if args.PX else None
                )

                self.send(SimpleStringType('OK'))
            elif command == 'GET':
                args = self.parse_args(request, args=['key'])

                value, ttl = self.server.store.get(args.key, (None, None))

                if isinstance(ttl, int) and int(time()) >= ttl:
                    value = None

                self.send(BulkStringType(value) if value else None)
            else:
                raise ValueError('Unknown command')

    def unpack(self, frame: str) -> Optional[Union[List, BulkStringType, SimpleStringType, ErrorType, int]]:
        type, body = frame[0], frame[1:]

        if type == '*': # Array
            length = int(body)

            return [
                self.unpack(self.receive()) for _ in range(length)
            ]
        elif type == '$': # Bulk string
            length = int(body)

            if length == -1: # Null bulk string
                return None

            return BulkStringType(self.receive(length))
        elif type == ':': # Integer
            return int(body)
        elif type == '-': # Error
            return ErrorType(body)
        elif type == '+': # Simple string
            return SimpleStringType(body)

        raise ValueError('Unknown type')

    def pack(self, data: Optional[Union[List, BulkStringType, SimpleStringType, ErrorType, int]]) -> str:
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
                    self.pack(item) for item in data
                ])
            )

        if not frame:
            raise ValueError('Unhandled type')

        return frame + '\r\n'


def main() -> None:
    with RedisServer(('127.0.0.1', 6379), RESPHandler) as server:
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass

if __name__ == '__main__':
    main()
