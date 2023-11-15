from app.custom_types import SimpleStringType, BulkStringType, ErrorType
from socketserver import StreamRequestHandler
from typing import Union, List, Optional
from types import SimpleNamespace
from server import RedisServer
from utils import time_ms


class RESPHandler(StreamRequestHandler):
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

    def parse_args(self, request: List[BulkStringType], args: Optional[List[str]] = None, kvargs: Optional[List[str]] = None) -> SimpleNamespace:
        all_args = request[1:]
        args = args if args is not None else []
        kvargs = kvargs if kvargs is not None else []

        ns = SimpleNamespace()

        for i, arg in enumerate(args):
            try:
                value = all_args[i]
            except IndexError:
                value = None

            setattr(ns, arg, value)

        for kvarg in kvargs:
            try:
                kvarg_index = all_args.index(BulkStringType(kvarg)) # Locate the arg name

                value = all_args[kvarg_index + 1] # Arg value is found just after the named arg
            except (ValueError, IndexError):
                value = None

            setattr(ns, kvarg, value)

        return ns

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

            command = request[0].upper()

            if command == 'COMMAND':
                break # Ignore COMMAND connection
            elif command == 'PING':
                self.send(SimpleStringType('PONG'))
            elif command == 'ECHO':
                args = self.parse_args(request, args=['message'])

                self.send(args.message)
            elif command == 'SET':
                args = self.parse_args(request, args=['key', 'value'], kvargs=['px'])

                self.server.store[args.key] = (
                    args.value,
                    (time_ms() + int(args.px)) if args.px else None
                )

                self.send(SimpleStringType('OK'))
            elif command == 'GET':
                args = self.parse_args(request, args=['key'])

                value, ttl = self.server.store.get(args.key, (None, None))

                if isinstance(ttl, int) and time_ms() >= ttl:
                    value = None

                self.send(BulkStringType(value) if value is not None else None)
            elif command == 'CONFIG':
                args = self.parse_args(request, args=['action', 'parameter', 'value'])

                if args.action is not None:
                    args.action = args.action.upper()

                if args.action == 'GET':
                    if not args.parameter:
                        self.send(ErrorType('CONFIG GET missing parameter name'))
                    else:
                        value = self.server.config.get(args.parameter)

                        self.send([
                            BulkStringType(args.parameter),
                            BulkStringType(value) if value is not None else None
                        ])
                elif args.action == 'SET':
                    if not args.parameter:
                        self.send(ErrorType('CONFIG SET missing parameter name'))
                    elif not args.value:
                        self.send(ErrorType('CONFIG SET missing parameter value'))
                    else:
                        self.server.config[args.parameter] = args.value

                        self.send(SimpleStringType('OK'))
                else:
                    self.send(ErrorType('Unknown CONFIG subcommand'))
            else:
                self.send(ErrorType('Unknown command'))

    def unpack(self, frame: str) -> Optional[Union[List, BulkStringType, SimpleStringType, ErrorType, int]]:
        frame_type, body = frame[0], frame[1:]

        if frame_type == '*': # Array
            length = int(body)

            return [
                self.unpack(self.receive()) for _ in range(length)
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
