from app.custom_types import SimpleStringType, BulkStringType, ErrorType
from socketserver import StreamRequestHandler
from app.serializer import Serializer
from app.server import RedisServer
from typing import List, Optional
from types import SimpleNamespace
from app.utils import time_ms


class RESPHandler(StreamRequestHandler):
    server: RedisServer
    serializer: Serializer

    def setup(self):
        super().setup()

        self.serializer = Serializer(self.rfile, self.wfile)

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
            frame = self.serializer.receive()

            if not frame:
                break

            request = self.serializer.unserialize(frame)

            if not request or not isinstance(request, list):
                raise ValueError('Expected non-empty array from client')

            for part in request:
                if not isinstance(part, BulkStringType):
                    raise ValueError('Expected non-empty array of bulk strings from client')

            command = request[0].upper()

            if command == 'COMMAND':
                break # Ignore COMMAND connection
            elif command == 'PING':
                self.serializer.send(SimpleStringType('PONG'))
            elif command == 'ECHO':
                args = self.parse_args(request, args=['message'])

                self.serializer.send(args.message)
            elif command == 'SET':
                args = self.parse_args(request, args=['key', 'value'], kvargs=['px'])

                self.server.store[args.key] = (
                    args.value,
                    (time_ms() + int(args.px)) if args.px else None
                )

                self.serializer.send(SimpleStringType('OK'))
            elif command == 'GET':
                args = self.parse_args(request, args=['key'])

                value, ttl = self.server.store.get(args.key, (None, None))

                if isinstance(ttl, int) and time_ms() >= ttl:
                    value = None

                self.serializer.send(BulkStringType(value) if value is not None else None)
            elif command == 'CONFIG':
                args = self.parse_args(request, args=['action', 'parameter', 'value'])

                if args.action is not None:
                    args.action = args.action.upper()

                if args.action == 'GET':
                    value = self.server.config.get(args.parameter)

                    self.serializer.send([
                        BulkStringType(args.parameter),
                        BulkStringType(value) if value is not None else None
                    ])
                elif args.action == 'SET':
                    self.server.config[args.parameter] = args.value

                    self.serializer.send(SimpleStringType('OK'))
                else:
                    self.serializer.send(ErrorType('Unknown CONFIG subcommand'))
            elif command == 'KEYS':
                args = self.parse_args(request, args=['pattern'])

                if args.pattern == '*':
                    self.serializer.send(list(self.server.store.keys()))
            elif command == 'INFO':
                replication_section = [
                    '# Replication',
                    f'role:{self.server.role}',
                    f'master_replid:{self.server.master_replid}',
                    f'master_repl_offset:{self.server.master_repl_offset}',
                ]

                self.serializer.send(BulkStringType('\n'.join(replication_section)))
            else:
                self.serializer.send(ErrorType('Unknown command'))
