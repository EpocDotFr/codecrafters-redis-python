from typing import Union, List, Optional
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


class RESPHandler(socketserver.StreamRequestHandler):
    def receive(self, length: Optional[int] = None) -> str:
        if length:
            frame = self.rfile.read(length + 2).decode() # Also read \r\n termination chars
        else:
            frame = self.rfile.readline().decode()

        if not frame:
            return ''

        return frame[:-2] # Remove \r\n termination chars

    def send(self, data: Optional[Union[List, BulkStringType, SimpleStringType, ErrorType, int]]) -> None:
        self.wfile.write(self.pack(data).encode())

    def handle(self) -> None:
        while True:
            frame = self.receive()

            if not frame:
                break

            request = self.unpack(frame)

            if not isinstance(request, list):
                raise ValueError('Expected array from client')

            command, args = request[0], request[1:]

            if command == 'PING':
                if not args:
                    self.send(SimpleStringType('PONG'))
                else:
                    self.send(BulkStringType(args[0]))
            elif command == 'ECHO':
                self.send(BulkStringType(args[0]))
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


def main():
    with RedisServer(('127.0.0.1', 6379), RESPHandler) as server:
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass

if __name__ == '__main__':
    main()
