import socketserver


class RedisServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


class RESPHandler(socketserver.StreamRequestHandler):
    def handle(self):
        self.wfile.write('+PONG\r\n'.encode())


def main():
    with RedisServer(('127.0.0.1', 6379), RESPHandler) as server:
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass

if __name__ == "__main__":
    main()
