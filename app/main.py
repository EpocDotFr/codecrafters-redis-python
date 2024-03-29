from app.handler import RESPHandler
from app.server import RedisServer
import argparse


def main() -> None:
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('--dir')
    arg_parser.add_argument('--dbfilename')
    arg_parser.add_argument('--port', type=int, default=6379)
    arg_parser.add_argument('--replicaof', nargs=2)

    args = arg_parser.parse_args()

    config = {}

    if args.dir:
        config['dir'] = args.dir

    if args.dbfilename:
        config['dbfilename'] = args.dbfilename

    if args.replicaof:
        config['replicaof'] = (args.replicaof[0], int(args.replicaof[1]))

    with RedisServer(('127.0.0.1', args.port), RESPHandler, config=config) as server:
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass


if __name__ == '__main__':
    main()
