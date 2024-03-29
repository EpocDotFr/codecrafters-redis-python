from socketserver import ThreadingTCPServer
from app.client import RedisClient
from typing import Optional, Dict
from app.utils import rand_alnum
from app.rdb import RdbFile
import tempfile
import os


class RedisServer(ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True
    config: Dict
    store: Dict
    role: str
    master_replid: str = ''
    master_repl_offset: int = 0

    def __init__(self, *args, config: Optional[Dict] = None, **kvargs):
        super().__init__(*args, **kvargs)

        self.config = {
            'dir': tempfile.gettempdir(),
            'dbfilename': 'dump.rdb',
            'replicaof': None,
            'replication': {}
        }

        if config:
            self.config.update(config)

        self.role = 'slave' if self.config['replicaof'] is not None else 'master'

        if self.role == 'master':
            self.master_replid = rand_alnum(40)
        elif self.role == 'slave':
            try:
                with RedisClient(self.config['replicaof']) as client:
                    client.ping()
                    client.replconf('listening-port', self.server_address[1])
                    client.replconf('capa', 'psync2')
                    # client.psync(self.master_replid or '?', self.master_repl_offset or -1)
            except (BrokenPipeError, OSError):
                pass

        self.store = {}

        rdb_filename = os.path.join(self.config.get('dir'), self.config.get('dbfilename'))

        try:
            with open(rdb_filename, 'rb') as f:
                RdbFile.load_data(f, self.store)
        except FileNotFoundError:
            pass
