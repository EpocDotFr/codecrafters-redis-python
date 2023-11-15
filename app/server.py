from socketserver import ThreadingTCPServer
from typing import Optional, Dict
from app.rdb import RdbFile
import tempfile
import os


class RedisServer(ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True
    config: Dict
    store: Dict

    def __init__(self, *args, config: Optional[Dict] = None, **kvargs):
        super().__init__(*args, **kvargs)

        self.config = {
            'dir': tempfile.gettempdir(),
            'dbfilename': 'dump.rdb',
        }

        if config:
            self.config.update(config)

        self.store = {}

        rdb_filename = os.path.join(self.config.get('dir'), self.config.get('dbfilename'))

        try:
            with open(rdb_filename, 'rb') as f:
                RdbFile.load_data(f, self.store)
        except FileNotFoundError:
            pass
