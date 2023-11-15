from socketserver import ThreadingTCPServer
from typing import Optional, Dict
import tempfile


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
