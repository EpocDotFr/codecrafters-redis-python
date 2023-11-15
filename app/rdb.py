from typing import Dict, BinaryIO, Optional
import struct
import os


class RdbFile:
    default_bsa: str = '>'
    f: BinaryIO

    def __init__(self, f: BinaryIO):
        self.f = f

    @classmethod
    def load_data(cls, f: BinaryIO, store: Dict):
        rdb = cls(f)
        rdb.read_data(store)

    def read_data(self, store: Dict):
        if self.read_bytes(5) != b'REDIS':
            raise ValueError('Not an RDB file')

        self.jump(4) # Ignore version

        while True:
            opcode = self.read_byte()

            if not opcode:
                break

            if opcode == 0xFA: # Auxiliary fields
                pass
            elif opcode == 0xFE: # Database selector
                pass
            elif opcode == 0xFB: # Hash table sizes
                pass
            elif opcode == 0xFD: # Key-Value pair: expire time in seconds
                pass
            elif opcode == 0xFC: # Key-Value pair: expire time in milliseconds
                pass
            elif opcode == 0xFF: # End of file
                pass

    def jump(self, size: int):
        self.f.seek(size, os.SEEK_CUR)

    def read_bytes(self, size: int):
        return self.f.read(size)

    def read_byte(self):
        return self.read_bytes(1)

    def unpack(self, fmt: str, size: int = 1, bsa: Optional[str] = None):
        ret = struct.unpack(
            ''.join([
                bsa or self.default_bsa or '',
                fmt
            ]),
            self.read_bytes(size)
        )

        return ret[0] if len(ret) == 1 else ret
