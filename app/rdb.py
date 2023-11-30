from typing import Dict, BinaryIO, Optional, Tuple, Any, Union
from app.custom_types import BulkStringType
from io import SEEK_CUR, SEEK_SET
from app.utils import time_ms
import struct


class RdbFile:
    f: BinaryIO

    def __init__(self, f: BinaryIO):
        self.f = f

        if self.read_bytes(5) != b'REDIS':
            raise ValueError('Not an RDB file')

    @classmethod
    def load_data(cls, f: BinaryIO, store: Dict) -> None:
        rdb = cls(f)
        rdb.read_data(store)

    def read_data(self, store: Dict) -> None:
        self.move_set(9)

        while True:
            opcode = self.read_byte()

            if not opcode:
                break

            if opcode == b'\xFA': # Auxiliary field
                self.read_string() # Ignore key
                self.read_string() # Ignore value
            elif opcode == b'\xFE': # Database selector
                self.read_length() # Ignore database number
            elif opcode == b'\xFB': # Hash table sizes
                self.read_length() # Ignore database hash table size
                self.read_length() # Ignore expiry hash table size
            elif opcode == b'\xFD': # Key-Value pair: expire time in seconds
                expiry_s = self.read_uint32()

                key, value = self.read_key_value()

                self.store(store, key, value, expiry_s * 1000)
            elif opcode == b'\xFC': # Key-Value pair: expire time in milliseconds
                expiry_ms = self.read_uint64()

                key, value = self.read_key_value()

                self.store(store, key, value, expiry_ms)
            elif opcode == b'\xFF': # End of file
                self.read_remaining_bytes() # Ignore checksum
            else: # Key-Value pair without expiry
                self.move(-1)

                key, value = self.read_key_value()

                self.store(store, key, value)

    def store(self, store: Dict, key: str, value, expiry_ms: Optional[int] = None) -> None:
        if expiry_ms and expiry_ms <= time_ms():
            return

        store[key] = (value, expiry_ms)

    def read_key_value(self) -> Tuple[BulkStringType, BulkStringType]:
        value_type = RdbFile.byte_to_int(self.read_byte())

        key = BulkStringType(self.read_string())

        value = None

        if value_type == 0: # String
            value = BulkStringType(self.read_string())

        # Other types don't have to be implemented

        return key, value

    def read_string(self) -> Union[str, int]:
        special, value = self.read_length()

        if not special: # Length prefixed string
            return self.read_bytes(value).decode()
        else:
            if value == 0: # 8 bit integer
                return self.read_uint8()
            elif value == 1: # 16 bit integer
                return self.read_uint16()
            elif value == 2: # 32 bit integer
                return self.read_uint32()
            elif value == 3: # Compressed string
                compressed_len = self.read_length()[1]

                self.read_length() # Ignore uncompressed string length
                self.read_bytes(compressed_len) # Ignore compressed string

    def read_length(self) -> Tuple[bool, int]:
        byte_binary = RdbFile.byte_to_bits(self.read_byte())
        byte_binary_prefix = byte_binary[:2]
        byte_binary_value = byte_binary[2:]

        if byte_binary_prefix == '00':
            return False, RdbFile.bits_to_int(byte_binary_value)
        elif byte_binary_prefix == '01':
            return False, RdbFile.bits_to_int(byte_binary_value + RdbFile.byte_to_bits(self.read_byte()))
        elif byte_binary_prefix == '10':
            return False, self.read_uint32()
        elif byte_binary_prefix == '11':
            return True, RdbFile.bits_to_int(byte_binary_value)

    def read_uint8(self) -> int:
        return self.unpack('B')

    def read_uint16(self) -> int:
        return self.unpack('H', 2)

    def read_uint32(self) -> int:
        return self.unpack('I', 4)

    def read_uint64(self) -> int:
        return self.unpack('Q', 8)

    def read_remaining_bytes(self) -> bytes:
        return self.f.read()

    def read_bytes(self, size: int) -> bytes:
        return self.f.read(size)

    def read_byte(self) -> bytes:
        return self.read_bytes(1)

    def move(self, offset: int) -> None:
        self.f.seek(offset, SEEK_CUR)

    def move_set(self, offset: int) -> None:
        self.f.seek(offset, SEEK_SET)

    def unpack(self, fmt: str, size: int = 1) -> Any:
        ret = struct.unpack(
            f'<{fmt}',
            self.read_bytes(size)
        )

        return ret[0] if len(ret) == 1 else ret

    @staticmethod
    def byte_to_int(b) -> int:
        return int.from_bytes(b, byteorder='big')

    @staticmethod
    def byte_to_bits(b) -> str:
        return format(RdbFile.byte_to_int(b), '08b')

    @staticmethod
    def bits_to_int(b) -> int:
        return int(b, 2)
