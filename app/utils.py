from time import time_ns


def time_ms() -> int:
    return time_ns() // 1000000
