from time import time_ns
import random
import string

def time_ms() -> int:
    return time_ns() // 1000000


def rand_alnum(length: int) -> str:
    return ''.join(random.choice(string.ascii_lowercase + string.digits) for _ in range(length))
