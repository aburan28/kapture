#!/usr/bin/env python3
"""Exercise the actual Rust adapter with fragmented, interleaved exchanges."""
import hashlib
import struct
import subprocess
import sys


def frame(flow, lane, data):
    name = flow.encode()
    return struct.pack("<HIB", len(name), len(data), lane) + name + data


size = 128
body = b"x" * size
digest = hashlib.sha256(body).hexdigest()
data = bytearray()
for ident in [1, 2, 3]:
    content = body if ident != 3 else b"z" * size
    request = f"POST /bench HTTP/1.1\r\nHost: localhost\r\nX-Bench-Id: {ident}\r\nX-Bench-Phase: measure\r\nContent-Length: {size}\r\n\r\n".encode() + content
    data.extend(frame(str(ident), 0, request[:17]))
    data.extend(frame(str(ident), 0, request[17:]))
for ident in [2, 1, 3]:
    response = f"HTTP/1.1 204 No Content\r\nX-Bench-Id: {ident}\r\nX-Bench-SHA256: {digest}\r\n\r\n".encode()
    data.extend(frame(str(ident), 1, response[:23]))
    data.extend(frame(str(ident), 1, response[23:]))
result = subprocess.run([sys.argv[1], str(size), digest], input=bytes(data), stdout=subprocess.PIPE, check=True)
assert len(result.stdout) == 30, (len(result.stdout), "expected exactly three exchanges")
records = [struct.unpack("<QBB", result.stdout[n:n+10]) for n in range(0, 30, 10)]
assert records == [(2, 1, 1), (1, 1, 1), (3, 0, 1)], records
print("Rust adapter: interleaved IDs, split headers/body and negative body oracle passed")
