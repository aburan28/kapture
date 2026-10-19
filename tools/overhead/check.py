#!/usr/bin/env python3
"""Exercise the actual Rust adapter with fragmented, interleaved exchanges."""
import hashlib
import base64
import json
import tempfile
from pathlib import Path
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

# Test the post-window evidence checker independently of the live collector.
# A correct request projection must not hide a response paired with another ID.
sys.dont_write_bytecode = True
from run import verify_evidence

with tempfile.TemporaryDirectory() as directory:
    root = Path(directory)
    path = root / "evidence/benchmark/capture/epoch/one.json"
    path.parent.mkdir(parents=True)
    exchange = {
        "request": {"method": "POST", "path": "/bench", "complete": True,
                    "headers": {"X-Bench-Id": ["1"], "X-Bench-Phase": ["measure"], "Authorization": ["[REDACTED]"]},
                    "body": base64.b64encode(body).decode()},
        "response": {"complete": True, "status": 204,
                     "headers": {"X-Bench-Id": ["1"], "X-Bench-SHA256": [digest]}},
        "quality": {"framingComplete": True}}
    path.write_text(json.dumps(exchange))
    assert verify_evidence(root, size, 1)["complete"]
    exchange["response"]["headers"]["X-Bench-Id"] = ["2"]
    path.write_text(json.dumps(exchange))
    checked = verify_evidence(root, size, 1)
    assert checked["invalid"] == 1 and not checked["complete"], checked
print("Evidence oracle: valid durable pairs accepted; mismatched response rejected")
