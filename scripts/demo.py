#!/usr/bin/env python3
"""Demo: hit the LB N times, print which backend served each conn."""
import socket
import sys

HOST, PORT = "127.0.0.1", 9000
N = int(sys.argv[1]) if len(sys.argv) > 1 else 6

for i in range(N):
    s = socket.create_connection((HOST, PORT), timeout=3)
    s.settimeout(3)
    f = s.makefile()
    print(f"conn{i}: {f.readline().strip()}")
    s.close()
