#!/usr/bin/env python3
"""
run_local_demo.py - Run five nodes on one machine and drive them with commands.

Each node gets its own directory demo/nodeN/ (shared/, downloads/, log.txt)
and TCP port 5000N. Their terminal output is saved to demo/nodeN/terminal.txt
and interleaved, with a [nodeN] prefix, in demo/demo_output.txt.

    python3 demo/run_local_demo.py

The scenario covers everything the assignment asks to demonstrate:
discovery, file lists propagating, a successful transfer, the error reply for
a missing file, and a node leaving (its files vanish from the other tables).
"""

import shutil
import struct
import subprocess
import sys
import threading
import time
import zlib
from pathlib import Path

DEMO_DIR = Path(__file__).resolve().parent
AGENT = DEMO_DIR.parent / "agent.py"
NODES = 5


def tiny_png() -> bytes:
    """A 16x16 gradient PNG, so one sample file is real binary data."""
    def chunk(kind, data):
        body = kind + data
        return struct.pack("!I", len(data)) + body + struct.pack("!I", zlib.crc32(body))
    rows = b"".join(b"\x00" + bytes(c for x in range(16) for c in (x * 16, y * 16, 128))
                    for y in range(16))
    return (b"\x89PNG\r\n\x1a\n" + chunk(b"IHDR", struct.pack("!IIBBBBB", 16, 16, 8, 2, 0, 0, 0))
            + chunk(b"IDAT", zlib.compress(rows)) + chunk(b"IEND", b""))


SEED_FILES = {
    1: {"alpha.txt": b"Hello from node 1.\n", "gradient.png": tiny_png()},
    2: {"bravo.txt": b"Node 2 says hi.\n"},
    3: {"charlie.txt": b"Node 3 shares this file.\n",
        "old_draft.txt": b"This file will be deleted before anyone downloads it.\n"},
    4: {"delta.csv": b"id,name\n1,ada\n2,grace\n3,linus\n"},
    5: {"echo.txt": b"Echo from node 5 - this file travels across the network.\n"},
}


class DemoNode:
    """One agent process with a pipe for commands and a thread for output."""

    def __init__(self, n: int, transcript, lock):
        self.n = n
        self.dir = DEMO_DIR / f"node{n}"
        self._reset_dir()
        self.proc = subprocess.Popen(
            [sys.executable, "-u", str(AGENT), "--dir", str(self.dir), "--port", str(50000 + n)],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        self.terminal = open(self.dir / "terminal.txt", "w")
        self._reader = threading.Thread(target=self._pump, args=(transcript, lock), daemon=True)
        self._reader.start()

    def _reset_dir(self):
        shutil.rmtree(self.dir, ignore_errors=True)
        (self.dir / "shared").mkdir(parents=True)
        for name, data in SEED_FILES[self.n].items():
            (self.dir / "shared" / name).write_bytes(data)

    def _pump(self, transcript, lock):
        for line in self.proc.stdout:
            self.terminal.write(line)
            self.terminal.flush()
            with lock:
                stamp = time.strftime("%H:%M:%S")
                out = f"{stamp} [node{self.n}] {line}"
                transcript.write(out)
                transcript.flush()
                sys.stdout.write(out)

    def send(self, command: str):
        self.proc.stdin.write(command + "\n")
        self.proc.stdin.flush()

    def wait(self):
        self.proc.wait(timeout=10)
        self._reader.join(timeout=2)
        self.terminal.close()


def main():
    lock = threading.Lock()
    with open(DEMO_DIR / "demo_output.txt", "w") as transcript:
        def step(title):
            with lock:
                banner = f"\n===== {title} =====\n"
                transcript.write(banner)
                sys.stdout.write(banner)

        step("1. Discovery: start five nodes, one second apart")
        nodes = {}
        for n in range(1, NODES + 1):
            nodes[n] = DemoNode(n, transcript, lock)
            time.sleep(1)
        time.sleep(2)

        step("2. Peer table and file lists on node1")
        nodes[1].send("peers")
        time.sleep(0.5)
        nodes[1].send("files")
        time.sleep(1)

        step("3. File transfer: node2 downloads echo.txt (from node5), node4 downloads gradient.png from peer #1")
        nodes[2].send("get echo.txt")
        time.sleep(0.5)
        nodes[4].send("peers")
        time.sleep(0.5)
        nodes[4].send("get 1 gradient.png")
        time.sleep(1.5)
        nodes[2].send("mine")
        time.sleep(0.5)
        nodes[2].send("get echo.txt")          # already have it -> refused locally
        time.sleep(1)

        step("4. Error reply: node3 deletes old_draft.txt, node1 asks for it anyway")
        (nodes[3].dir / "shared" / "old_draft.txt").unlink()
        nodes[1].send("get old_draft.txt")
        time.sleep(1.5)

        step("5. Propagation: wait for the next 10-second FILE_LIST round")
        time.sleep(11)
        nodes[3].send("files")
        time.sleep(1)

        step("6. node5 leaves the network")
        nodes[5].send("quit")
        nodes[5].wait()
        time.sleep(1)
        nodes[1].send("files")
        time.sleep(0.5)
        nodes[3].send("peers")
        time.sleep(1)

        step("7. Shut down the remaining nodes")
        for n in range(1, NODES):
            nodes[n].send("quit")
            time.sleep(0.3)
        for n in range(1, NODES):
            nodes[n].wait()


if __name__ == "__main__":
    main()
