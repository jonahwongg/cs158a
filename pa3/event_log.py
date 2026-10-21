"""
event_log.py - Thread-safe writer for log.txt.

Every line has the same columns so the log is easy to read and grep:

    TIMESTAMP                DIRECTION  PEER ADDRESS           TYPE           DETAIL
    2026-10-20 23:01:02.123  SENT       192.168.1.5:50001      FILE_LIST      files=3

Peer table changes use the directions PEER_ADDED / PEER_REMOVED.
"""

import threading
from datetime import datetime


class EventLog:
    """Appends timestamped lines to a log file from any thread."""

    def __init__(self, path):
        self._lock = threading.Lock()
        # line-buffered so the log is readable while the node is running
        self._file = open(path, "a", encoding="utf-8", buffering=1)

    def _write(self, direction: str, addr: str, kind: str, detail: str = "") -> None:
        stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        line = f"{stamp}  {direction:<12} {addr:<22} {kind:<13} {detail}".rstrip()
        with self._lock:
            if not self._file.closed:
                self._file.write(line + "\n")

    # --- message traffic ---------------------------------------------------

    def sent(self, addr: str, msg_type: str, detail: str = "") -> None:
        self._write("SENT", addr, msg_type, detail)

    def received(self, addr: str, msg_type: str, detail: str = "") -> None:
        self._write("RECEIVED", addr, msg_type, detail)

    # --- peer table changes ------------------------------------------------

    def peer_added(self, addr: str, detail: str) -> None:
        self._write("PEER_ADDED", addr, "-", detail)

    def peer_removed(self, addr: str, detail: str) -> None:
        self._write("PEER_REMOVED", addr, "-", detail)

    # --- everything else (startup, errors, completed transfers) ------------

    def info(self, text: str, addr: str = "-") -> None:
        self._write("INFO", addr, "-", text)

    def close(self) -> None:
        with self._lock:
            self._file.close()
