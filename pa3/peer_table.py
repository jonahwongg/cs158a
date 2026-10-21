"""
peer_table.py - The shared table of connected peers and their file lists.

The UDP thread, the accept thread, every per-peer receive thread, the
file-list timer thread and the user's CLI all touch this table, so every
access goes through one re-entrant lock and callers only ever get snapshots.
"""

import socket
import threading
from dataclasses import dataclass, field
from typing import Optional


@dataclass(eq=False)
class Peer:
    """One live TCP connection to another node."""

    peer_id: int                  # small local number shown to the user
    sock: socket.socket
    host: str                     # peer's IP address
    port: int                     # peer's listening TCP port (once known)
    initiated: bool               # True if WE dialed (we are the TCP client)
    node_id: Optional[str] = None  # learned from PEER_ACK or first FILE_LIST
    files: list = field(default_factory=list)
    send_lock: threading.Lock = field(default_factory=threading.Lock)

    @property
    def addr(self) -> str:
        return f"{self.host}:{self.port}"


class PeerTable:
    """Thread-safe mapping peer_id -> Peer, plus the set of in-progress dials."""

    def __init__(self):
        self._lock = threading.RLock()
        self._peers = {}          # peer_id -> Peer
        self._dialing = set()     # node_ids we are currently connecting to
        self._next_id = 1

    # --- membership ----------------------------------------------------------

    def add(self, sock, host, port, initiated, node_id=None) -> Peer:
        with self._lock:
            peer = Peer(self._next_id, sock, host, port, initiated, node_id)
            self._peers[peer.peer_id] = peer
            self._next_id += 1
            return peer

    def remove(self, peer: Peer) -> bool:
        """Remove a peer and its file list. Returns False if already gone."""
        with self._lock:
            return self._peers.pop(peer.peer_id, None) is not None

    def contains(self, peer: Peer) -> bool:
        with self._lock:
            return peer.peer_id in self._peers

    def get(self, peer_id: int) -> Optional[Peer]:
        with self._lock:
            return self._peers.get(peer_id)

    def all(self) -> list:
        with self._lock:
            return sorted(self._peers.values(), key=lambda p: p.peer_id)

    def __len__(self):
        with self._lock:
            return len(self._peers)

    # --- dial bookkeeping (prevents a second connection to the same node) -----

    def begin_dial(self, node_id: str) -> bool:
        """Claim the right to dial node_id. False if connected or already dialing."""
        with self._lock:
            connected = any(p.node_id == node_id for p in self._peers.values())
            if connected or node_id in self._dialing:
                return False
            self._dialing.add(node_id)
            return True

    def end_dial(self, node_id: str) -> None:
        with self._lock:
            self._dialing.discard(node_id)

    # --- identity and file lists ---------------------------------------------

    def identify(self, peer: Peer, node_id: str, port: int) -> Optional[Peer]:
        """Record who is behind a connection (from its FILE_LIST).

        Returns another connection to the same node, if one exists, so the
        caller can resolve the duplicate.
        """
        with self._lock:
            peer.node_id = node_id
            peer.port = port
            for other in self._peers.values():
                if other is not peer and other.node_id == node_id:
                    return other
            return None

    def set_files(self, peer: Peer, files: list) -> bool:
        """Replace a peer's advertised list. Returns True if it changed."""
        with self._lock:
            new = sorted(set(files))
            changed = new != peer.files
            peer.files = new
            return changed

    def peers_with_file(self, filename: str) -> list:
        with self._lock:
            return [p for p in self.all() if filename in p.files]
