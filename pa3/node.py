"""
node.py - The P2P node: discovery, connections, file lists and transfers.

Threads started by P2PNode.start():

    udp-listener    answers PEER_REQUEST broadcasts on UDP 54321 (lifetime of node)
    tcp-accept      accepts TCP connections from nodes that received our PEER_ACK
    file-list       sends our FILE_LIST to every peer every 10 seconds
    discovery       broadcasts PEER_REQUEST at startup and dials each PEER_ACK
    peer-N          one receive loop per connected peer

The node never prints directly; user-visible events go through the
`on_event` callback so the CLI decides how to show them.
"""

import os
import shutil
import socket
import threading
import time
import uuid
from pathlib import Path

import protocol as proto
from event_log import EventLog
from peer_table import Peer, PeerTable

FILE_LIST_INTERVAL = 10.0   # seconds between periodic FILE_LIST messages
DISCOVERY_ROUNDS = 3        # PEER_REQUEST broadcasts at startup (UDP may drop one)
DISCOVERY_WAIT = 1.0        # seconds to collect PEER_ACKs after each broadcast
CONNECT_TIMEOUT = 5.0


def safe_filename(name) -> str | None:
    """Return name if it is a plain file name (no paths, not hidden), else None."""
    if (not isinstance(name, str) or not name or name.startswith(".")
            or os.path.basename(name) != name or "\\" in name):
        return None
    return name


def local_ip_toward(remote_ip: str) -> str:
    """The IP of our interface that routes to remote_ip (no packets are sent)."""
    probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        probe.connect((remote_ip, 9))
        return probe.getsockname()[0]
    except OSError:
        return "127.0.0.1"
    finally:
        probe.close()


class P2PNode:
    """One peer in the network: both a TCP server and a TCP client."""

    def __init__(self, base_dir=".", tcp_port=0, broadcast_addr="255.255.255.255",
                 on_event=print):
        self.node_id = uuid.uuid4().hex[:8]        # tells our broadcasts apart
        self.base_dir = Path(base_dir)
        self.shared_dir = self.base_dir / "shared"
        self.downloads_dir = self.base_dir / "downloads"
        self.shared_dir.mkdir(parents=True, exist_ok=True)
        self.downloads_dir.mkdir(parents=True, exist_ok=True)

        self.requested_port = tcp_port
        self.tcp_port = None
        self.broadcast_addr = broadcast_addr
        self.on_event = on_event

        self.log = EventLog(self.base_dir / "log.txt")
        self.peers = PeerTable()
        self._stop = threading.Event()
        self._downloads = {}                        # filename -> peer_id in flight
        self._downloads_lock = threading.Lock()
        self._tcp_server = None
        self._udp_sock = None

    # =====================================================================
    # Lifecycle
    # =====================================================================

    def start(self) -> None:
        """Open sockets, start the background threads and run discovery."""
        self._tcp_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._tcp_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._tcp_server.bind(("", self.requested_port))
        self._tcp_server.listen()
        self.tcp_port = self._tcp_server.getsockname()[1]

        self._udp_sock = self._open_udp_listener()

        self.log.info(f"node {self.node_id} started; TCP port {self.tcp_port}, "
                      f"UDP port {proto.UDP_PORT}, shared={self.local_files()}")

        self._spawn(self._udp_listen_loop, "udp-listener")
        self._spawn(self._accept_loop, "tcp-accept")
        self._spawn(self._file_list_loop, "file-list")
        self.discover()

    def stop(self) -> None:
        """Leave the network: close every socket. There is no goodbye message;
        peers notice the closed sockets on their read path."""
        if self._stop.is_set():
            return
        self._stop.set()
        self.log.info("node shutting down")
        for sock in (self._tcp_server, self._udp_sock):
            try:
                sock.close()
            except (OSError, AttributeError):
                pass
        for peer in self.peers.all():
            self._drop_peer(peer, "local node shutting down")
        self.log.close()

    def discover(self) -> None:
        """Broadcast PEER_REQUEST (in the background) and dial whoever answers."""
        self._spawn(self._discovery, "discovery")

    @staticmethod
    def _spawn(target, name, *args) -> None:
        threading.Thread(target=target, args=args, name=name, daemon=True).start()

    @staticmethod
    def _open_udp_listener() -> socket.socket:
        """Bind UDP 54321. SO_REUSEPORT lets several nodes share one machine."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if hasattr(socket, "SO_REUSEPORT"):
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        sock.bind(("", proto.UDP_PORT))
        return sock

    # =====================================================================
    # Discovery (UDP)
    # =====================================================================

    def _discovery(self) -> None:
        """Send PEER_REQUEST from a fresh socket and collect PEER_ACKs on it.

        The request goes to UDP port 54321. Replies come back to this socket's
        own (ephemeral) port, so they reach this node even when several nodes
        share port 54321 on one machine.
        """
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        sock.bind(("", 0))
        sock.settimeout(0.2)
        request = proto.encode(proto.peer_request(self.node_id, self.tcp_port))
        target = (self.broadcast_addr, proto.UDP_PORT)
        try:
            for _ in range(DISCOVERY_ROUNDS):
                if self._stop.is_set():
                    return
                sock.sendto(request, target)
                self.log.sent(f"{target[0]}:{target[1]}", proto.PEER_REQUEST,
                              f"node={self.node_id} tcp_port={self.tcp_port}")
                self._collect_acks(sock, DISCOVERY_WAIT)
        except OSError as exc:
            self.log.info(f"discovery failed: {exc}")
            self.on_event(f"[!] discovery failed: {exc}")
        finally:
            sock.close()

    def _collect_acks(self, sock: socket.socket, duration: float) -> None:
        """Read PEER_ACKs for `duration` seconds and dial each new peer."""
        deadline = time.monotonic() + duration
        while time.monotonic() < deadline and not self._stop.is_set():
            try:
                data, addr = sock.recvfrom(65535)
                msg = proto.decode(data)
            except socket.timeout:
                continue
            except proto.ProtocolError as exc:
                self.log.info(f"ignored bad datagram: {exc}", f"{addr[0]}:{addr[1]}")
                continue
            if msg["type"] != proto.PEER_ACK or msg.get("node_id") == self.node_id:
                continue
            self.log.received(f"{addr[0]}:{addr[1]}", proto.PEER_ACK,
                              f"node={msg.get('node_id')} ip={msg.get('ip')} "
                              f"tcp_port={msg.get('tcp_port')}")
            host = msg.get("ip") or addr[0]
            self._spawn(self._connect_to, "dial", host, int(msg["tcp_port"]),
                        str(msg["node_id"]))

    def _udp_listen_loop(self) -> None:
        """Answer every PEER_REQUEST for as long as the node runs, so nodes
        that join later can find us."""
        while not self._stop.is_set():
            try:
                data, addr = self._udp_sock.recvfrom(65535)
            except OSError:
                break                                    # socket closed by stop()
            src = f"{addr[0]}:{addr[1]}"
            try:
                msg = proto.decode(data)
            except proto.ProtocolError as exc:
                self.log.info(f"ignored bad datagram: {exc}", src)
                continue
            if msg.get("node_id") == self.node_id:
                continue                                 # our own broadcast
            if msg["type"] != proto.PEER_REQUEST:
                continue
            self.log.received(src, proto.PEER_REQUEST,
                              f"node={msg.get('node_id')} tcp_port={msg.get('tcp_port')}")
            self._send_ack(addr)

    def _send_ack(self, addr) -> None:
        """Reply to a PEER_REQUEST with our IP and TCP port (unicast)."""
        ack = proto.peer_ack(self.node_id, local_ip_toward(addr[0]), self.tcp_port)
        try:
            self._udp_sock.sendto(proto.encode(ack), addr)
            self.log.sent(f"{addr[0]}:{addr[1]}", proto.PEER_ACK,
                          f"ip={ack['ip']} tcp_port={self.tcp_port}")
        except OSError as exc:
            self.log.info(f"could not send PEER_ACK: {exc}", f"{addr[0]}:{addr[1]}")

    # =====================================================================
    # Connections (TCP)
    # =====================================================================

    def _connect_to(self, host: str, port: int, node_id: str) -> None:
        """We sent the PEER_REQUEST, so we are the TCP client."""
        if not self.peers.begin_dial(node_id):
            return                                     # already connected/dialing
        try:
            sock = socket.create_connection((host, port), timeout=CONNECT_TIMEOUT)
            sock.settimeout(None)
        except OSError as exc:
            self.log.info(f"connect to node {node_id} failed: {exc}", f"{host}:{port}")
            self.on_event(f"[!] could not connect to {host}:{port}: {exc}")
            self.peers.end_dial(node_id)
            return
        peer = self.peers.add(sock, host, port, initiated=True, node_id=node_id)
        self.peers.end_dial(node_id)
        self._on_connected(peer)

    def _accept_loop(self) -> None:
        """The node that sent the PEER_ACK is the TCP server."""
        while not self._stop.is_set():
            try:
                sock, (host, port) = self._tcp_server.accept()
            except OSError:
                break                                    # socket closed by stop()
            # The real listening port arrives in the peer's first FILE_LIST.
            peer = self.peers.add(sock, host, port, initiated=False)
            self._on_connected(peer)

    def _on_connected(self, peer: Peer) -> None:
        role = "we connected to it" if peer.initiated else "it connected to us"
        self.log.peer_added(peer.addr, f"peer #{peer.peer_id} node={peer.node_id or '?'} "
                                       f"({role}); peers now {len(self.peers)}")
        self.on_event(f"[+] peer #{peer.peer_id} connected: {peer.addr} ({role})")
        self._spawn(self._recv_loop, f"peer-{peer.peer_id}", peer)
        self._send_file_list(peer)                     # immediately on connect

    def _recv_loop(self, peer: Peer) -> None:
        """Read framed messages from one peer until its socket closes."""
        reason = "connection closed by peer"
        try:
            while not self._stop.is_set():
                self._handle(peer, proto.recv_message(peer.sock))
        except proto.ConnectionClosed:
            reason = "connection closed by peer (empty read)"
        except ConnectionResetError:
            reason = "connection reset by peer"
        except proto.ProtocolError as exc:
            reason = f"protocol error: {exc}"
        except OSError as exc:
            reason = f"socket error: {exc}"
        finally:
            self._drop_peer(peer, reason)

    def _drop_peer(self, peer: Peer, reason: str) -> None:
        """Remove a peer and its files from the table and close its socket.
        Safe to call more than once and from any thread."""
        if not self.peers.remove(peer):
            return
        try:
            peer.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        peer.sock.close()
        self.log.peer_removed(peer.addr, f"peer #{peer.peer_id} node={peer.node_id} "
                                         f"dropped {len(peer.files)} files; {reason}; "
                                         f"peers now {len(self.peers)}")
        if not self._stop.is_set():
            self.on_event(f"[-] peer #{peer.peer_id} {peer.addr} left ({reason}); "
                          f"removed its files ({len(peer.files)})")
        self._cancel_downloads_from(peer)

    def _resolve_duplicate(self, a: Peer, b: Peer) -> None:
        """Two connections lead to the same node (both started at once and each
        dialed the other). Both ends apply the same rule - keep the connection
        whose TCP client has the smaller node_id - so they close the same one."""
        def client_id(p):
            return self.node_id if p.initiated else p.node_id
        loser = max((a, b), key=lambda p: (client_id(p), p.peer_id))
        self._drop_peer(loser, "duplicate connection to the same node")

    def _send(self, peer: Peer, msg: dict, detail: str = "") -> bool:
        """Send one message to a peer; one sender at a time per socket."""
        try:
            with peer.send_lock:
                proto.send_message(peer.sock, msg)
        except OSError as exc:
            self._drop_peer(peer, f"send failed: {exc}")
            return False
        self.log.sent(peer.addr, msg["type"], detail)
        return True

    # =====================================================================
    # Incoming TCP messages
    # =====================================================================

    def _handle(self, peer: Peer, msg: dict) -> None:
        handlers = {
            proto.FILE_LIST: self._on_file_list,
            proto.DATA_REQUEST: self._on_data_request,
            proto.FILE_DATA: self._on_file_data,
        }
        handler = handlers.get(msg["type"])
        if handler is None:                 # PEER_REQUEST/ACK belong on UDP
            self.log.received(peer.addr, msg["type"], "unexpected on TCP; ignored")
            return
        handler(peer, msg)

    def _on_file_list(self, peer: Peer, msg: dict) -> None:
        files = [f for f in msg.get("files", []) if safe_filename(f)]
        duplicate = self.peers.identify(peer, str(msg.get("node_id")),
                                        int(msg.get("tcp_port", peer.port)))
        self.log.received(peer.addr, proto.FILE_LIST, f"files={files}")
        if duplicate is not None:
            self._resolve_duplicate(peer, duplicate)
            if not self.peers.contains(peer):
                return
        if self.peers.set_files(peer, files):
            self.on_event(f"[i] peer #{peer.peer_id} {peer.addr} now shares: "
                          f"{', '.join(files) or '(nothing)'}")

    def _on_data_request(self, peer: Peer, msg: dict) -> None:
        """Serve a file from shared/, or answer with an ERROR FILE_DATA."""
        requested = msg.get("filename")
        self.log.received(peer.addr, proto.DATA_REQUEST, f"filename={requested}")
        name = safe_filename(requested)
        path = self.shared_dir / name if name else None
        if path is None or not path.is_file():
            reply = proto.file_data_error(str(requested), "file not found")
        else:
            try:
                reply = proto.file_data_ok(name, path.read_bytes())
            except OSError as exc:
                reply = proto.file_data_error(name, f"cannot read file: {exc}")
        detail = (f"filename={reply['filename']} status={reply['status']} "
                  + (f"size={reply['size']}" if "size" in reply else f"error={reply['error']}"))
        self._send(peer, reply, detail)

    def _on_file_data(self, peer: Peer, msg: dict) -> None:
        """Save a downloaded file: downloads/ first, then a copy into shared/."""
        name = safe_filename(msg.get("filename"))
        status = msg.get("status")
        self.log.received(peer.addr, proto.FILE_DATA,
                          f"filename={msg.get('filename')} status={status} "
                          + (f"size={msg.get('size')}" if status == proto.STATUS_OK
                             else f"error={msg.get('error')}"))
        with self._downloads_lock:
            expected = self._downloads.pop(name, None) is not None
        if name is None or not expected:
            self.log.info(f"unrequested FILE_DATA for {msg.get('filename')}; ignored",
                          peer.addr)
            return
        if status != proto.STATUS_OK:
            self.on_event(f"[!] download of '{name}' from peer #{peer.peer_id} failed: "
                          f"{msg.get('error', 'unknown error')}")
            return
        try:
            content = proto.file_content(msg)
            self._store_download(name, content)
        except (proto.ProtocolError, OSError) as exc:
            self.log.info(f"could not save {name}: {exc}", peer.addr)
            self.on_event(f"[!] could not save '{name}': {exc}")
            return
        self.log.info(f"download complete: {name} ({len(content)} bytes) "
                      f"saved to downloads/ and shared/", peer.addr)
        self.on_event(f"[v] downloaded '{name}' ({len(content)} bytes) from peer "
                      f"#{peer.peer_id}; now sharing it")

    def _store_download(self, name: str, content: bytes) -> None:
        """Write to downloads/, then copy into shared/ under a hidden temporary
        name and rename it. Hidden files are never advertised and the rename is
        atomic, so a periodic FILE_LIST can never offer a half-written file."""
        if (self.shared_dir / name).exists():
            return                       # keep the copy we already have
        final = self.downloads_dir / name
        partial = self.downloads_dir / f".{name}.part"
        partial.write_bytes(content)
        os.replace(partial, final)
        staged = self.shared_dir / f".{name}.part"
        shutil.copyfile(final, staged)
        os.replace(staged, self.shared_dir / name)

    def _cancel_downloads_from(self, peer: Peer) -> None:
        """A peer left mid-transfer: forget what we were waiting for from it."""
        with self._downloads_lock:
            lost = [n for n, pid in self._downloads.items() if pid == peer.peer_id]
            for name in lost:
                del self._downloads[name]
        for name in lost:
            self.on_event(f"[!] download of '{name}' aborted: peer #{peer.peer_id} left")

    # =====================================================================
    # File lists
    # =====================================================================

    def local_files(self) -> list:
        """Names of complete, non-hidden files in shared/."""
        return sorted(p.name for p in self.shared_dir.iterdir()
                      if p.is_file() and not p.name.startswith("."))

    def _send_file_list(self, peer: Peer) -> None:
        files = self.local_files()
        self._send(peer, proto.file_list(self.node_id, self.tcp_port, files),
                   f"files={files}")

    def _file_list_loop(self) -> None:
        while not self._stop.wait(FILE_LIST_INTERVAL):
            for peer in self.peers.all():
                self._send_file_list(peer)

    # =====================================================================
    # Public actions used by the CLI
    # =====================================================================

    def request_file(self, filename: str, peer_id: int | None = None) -> str:
        """Send a DATA_REQUEST. Returns a message for the user. The file arrives
        later, asynchronously, in that peer's receive loop."""
        name = safe_filename(filename)
        if name is None:
            return f"invalid file name: {filename!r}"
        if (self.shared_dir / name).exists():
            return f"you already have '{name}' in shared/; not requesting it"

        if peer_id is None:
            holders = self.peers.peers_with_file(name)
            if not holders:
                return f"no connected peer advertises '{name}'"
            peer = holders[0]
        else:
            peer = self.peers.get(peer_id)
            if peer is None:
                return f"no peer #{peer_id}"
            if name not in peer.files:
                return f"peer #{peer_id} does not advertise '{name}'"

        with self._downloads_lock:
            if name in self._downloads:
                return f"'{name}' is already being downloaded"
            self._downloads[name] = peer.peer_id
        if not self._send(peer, proto.data_request(name), f"filename={name}"):
            with self._downloads_lock:
                self._downloads.pop(name, None)
            return f"could not reach peer #{peer.peer_id}"
        return f"requested '{name}' from peer #{peer.peer_id} {peer.addr}"
