"""
protocol.py - Wire protocol shared by every node in the group.

All messages are JSON objects with a mandatory "type" field, so every
message is self-identifying regardless of the order it arrives in.

Transport rules
---------------
* UDP (port 54321): PEER_REQUEST and PEER_ACK. One message per datagram,
  so UDP needs no extra framing - the datagram boundary is the message
  boundary.
* TCP: FILE_LIST, DATA_REQUEST and FILE_DATA. TCP is a byte stream, so each
  message is framed as

      +----------------------+-------------------------------+
      | 4-byte length N      | N bytes of UTF-8 encoded JSON |
      | (unsigned, big-end.) |                               |
      +----------------------+-------------------------------+

  The receiver reads exactly 4 bytes, decodes N, then reads exactly N bytes.
  File content travels inside FILE_DATA as a base64 string.

See README.md for the full field-by-field specification.
"""

import base64
import binascii
import json
import socket
import struct

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

UDP_PORT = 54321                      # discovery port, fixed for the group
HEADER = struct.Struct("!I")          # 4-byte unsigned big-endian length
MAX_MESSAGE_SIZE = 256 * 1024 * 1024  # refuse absurd lengths (corrupt stream)
RECV_CHUNK = 64 * 1024                # largest single recv() call

# The five message types
PEER_REQUEST = "PEER_REQUEST"
PEER_ACK = "PEER_ACK"
FILE_LIST = "FILE_LIST"
DATA_REQUEST = "DATA_REQUEST"
FILE_DATA = "FILE_DATA"

MESSAGE_TYPES = {PEER_REQUEST, PEER_ACK, FILE_LIST, DATA_REQUEST, FILE_DATA}

# Values of the FILE_DATA "status" field
STATUS_OK = "OK"
STATUS_ERROR = "ERROR"


class ProtocolError(Exception):
    """A message violated the protocol (bad JSON, unknown type, bad length)."""


class ConnectionClosed(Exception):
    """The peer closed the TCP connection (recv returned b'')."""


# ---------------------------------------------------------------------------
# Encoding / decoding of a single JSON message body
# ---------------------------------------------------------------------------

def encode(msg: dict) -> bytes:
    """Serialize a message dict to compact UTF-8 JSON bytes."""
    return json.dumps(msg, separators=(",", ":")).encode("utf-8")


def decode(raw: bytes) -> dict:
    """Parse JSON bytes into a message dict and check its "type" field."""
    try:
        msg = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ProtocolError(f"malformed message: {exc}") from exc
    if not isinstance(msg, dict) or msg.get("type") not in MESSAGE_TYPES:
        raise ProtocolError(f"unknown message type: {str(msg)[:60]}")
    return msg


# ---------------------------------------------------------------------------
# Message constructors - one per message type
# ---------------------------------------------------------------------------

def peer_request(node_id: str, tcp_port: int) -> dict:
    """UDP broadcast sent by a node looking for peers."""
    return {"type": PEER_REQUEST, "node_id": node_id, "tcp_port": tcp_port}


def peer_ack(node_id: str, ip: str, tcp_port: int) -> dict:
    """UDP reply telling the requester where to open a TCP connection."""
    return {"type": PEER_ACK, "node_id": node_id, "ip": ip, "tcp_port": tcp_port}


def file_list(node_id: str, tcp_port: int, files: list) -> dict:
    """Names of the files in the sender's shared/ directory."""
    return {"type": FILE_LIST, "node_id": node_id, "tcp_port": tcp_port,
            "files": list(files)}


def data_request(filename: str) -> dict:
    """Ask a peer for one file from its advertised list."""
    return {"type": DATA_REQUEST, "filename": filename}


def file_data_ok(filename: str, content: bytes) -> dict:
    """Successful reply to DATA_REQUEST, carrying the file as base64."""
    return {"type": FILE_DATA, "filename": filename, "status": STATUS_OK,
            "size": len(content),
            "data": base64.b64encode(content).decode("ascii")}


def file_data_error(filename: str, reason: str) -> dict:
    """Failed reply to DATA_REQUEST (e.g. the file no longer exists)."""
    return {"type": FILE_DATA, "filename": filename, "status": STATUS_ERROR,
            "error": reason}


def file_content(msg: dict) -> bytes:
    """Extract and verify the raw bytes from an OK FILE_DATA message."""
    try:
        content = base64.b64decode(msg["data"], validate=True)
    except (KeyError, TypeError, binascii.Error) as exc:
        raise ProtocolError(f"bad file data: {exc}") from exc
    if len(content) != msg.get("size", len(content)):
        raise ProtocolError("file size does not match 'size' field")
    return content


# ---------------------------------------------------------------------------
# TCP framing
# ---------------------------------------------------------------------------

def send_message(sock: socket.socket, msg: dict) -> None:
    """Send one length-prefixed message. Caller must hold the socket's send lock."""
    body = encode(msg)
    sock.sendall(HEADER.pack(len(body)))
    sock.sendall(body)


def recv_exact(sock: socket.socket, n: int) -> bytes:
    """Read exactly n bytes.

    A short recv() is normal on a stream, so keep looping. An empty recv()
    means the peer closed its end - possibly in the middle of a message -
    which we report as ConnectionClosed. ConnectionResetError (an abrupt
    reset) propagates to the caller, which treats it the same way.
    """
    chunks = []
    remaining = n
    while remaining > 0:
        chunk = sock.recv(min(remaining, RECV_CHUNK))
        if not chunk:
            raise ConnectionClosed()
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def recv_message(sock: socket.socket) -> dict:
    """Block until one complete framed message has arrived and return it."""
    (length,) = HEADER.unpack(recv_exact(sock, HEADER.size))
    if length > MAX_MESSAGE_SIZE:
        raise ProtocolError(f"message length {length} exceeds limit")
    return decode(recv_exact(sock, length))
