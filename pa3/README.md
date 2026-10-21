# PA3 – P2P Data Sharing Agent

A peer-to-peer file sharing agent with no central tracker. Every node is both
a TCP server and a TCP client. Nodes find each other with UDP broadcast on port
54321, keep one TCP connection per peer, advertise the files in their `shared/`
directory every 10 seconds, and serve those files on request.

## Files

| File | Purpose |
|---|---|
| `agent.py` | Entry point and command-line interface |
| `node.py` | `P2PNode`: discovery, connections, threads, file lists, transfers |
| `protocol.py` | Message types, constructors, JSON encoding and TCP length-prefix framing |
| `peer_table.py` | Thread-safe peer table (peer → connection, identity, advertised files) |
| `event_log.py` | Thread-safe writer for `log.txt` |
| `shared/` | Sample files offered by a node started in this directory |
| `demo/run_local_demo.py` | Starts five nodes on one machine and runs the demo scenario |
| `demo/nodeN/` | Each demo node's `shared/`, `downloads/`, `log.txt` and `terminal.txt` |
| `demo/demo_output.txt` | Interleaved terminal output of all five demo nodes |

## How to run

Requires Python 3.10 or newer and no third-party packages.

```bash
cd pa3
python3 agent.py                   # TCP port chosen by the OS; uses ./shared, ./downloads, ./log.txt
python3 agent.py --port 50001      # fixed TCP port
```

Several nodes on **one machine** need different TCP ports and different
directories (each gets its own `shared/`, `downloads/` and `log.txt`):

```bash
python3 agent.py --dir node1 --port 50001
python3 agent.py --dir node2 --port 50002     # in another terminal, and so on
```

On **separate machines**, connect all of them to the same Wi-Fi router or
phone hotspot (campus Wi-Fi blocks broadcast) and allow Python through the
firewall when asked. If discovery finds nobody, pass the subnet's broadcast
address explicitly, for example `--broadcast 192.168.1.255`.

### Commands

| Command | Action |
|---|---|
| `peers` | Show the peer table: local peer number, address, node ID, role, file count |
| `files` | Show the files each peer advertises |
| `mine` | Show the files in your own `shared/` |
| `get <filename>` | Download a file from any peer that advertises it |
| `get <peer#> <filename>` | Download a file from a specific peer |
| `discover` | Broadcast another PEER_REQUEST |
| `help` | List commands |
| `quit` | Close all sockets and exit (Ctrl+C also works) |

Events appear as they happen: `[+]` peer connected, `[-]` peer left,
`[i]` a peer's file list changed, `[v]` download finished, `[!]` error.

### Reproducing the demo

```bash
python3 demo/run_local_demo.py
```

This wipes and recreates `demo/node1` … `demo/node5`, starts five agents on
TCP ports 50001–50005, sends them the commands shown under
[Execution example](#execution-example) and saves the logs and terminal output.

## Message format specification

### Common rules

* Every message is one **JSON object encoded as UTF-8**.
* Every message has a **`"type"` field** naming one of the five types below,
  so the receiver identifies a message by its content, never by its position
  in the conversation. A message with a missing or unknown type is rejected.
* Files are identified **by name only**. A valid file name is a non-empty
  plain name: no `/` or `\`, and it does not start with `.`.

### Message boundaries

**UDP** (PEER_REQUEST, PEER_ACK): one message per datagram. UDP keeps
datagram boundaries, so no extra framing is needed.

**TCP** (FILE_LIST, DATA_REQUEST, FILE_DATA): every message is framed with a
length prefix:

```
+--------------------------------+-----------------------------------+
| length N                       | body                              |
| 4 bytes, unsigned, big-endian  | N bytes of UTF-8 JSON             |
+--------------------------------+-----------------------------------+
```

To receive, a node reads exactly 4 bytes, decodes N, then reads exactly N
bytes. `recv` may return fewer bytes than asked, so `recv_exact()` in
`protocol.py` loops until it has them all. An empty `recv` at any point,
including in the middle of a message, means the peer closed the connection.
A length above 256 MiB is treated as a corrupt stream and the connection is
dropped.

### File content encoding

File bytes are carried **base64-encoded inside the FILE_DATA JSON object**.
We chose this because every message, including file transfers, can then be
parsed the same way (one length prefix, one JSON object, one `type` field),
and the success and error replies share a single message type. The cost is
about 33% extra bytes on the wire and the whole file held in memory, which is
fine for the small files this assignment shares. A `size` field (the decoded
length) lets the receiver check that the content arrived intact.

### The five message types

#### 1. PEER_REQUEST: UDP broadcast to port 54321

Sent at startup (three times, one second apart, in case a datagram is lost)
and whenever the user types `discover`.

| Field | Type | Meaning |
|---|---|---|
| `type` | string | `"PEER_REQUEST"` |
| `node_id` | string | Random 8-hex-digit ID created at startup. A node ignores datagrams carrying its own ID, which is how it ignores its own broadcasts (comparing IP addresses would fail when several nodes share one machine). |
| `tcp_port` | int | The sender's TCP listening port (informational) |

```json
{"type":"PEER_REQUEST","node_id":"72033518","tcp_port":50001}
```

The request is sent from a fresh UDP socket with an OS-assigned port, and the
sender keeps listening on that socket for PEER_ACKs. Every node keeps a UDP
listener bound to port 54321 for its whole lifetime, so a node that is
already running answers nodes that join later.

#### 2. PEER_ACK: UDP unicast reply

Sent by every node that receives a PEER_REQUEST, back to the source address
and port of that datagram.

| Field | Type | Meaning |
|---|---|---|
| `type` | string | `"PEER_ACK"` |
| `node_id` | string | The responder's node ID |
| `ip` | string | The responder's IP address on the interface facing the requester |
| `tcp_port` | int | The responder's TCP listening port |

```json
{"type":"PEER_ACK","node_id":"c2b57054","ip":"192.168.1.12","tcp_port":50002}
```

**Connection rule:** the node that sent the PEER_REQUEST connects to
`ip:tcp_port` and acts as the TCP client. The node that sent the ACK is the
TCP server. A node does not dial a `node_id` it is already connected to or
already dialing.

If two nodes start at the same moment, each can receive the other's request
and both may dial. When a FILE_LIST shows two connections to the same
`node_id`, both ends keep the connection **whose TCP client has the smaller
`node_id`** and close the other. Because both ends apply the same rule, they
close the same connection.

#### 3. FILE_LIST: TCP

Sent immediately after a connection is established (by both ends) and then
to every peer every 10 seconds.

| Field | Type | Meaning |
|---|---|---|
| `type` | string | `"FILE_LIST"` |
| `node_id` | string | Sender's node ID. This is how the TCP server learns who connected to it. |
| `tcp_port` | int | Sender's TCP listening port, used as the peer's display address |
| `files` | array of strings | Names of complete files in the sender's `shared/`. Names only. |

```json
{"type":"FILE_LIST","node_id":"72033518","tcp_port":50001,"files":["alpha.txt","gradient.png"]}
```

The receiver replaces that peer's entry in its file table with this list.

#### 4. DATA_REQUEST: TCP

| Field | Type | Meaning |
|---|---|---|
| `type` | string | `"DATA_REQUEST"` |
| `filename` | string | One name from the receiving peer's advertised list |

```json
{"type":"DATA_REQUEST","filename":"echo.txt"}
```

A node never requests a file it already has in `shared/`, and never has two
requests for the same file in flight.

#### 5. FILE_DATA: TCP

The reply to every DATA_REQUEST, on the same connection. The connection
stays open whether the reply succeeds or fails.

| Field | Type | Present when | Meaning |
|---|---|---|---|
| `type` | string | always | `"FILE_DATA"` |
| `filename` | string | always | The requested name |
| `status` | string | always | `"OK"` or `"ERROR"` |
| `size` | int | `OK` | File length in bytes (before base64) |
| `data` | string | `OK` | File content, base64 encoded |
| `error` | string | `ERROR` | Human-readable reason, e.g. `"file not found"` |

```json
{"type":"FILE_DATA","filename":"echo.txt","status":"OK","size":57,"data":"RWNobyBmcm9tIG5vZGUgNSAtIHRoaXMgZmlsZSB0cmF2ZWxzIGFjcm9zcyB0aGUgbmV0d29yay4K"}
{"type":"FILE_DATA","filename":"old_draft.txt","status":"ERROR","error":"file not found"}
```

On `OK`, the receiver writes the file to `downloads/` (through a hidden
`.name.part` temporary file and an atomic rename), then copies it into
`shared/` the same way. Hidden files are never advertised, so a FILE_LIST can
never offer a half-written file. If a file with that name already exists in
`shared/`, the existing copy is kept.

### Termination

There is no goodbye message. A node leaves by closing its sockets. The other
end detects this in that peer's receive loop: an empty `recv` (including one
in the middle of a message), `ConnectionResetError`, or any other socket error
counts as a disconnection. The node then removes the peer from the peer
table, drops its file list, cancels any download pending from it, closes the
socket and logs `PEER_REMOVED`. A node that comes back is discovered again
through UDP broadcast.

## Design

### Threads

| Thread | Job |
|---|---|
| `udp-listener` | Bound to UDP 54321 for the node's lifetime; answers each PEER_REQUEST with a PEER_ACK |
| `tcp-accept` | Accepts connections from nodes that received our PEER_ACK |
| `peer-N` (one per peer) | Reads framed messages and handles FILE_LIST, DATA_REQUEST, FILE_DATA |
| `file-list` | Sends our FILE_LIST to every peer every 10 seconds |
| `discovery` | Broadcasts PEER_REQUEST and collects PEER_ACKs at startup or on `discover` |
| `dial` (short-lived) | Opens the TCP connection to one peer that answered |
| main thread | The command line |

### Shared state

* The `PeerTable` holds every peer and its file list behind one lock.
  Readers only get snapshots, so no thread iterates a dictionary another
  thread is changing.
* Each peer has its own send lock, because the file-list thread, that peer's
  receive thread (replying with FILE_DATA) and the CLI (sending DATA_REQUEST)
  can all write to the same socket. Without the lock, two messages could be
  interleaved byte by byte.
* `_drop_peer()` can be called more than once from different threads; only
  the first call removes and logs the peer.

### Log format

Each node appends to `log.txt` in its directory:

```
TIMESTAMP                DIRECTION    PEER ADDRESS           TYPE          DETAIL
2026-10-20 23:04:46.088  RECEIVED     192.0.2.2:48967        PEER_REQUEST  node=c2b57054 tcp_port=50002
2026-10-20 23:04:46.088  SENT         192.0.2.2:48967        PEER_ACK      ip=192.0.2.2 tcp_port=50001
2026-10-20 23:04:46.091  PEER_ADDED   192.0.2.2:56852        -             peer #1 node=? (it connected to us); peers now 1
2026-10-20 23:04:46.092  RECEIVED     192.0.2.2:50002        FILE_LIST     files=['bravo.txt']
2026-10-20 23:04:57.526  RECEIVED     192.0.2.2:50003        FILE_DATA     filename=old_draft.txt status=ERROR error=file not found
2026-10-20 23:05:11.027  PEER_REMOVED 192.0.2.2:50005        -             peer #4 node=2774225d dropped 1 files; connection closed by peer (empty read); peers now 3
```

Every message sent or received has a `SENT` or `RECEIVED` line. Peer table
changes have `PEER_ADDED` and `PEER_REMOVED` lines. `INFO` lines record
startup, shutdown and completed downloads. On the server side, `PEER_ADDED`
shows the client's temporary port because the peer's listening port and node
ID are not known until its first FILE_LIST arrives a moment later.

## Division of work

> **TODO:** Fill this in with what each member actually did. It must match
> the commit history on GitHub.

| Member | Responsibilities |
|---|---|
| _Name 1_ | |
| _Name 2_ | |
| _Name 3_ | |

## Execution example

Five nodes on one machine (`python3 demo/run_local_demo.py`), started one
second apart on TCP ports 50001–50005. Below is the interleaved terminal
output; each line is prefixed with the node that printed it. The full output
is in `demo/demo_output.txt`, and each node's log is in `demo/nodeN/log.txt`.

**1. Discovery.** Each new node broadcasts PEER_REQUEST, every running node
answers with PEER_ACK, and the new node connects to each of them. Excerpt for
node5 joining the four running nodes:

```
23:04:49 [node5] node 2774225d listening on TCP port 50005 (discovery on UDP 54321)
23:04:49 [node5] sharing: echo.txt
23:04:49 [node5] [+] peer #1 connected: 192.0.2.2:50003 (we connected to it)
23:04:49 [node5] [+] peer #2 connected: 192.0.2.2:50001 (we connected to it)
23:04:49 [node4] [+] peer #4 connected: 192.0.2.2:49808 (it connected to us)
23:04:49 [node3] [+] peer #4 connected: 192.0.2.2:42700 (it connected to us)
23:04:49 [node5] [+] peer #3 connected: 192.0.2.2:50004 (we connected to it)
23:04:49 [node1] [+] peer #4 connected: 192.0.2.2:49012 (it connected to us)
23:04:49 [node5] [i] peer #3 192.0.2.2:50004 now shares: delta.csv
23:04:49 [node4] [i] peer #4 192.0.2.2:50005 now shares: echo.txt
23:04:49 [node5] [+] peer #4 connected: 192.0.2.2:50002 (we connected to it)
23:04:49 [node2] [+] peer #4 connected: 192.0.2.2:45658 (it connected to us)
23:04:49 [node5] [i] peer #1 192.0.2.2:50003 now shares: charlie.txt, old_draft.txt
23:04:49 [node5] [i] peer #2 192.0.2.2:50001 now shares: alpha.txt, gradient.png
23:04:49 [node2] [i] peer #4 192.0.2.2:50005 now shares: echo.txt
23:04:49 [node5] [i] peer #4 192.0.2.2:50002 now shares: bravo.txt
23:04:49 [node3] [i] peer #4 192.0.2.2:50005 now shares: echo.txt
23:04:49 [node1] [i] peer #4 192.0.2.2:50005 now shares: echo.txt
```

**2. Peer table and file lists on node1:**

```
23:04:52 [node1] > peers
23:04:52 [node1] #   ADDRESS               NODE ID   ROLE    FILES
23:04:52 [node1] 1   192.0.2.2:50002       c2b57054  server  1
23:04:52 [node1] 2   192.0.2.2:50003       a5cb2ea8  server  2
23:04:52 [node1] 3   192.0.2.2:50004       379b5fb5  server  1
23:04:52 [node1] 4   192.0.2.2:50005       2774225d  server  1
23:04:52 [node1] > files
23:04:52 [node1] #   ADDRESS               FILES
23:04:52 [node1] 1   192.0.2.2:50002       bravo.txt
23:04:52 [node1] 2   192.0.2.2:50003       charlie.txt, old_draft.txt
23:04:52 [node1] 3   192.0.2.2:50004       delta.csv
23:04:52 [node1] 4   192.0.2.2:50005       echo.txt
```

**3. File transfer.** node2 downloads `echo.txt` from node5 and node4
downloads the binary `gradient.png` from node1. On the next FILE_LIST round,
every node sees that node2 now shares `echo.txt`. Asking for a file you
already have is refused locally.

```
23:04:53 [node2] > get echo.txt
23:04:53 [node2] requested 'echo.txt' from peer #4 192.0.2.2:50005
23:04:53 [node2] [v] downloaded 'echo.txt' (57 bytes) from peer #4; now sharing it
23:04:54 [node4] > get 1 gradient.png
23:04:54 [node4] requested 'gradient.png' from peer #1 192.0.2.2:50001
23:04:54 [node4] [v] downloaded 'gradient.png' (463 bytes) from peer #1; now sharing it
23:04:56 [node2] > mine
23:04:56 [node2] bravo.txt, echo.txt
23:04:56 [node3] [i] peer #2 192.0.2.2:50002 now shares: bravo.txt, echo.txt
23:04:56 [node4] [i] peer #2 192.0.2.2:50002 now shares: bravo.txt, echo.txt
23:04:56 [node1] [i] peer #1 192.0.2.2:50002 now shares: bravo.txt, echo.txt
23:04:56 [node5] [i] peer #4 192.0.2.2:50002 now shares: bravo.txt, echo.txt
23:04:56 [node2] > get echo.txt
23:04:56 [node2] you already have 'echo.txt' in shared/; not requesting it
```

**4. Error reply.** node3 deletes `old_draft.txt` after advertising it. node1
still requests it and gets an `ERROR` FILE_DATA. The connection stays open,
and node3's next FILE_LIST no longer contains the file.

```
23:04:57 [node1] > get old_draft.txt
23:04:57 [node1] requested 'old_draft.txt' from peer #2 192.0.2.2:50003
23:04:57 [node1] [!] download of 'old_draft.txt' from peer #2 failed: file not found
23:05:07 [node1] [i] peer #2 192.0.2.2:50003 now shares: charlie.txt
23:05:10 [node3] > files
23:05:10 [node3] #   ADDRESS               FILES
23:05:10 [node3] 1   192.0.2.2:50001       alpha.txt, gradient.png
23:05:10 [node3] 2   192.0.2.2:50002       bravo.txt, echo.txt
23:05:10 [node3] 3   192.0.2.2:50004       delta.csv, gradient.png
23:05:10 [node3] 4   192.0.2.2:50005       echo.txt
```

**5. A node leaves.** node5 quits. Every other node detects the closed socket
and removes node5 and its files from its tables.

```
23:05:11 [node5] > quit
23:05:11 [node1] [-] peer #4 192.0.2.2:50005 left (connection closed by peer (empty read)); removed its files (1)
23:05:11 [node5] left the network
23:05:11 [node3] [-] peer #4 192.0.2.2:50005 left (connection closed by peer (empty read)); removed its files (1)
23:05:11 [node4] [-] peer #4 192.0.2.2:50005 left (connection closed by peer (empty read)); removed its files (1)
23:05:11 [node2] [-] peer #4 192.0.2.2:50005 left (connection closed by peer (empty read)); removed its files (1)
23:05:12 [node1] > files
23:05:12 [node1] #   ADDRESS               FILES
23:05:12 [node1] 1   192.0.2.2:50002       bravo.txt, echo.txt
23:05:12 [node1] 2   192.0.2.2:50003       charlie.txt
23:05:12 [node1] 3   192.0.2.2:50004       delta.csv, gradient.png
23:05:12 [node3] > peers
23:05:12 [node3] #   ADDRESS               NODE ID   ROLE    FILES
23:05:12 [node3] 1   192.0.2.2:50001       72033518  client  2
23:05:12 [node3] 2   192.0.2.2:50002       c2b57054  client  2
23:05:12 [node3] 3   192.0.2.2:50004       379b5fb5  server  2
```

ROLE is this node's own role on that connection: node3 joined after node1
and node2, so it dialed them (client); node4 joined later and dialed node3
(server).

node1's `log.txt` records the same departure:

```
2026-10-20 23:05:11.027  PEER_REMOVED 192.0.2.2:50005        -             peer #4 node=2774225d dropped 1 files; connection closed by peer (empty read); peers now 3
```
