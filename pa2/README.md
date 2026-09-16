\# PA2 - Leader Election (Chang-Roberts Algorithm)



\## Description

This program implements the Chang-Roberts leader election algorithm on an

asynchronous, non-anonymous ring of processes. Each process generates a

random UUID on startup and communicates with its two ring neighbors over

TCP sockets. Messages circulate around the ring until the process with the

largest UUID is identified as the leader; every process then agrees on and

prints that leader's ID.



\## Requirements

\- Python 3.x (standard library only — no external packages needed)



\## Files

\- `myleprocess.py` — main program (client/server node implementation)

\- `config1.txt`, `config2.txt`, `config3.txt` — example configuration files

&#x20; for a 3-node local ring (using localhost)

\- `log1.txt`, `log2.txt`, `log3.txt` — log output from a successful local run



\## Config File Format

Each config file contains two lines:

```

<my\_ip>,<my\_port>

<peer\_ip>,<peer\_port>

```

\- Line 1: the IP and port this process listens on (as a server)

\- Line 2: the IP and port of the neighbor this process connects to (as a client)



Example (`config1.txt`):

```

127.0.0.1,5001

127.0.0.1,5002

```



\## How to Run

Each process is started with its config file as a command-line argument.

For a 3-node local ring, open three terminals in this directory and run:



```

python myleprocess.py config1.txt

python myleprocess.py config2.txt

python myleprocess.py config3.txt

```



Start all three within a few seconds of each other. Each process prints

`leader is <uuid>` once the election finishes, and writes its own log file

(e.g., running with `config1.txt` produces `log1.txt`).



\## Example Run



\*\*Terminal 1 — `python myleprocess.py config1.txt`\*\*

```

My ID: 8d349eb8-5e23-441b-95d9-06c595ebcde3

Both connections established.

Sent: uuid=8d349eb8-5e23-441b-95d9-06c595ebcde3, flag=0

Received: uuid=cb099c24-ea25-46b0-a41e-c69b51f36e36, flag=0, greater, 0

Sent: uuid=cb099c24-ea25-46b0-a41e-c69b51f36e36, flag=0

Received: uuid=e0c97948-4a51-4d1c-925e-59b173e6d9b7, flag=0, greater, 0

Sent: uuid=e0c97948-4a51-4d1c-925e-59b173e6d9b7, flag=0

Received: uuid=e0c97948-4a51-4d1c-925e-59b173e6d9b7, flag=1, greater, 0

Leader is decided to e0c97948-4a51-4d1c-925e-59b173e6d9b7.

Sent: uuid=e0c97948-4a51-4d1c-925e-59b173e6d9b7, flag=1

```



\*\*Terminal 2 — `python myleprocess.py config2.txt`\*\*

```

My ID: e0c97948-4a51-4d1c-925e-59b173e6d9b7

Both connections established.

Sent: uuid=e0c97948-4a51-4d1c-925e-59b173e6d9b7, flag=0

Received: uuid=8d349eb8-5e23-441b-95d9-06c595ebcde3, flag=0, less, 0

Ignored: uuid=8d349eb8-5e23-441b-95d9-06c595ebcde3, flag=0

Received: uuid=cb099c24-ea25-46b0-a41e-c69b51f36e36, flag=0, less, 0

Ignored: uuid=cb099c24-ea25-46b0-a41e-c69b51f36e36, flag=0

Received: uuid=e0c97948-4a51-4d1c-925e-59b173e6d9b7, flag=0, same, 0

Sent: uuid=e0c97948-4a51-4d1c-925e-59b173e6d9b7, flag=1

Leader is decided to e0c97948-4a51-4d1c-925e-59b173e6d9b7.

Received: uuid=e0c97948-4a51-4d1c-925e-59b173e6d9b7, flag=1, same, 1, leader=e0c97948-4a51-4d1c-925e-59b173e6d9b7

Leader is decided to e0c97948-4a51-4d1c-925e-59b173e6d9b7.

```



\*\*Terminal 3 — `python myleprocess.py config3.txt`\*\*

```

My ID: cb099c24-ea25-46b0-a41e-c69b51f36e36

Both connections established.

Sent: uuid=cb099c24-ea25-46b0-a41e-c69b51f36e36, flag=0

Received: uuid=e0c97948-4a51-4d1c-925e-59b173e6d9b7, flag=0, greater, 0

Sent: uuid=e0c97948-4a51-4d1c-925e-59b173e6d9b7, flag=0

Received: uuid=e0c97948-4a51-4d1c-925e-59b173e6d9b7, flag=1, greater, 0

Leader is decided to e0c97948-4a51-4d1c-925e-59b173e6d9b7.

Sent: uuid=e0c97948-4a51-4d1c-925e-59b173e6d9b7, flag=1

```



\## Result

All three processes independently agreed on the same leader:

`e0c97948-4a51-4d1c-925e-59b173e6d9b7` — the process with the largest UUID

among the three, satisfying the Termination, Uniqueness, and Agreement

conditions of the leader election problem.

