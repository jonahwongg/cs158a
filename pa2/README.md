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

My ID: 2e4554ce-0258-494b-8b3e-d484688a6f7c

Both connections established.

Sent: uuid=2e4554ce-0258-494b-8b3e-d484688a6f7c, flag=0

Received: uuid=1178d9ee-173c-472e-aa7c-ca4419c235a7, flag=0, less, 0

Ignored: uuid=1178d9ee-173c-472e-aa7c-ca4419c235a7, flag=0

Received: uuid=25f75839-4ddb-445f-81e8-4246dda55f78, flag=0, less, 0

Ignored: uuid=25f75839-4ddb-445f-81e8-4246dda55f78, flag=0

Received: uuid=2e4554ce-0258-494b-8b3e-d484688a6f7c, flag=0, same, 0

Sent: uuid=2e4554ce-0258-494b-8b3e-d484688a6f7c, flag=1

Leader is decided to 2e4554ce-0258-494b-8b3e-d484688a6f7c.

leader is 2e4554ce-0258-494b-8b3e-d484688a6f7c

```



\*\*Terminal 2 — `python myleprocess.py config2.txt`\*\*

```

My ID: 2e4554ce-0258-494b-8b3e-d484688a6f7c

Both connections established.

Sent: uuid=2e4554ce-0258-494b-8b3e-d484688a6f7c, flag=0

Received: uuid=1178d9ee-173c-472e-aa7c-ca4419c235a7, flag=0, less, 0

Ignored: uuid=1178d9ee-173c-472e-aa7c-ca4419c235a7, flag=0

Received: uuid=25f75839-4ddb-445f-81e8-4246dda55f78, flag=0, less, 0

Ignored: uuid=25f75839-4ddb-445f-81e8-4246dda55f78, flag=0

Received: uuid=2e4554ce-0258-494b-8b3e-d484688a6f7c, flag=0, same, 0

Sent: uuid=2e4554ce-0258-494b-8b3e-d484688a6f7c, flag=1

Leader is decided to 2e4554ce-0258-494b-8b3e-d484688a6f7c.

Received: uuid=2e4554ce-0258-494b-8b3e-d484688a6f7c, flag=1, same, 1, leader=2e4554ce-0258-494b-8b3e-d484688a6f7c

Leader is decided to 2e4554ce-0258-494b-8b3e-d484688a6f7c.

leader is 2e4554ce-0258-494b-8b3e-d484688a6f7c

```



\*\*Terminal 3 — `python myleprocess.py config3.txt`\*\*

```

My ID: 25f75839-4ddb-445f-81e8-4246dda55f78

Both connections established.

Sent: uuid=25f75839-4ddb-445f-81e8-4246dda55f78, flag=0

Received: uuid=2e4554ce-0258-494b-8b3e-d484688a6f7c, flag=0, greater, 0

Sent: uuid=2e4554ce-0258-494b-8b3e-d484688a6f7c, flag=0

Received: uuid=2e4554ce-0258-494b-8b3e-d484688a6f7c, flag=1, greater, 0

Leader is decided to 2e4554ce-0258-494b-8b3e-d484688a6f7c.

leader is 2e4554ce-0258-494b-8b3e-d484688a6f7c

Sent: uuid=2e4554ce-0258-494b-8b3e-d484688a6f7c, flag=1```



\## Result

All three processes independently agreed on the same leader:

`2e4554ce-0258-494b-8b3e-d484688a6f7c` — the process with the largest UUID

among the three, satisfying the Termination, Uniqueness, and Agreement

conditions of the leader election problem.

