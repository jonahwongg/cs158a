#!/usr/bin/env python3
"""
agent.py - Command-line front end for the P2P file sharing node.

    python3 agent.py                      # TCP port chosen by the OS, files in ./shared
    python3 agent.py --port 50001         # fixed TCP port
    python3 agent.py --dir node2 --port 50002   # second node on the same machine

Type `help` at the prompt for the list of commands.
"""

import argparse
import sys
import threading

from node import P2PNode

HELP = """\
Commands:
  peers                  show connected peers
  files                  show the files each peer advertises
  mine                   show the files in your shared/ directory
  get <filename>         download a file from any peer that has it
  get <peer#> <filename> download a file from a specific peer
  discover               broadcast another PEER_REQUEST
  help                   show this message
  quit                   leave the network and exit"""


# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------

_print_lock = threading.Lock()


def say(text: str) -> None:
    """Print from any thread without interleaving lines."""
    with _print_lock:
        print(text, flush=True)


def show_peers(node: P2PNode) -> None:
    peers = node.peers.all()
    if not peers:
        say("(no connected peers)")
        return
    lines = [f"{'#':<4}{'ADDRESS':<22}{'NODE ID':<10}{'ROLE':<8}FILES"]
    for p in peers:
        role = "client" if p.initiated else "server"
        lines.append(f"{p.peer_id:<4}{p.addr:<22}{p.node_id or '?':<10}{role:<8}{len(p.files)}")
    say("\n".join(lines))


def show_files(node: P2PNode) -> None:
    peers = node.peers.all()
    if not peers:
        say("(no connected peers)")
        return
    lines = [f"{'#':<4}{'ADDRESS':<22}FILES"]
    for p in peers:
        lines.append(f"{p.peer_id:<4}{p.addr:<22}{', '.join(p.files) or '(none)'}")
    say("\n".join(lines))


# ---------------------------------------------------------------------------
# Command loop
# ---------------------------------------------------------------------------

def run_command(node: P2PNode, line: str) -> bool:
    """Execute one command. Returns False when the user wants to quit."""
    words = line.split()
    if not words:
        return True
    cmd, args = words[0].lower(), words[1:]

    if cmd in ("quit", "exit"):
        return False
    elif cmd == "help":
        say(HELP)
    elif cmd == "peers":
        show_peers(node)
    elif cmd == "files":
        show_files(node)
    elif cmd in ("mine", "ls"):
        say(", ".join(node.local_files()) or "(shared/ is empty)")
    elif cmd == "discover":
        node.discover()
        say("broadcasting PEER_REQUEST")
    elif cmd == "get" and len(args) == 1:
        say(node.request_file(args[0]))
    elif cmd == "get" and len(args) == 2 and args[0].isdigit():
        say(node.request_file(args[1], int(args[0])))
    else:
        say("unknown command; type 'help'")
    return True


def command_loop(node: P2PNode) -> None:
    interactive = sys.stdin.isatty()
    while True:
        try:
            line = input("> " if interactive else "")
        except EOFError:
            # No more input (e.g. started with < /dev/null): keep serving
            # peers until Ctrl+C.
            say("stdin closed; node keeps running. Press Ctrl+C to stop.")
            threading.Event().wait()
        if not interactive:
            say(f"> {line}")          # echo scripted commands into the transcript
        if not run_command(node, line):
            return


def parse_args():
    parser = argparse.ArgumentParser(description="P2P file sharing agent")
    parser.add_argument("--port", type=int, default=0,
                        help="TCP port to listen on (default: chosen by the OS)")
    parser.add_argument("--dir", default=".",
                        help="directory holding shared/, downloads/ and log.txt "
                             "(default: current directory)")
    parser.add_argument("--broadcast", default="255.255.255.255",
                        help="broadcast address for PEER_REQUEST "
                             "(e.g. 192.168.1.255 if the default does not work)")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    node = P2PNode(args.dir, args.port, args.broadcast, on_event=say)
    try:
        node.start()
    except OSError as exc:
        say(f"cannot start node: {exc}")
        sys.exit(1)
    say(f"node {node.node_id} listening on TCP port {node.tcp_port} "
        f"(discovery on UDP 54321)")
    say(f"sharing: {', '.join(node.local_files()) or '(nothing)'}")
    say("type 'help' for commands")
    try:
        command_loop(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.stop()
        say("left the network")


if __name__ == "__main__":
    main()
