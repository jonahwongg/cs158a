import sys
import socket
import threading
import json
import uuid
import time

# ---------- Message class ----------
class Message:
    def __init__(self, msg_uuid, flag):
        self.uuid = msg_uuid
        self.flag = flag

    def to_json(self):
        return json.dumps({"uuid": str(self.uuid), "flag": self.flag})

    @staticmethod
    def from_json(data):
        obj = json.loads(data)
        return Message(uuid.UUID(obj["uuid"]), obj["flag"])


# ---------- Logging ----------
log_file = None

def log(line):
    print(line)
    with open(log_file, "a") as f:
        f.write(line + "\n")


# ---------- Networking helpers ----------
def send_msg(sock, msg):
    sock.sendall((msg.to_json() + "\n").encode())
    log(f"Sent: uuid={msg.uuid}, flag={msg.flag}")

def recv_msg(sockfile):
    line = sockfile.readline()
    if not line:
        raise ConnectionError("Connection closed by peer")
    return Message.from_json(line.strip())


# ---------- Main ----------
def main():
    global log_file

    if len(sys.argv) != 2:
        print("Usage: python myleprocess.py <config_file>")
        sys.exit(1)

    config_file = sys.argv[1]
    log_file = config_file.replace("config", "log")  # e.g. config1.txt -> log1.txt

    with open(config_file) as f:
        lines = f.read().splitlines()
        my_ip, my_port = lines[0].split(",")
        peer_ip, peer_port = lines[1].split(",")
        my_port = int(my_port)
        peer_port = int(peer_port)

    my_id = uuid.uuid4()
    log(f"My ID: {my_id}")

    server_sock_holder = {}
    client_sock_holder = {}

    def run_server():
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((my_ip, my_port))
        s.listen(1)
        conn, addr = s.accept()
        server_sock_holder["sock"] = conn

    def run_client():
        while True:
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect((peer_ip, peer_port))
                client_sock_holder["sock"] = s
                break
            except ConnectionRefusedError:
                time.sleep(1)

    t1 = threading.Thread(target=run_server)
    t2 = threading.Thread(target=run_client)
    t1.start()
    t2.start()

    # Optional sync point for in-class demo -- uncomment if needed:
    # input("press Enter when everyone is ready.")

    t1.join()
    t2.join()

    server_sock = server_sock_holder["sock"]
    client_sock = client_sock_holder["sock"]
    server_sockfile = server_sock.makefile("r")

    log("Both connections established.")

    # Send initial message (no comparison)
    send_msg(client_sock, Message(my_id, 0))

    leader_id = None
    state = 0  # 0 = still electing, 1 = leader known
    sent_own_announcement = False

    while True:
        msg = recv_msg(server_sockfile)

        if msg.uuid > my_id:
            cmp = "greater"
        elif msg.uuid < my_id:
            cmp = "less"
        else:
            cmp = "same"

        line = f"Received: uuid={msg.uuid}, flag={msg.flag}, {cmp}, {state}"
        if state == 1:
            line += f", leader={leader_id}"
        log(line)

        if msg.flag == 1:
            leader_id = msg.uuid
            log(f"Leader is decided to {leader_id}.")
            print(f"leader is {leader_id}")
            if not sent_own_announcement:
                # forward the announcement around the ring
                send_msg(client_sock, msg)
            break  # terminate after forwarding (or after hearing your own come back)

        else:  # flag == 0
            if cmp == "greater":
                send_msg(client_sock, msg)
            elif cmp == "less":
                log(f"Ignored: uuid={msg.uuid}, flag={msg.flag}")
            else:  # same -> I am the leader
                leader_id = my_id
                state = 1
                announce = Message(my_id, 1)
                sent_own_announcement = True
                send_msg(client_sock, announce)
                log(f"Leader is decided to {leader_id}.")


if __name__ == "__main__":
    main()