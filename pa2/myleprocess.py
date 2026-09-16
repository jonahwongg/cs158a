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
recv_buffers = {}  

def send_msg(sock, msg):
    sock.sendall(msg.to_json().encode())  
    log(f"Sent: uuid={msg.uuid}, flag={msg.flag}")

def recv_msg(sock):
    buf = recv_buffers.get(sock, "")
    while "}" not in buf:
        data = sock.recv(1024).decode()
        if not data:
            raise ConnectionError("Connection closed by peer")
        buf += data
    idx = buf.index("}")
    msg_str = buf[:idx + 1]              
    recv_buffers[sock] = buf[idx + 1:]   
    return Message.from_json(msg_str)


# ---------- Main ----------
def main():
    global log_file

    if len(sys.argv) != 2:
        print("Usage: python myleprocess.py <config_file>")
        sys.exit(1)

    config_file = sys.argv[1]
    log_file = config_file.replace("config", "log")  	

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

    t1.join()
    t2.join()

    server_sock = server_sock_holder["sock"]
    client_sock = client_sock_holder["sock"]

    log("Both connections established.")

    send_msg(client_sock, Message(my_id, 0))

    leader_id = None
    state = 0  
    sent_own_announcement = False

    while True:
        msg = recv_msg(server_sock)

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
               
                send_msg(client_sock, msg)
            break  

        else:  # flag == 0
            if cmp == "greater":
                send_msg(client_sock, msg)
            elif cmp == "less":
                log(f"Ignored: uuid={msg.uuid}, flag={msg.flag}")
            else:  
                leader_id = my_id
                state = 1
                announce = Message(my_id, 1)
                sent_own_announcement = True
                send_msg(client_sock, announce)
                log(f"Leader is decided to {leader_id}.")


if __name__ == "__main__":
    main()