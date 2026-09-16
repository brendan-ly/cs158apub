
import json
import socket
import sys
import threading
import time
import uuid

BUFFER_SIZE = 1024

class Message:
    def __init__(self, uuid, flag=0):
        self.uuid = uuid
        self.flag = flag

    def to_json(self):
        """Serialize to a JSON string for sending over socket."""
        my_dict = { "uuid": str(self.uuid), "flag": self.flag }

        return json.dumps(my_dict)

    @staticmethod
    def from_json(raw):
        """Parse a JSON string received from a neighbor back into a Message."""
        my_string = json.loads(raw)

        return Message(uuid.UUID(my_string["uuid"]), int(my_string["flag"]))


def read_config(path="config.txt"):
    """Function to read two-line config.txt to initialize connections"""

    # open file
    with open(path, "r") as file:
        # First line is your IP address as a server
        first = file.readline()
        first_cleaned = first.strip()
        first_split = first_cleaned.split(",")

        my_host = first_split[0]
        my_port = int(first_split[1])

        # Second line is to retrieve the client aka the successor node
        second = file.readline()
        second_cleaned = second.strip()
        second_split = second_cleaned.split(",")

        client_host = second_split[0]
        client_port = int(second_split[1])

        return ((my_host, my_port), (client_host, client_port))

class Logger:
    """Function to write received/sent lines to the log.txt"""

    def __init__(self, path="log.txt"):
        self.file = open(path, "w")

        # make sure no other thread slips in during writing to log
        self.lock = threading.Lock()

    def write(self, line):
        # write line to the log
        with self.lock:
            self.file.write(line + "\n")

            self.file.flush()

            print(line)

class LeaderElectionNode:
    """One process in the ring: server side, client side, and the algorithm."""

    def __init__(self, config="config.txt", log="log.txt"):
        self.addr = None
        self.server_sock = None
        self.config_path = config
        self.log_path = log
        self.my_uuid = uuid.uuid4()
        self.leader_id = None
        self.state = 0
        self.done = threading.Event()

        self.my_addr, self.succ_addr = read_config(self.config_path)
        self.log = Logger(self.log_path)

        # When a process starts, it logs its ID first.
        self.log.write(f"My uuid is {self.my_uuid}")

        self.conn = None                    # Inbound socket
        self.client_sock = None             # Outbound socket
        self.connected = threading.Event()

        self.buffer = ""

    def start_server(self):

        # Create a TCP socket
        self.server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        # Avoid "Address already in use" when you restart
        self.server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        self.server_sock.bind(("", self.my_addr[1]))

        # Start listening
        self.server_sock.listen(1)

        # Blocks until a neighbor connects
        self.conn, self.addr = self.server_sock.accept()

        # Tell main thread a client that predecessor's connection is ready
        self.connected.set()

        # print(f"accepted from {self.addr}")

    def connect_to_successor(self):
        # Connect to the next node in the ring, keep trying until its up
        while True:
            try:
                self.client_sock = socket.create_connection(self.succ_addr)
                break

            except ConnectionRefusedError:
                time.sleep(1)

    def send(self, message):
        # send the message
        self.client_sock.sendall(message.to_json().encode())

        self.log.write(f"Sent: uuid={message.uuid}, flag={message.flag}")

    def handle_message(self, message):
        """Apply the election rules to one received message."""
        if message.uuid > self.my_uuid:
            comparison = "greater"
        elif message.uuid < self.my_uuid:
            comparison = "less"
        else:
            comparison = "same"

        state = str(self.state)
        # if a process in state 1, show the leader's ID
        if self.state == 1:
            state += f", leader={self.leader_id}"

        self.log.write(
            f"Received: uuid={message.uuid}, "
            f"flag={message.flag}, {comparison}, {state}"
        )

        # election still in progress
        if message.flag == 0:

            # if message.uuid > self.my_uuid, forward the message
            if comparison == "greater":
                self.send(message)

            # else ignore the message and do nothing
            elif comparison == "less":
                self.log.write(
                    f"Ignored: uuid={message.uuid}, flag={message.flag}"
                )

            # else they are equal, so elect myself
            else:
                self.leader_id = self.my_uuid
                self.state = 1
                self.log.write(f"Leader is decided to {self.leader_id}.")
                self.send(Message(self.my_uuid, 1))

        # flag = 1, election ended
        else:
            if comparison != "same":

                # pass the leader on to the other nodes
                self.leader_id = message.uuid
                self.state = 1
                self.send(message)

            self.done.set()

    def run(self):
        """Start the ring, kick off the election, then react until done."""

        # Start a thread
        threading.Thread(target=self.start_server, daemon=True).start()

        # blocking between accept and connect, until everyone is ready
        input("press Enter when everyone is ready.")

        self.connect_to_successor()

        self.connected.wait()

        # send uuid once connected to a server as initial message
        self.send(Message(self.my_uuid, 0))

        while not self.done.is_set():
            # receive the data
            data = self.conn.recv(BUFFER_SIZE)

            if not data:
                # Empty bytes means the neighbor closed the connection.
                self.log.write("Predecessor closed the connection.")
                break

            self.buffer += data.decode()

            while "}" in self.buffer:
                end = self.buffer.index("}") + 1
                raw = self.buffer[:end]
                self.buffer = self.buffer[end:]

                self.handle_message(Message.from_json(raw))

                if self.done.is_set():
                    break

        self.log.write(f"leader is {self.leader_id}")

if __name__ == "__main__":
    # run with optional arguments to provide config and log
    # python myleprocess.py config1.txt log1.txt
    config_path = sys.argv[1] if len(sys.argv) > 1 else "config.txt"
    log_path = sys.argv[2] if len(sys.argv) > 2 else "log.txt"

    node = LeaderElectionNode(config_path, log_path)
    node.run()
