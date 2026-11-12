import os
import sys
import time
import random
import threading
import zmq
import chat_pb2

class Client:
    def __init__(self, broker_uri: str, proxy_uri: str, name: str):
        self.user = name
        self.clock = 0
        self.subscribed = set()
        self.context = zmq.Context()
        self.req_socket = self.context.socket(zmq.REQ)
        self.req_socket.connect(broker_uri)
        self.sub_socket = self.context.socket(zmq.SUB)
        self.sub_socket.connect(proxy_uri)
        self.user_id = self.gen_id()

    def get_timestamp(self) -> int:
        return int(time.time() * 1000)

    def gen_id(self) -> str:
        return f"{self.get_timestamp()}_{random.randint(0, 9999)}"

    def send_req(self, req: chat_pb2.Request) -> chat_pb2.Response:
        self.clock += 1
        req.id = self.gen_id()
        req.timestamp = self.get_timestamp()
        req.clock = self.clock

        self.req_socket.send(req.SerializeToString())
        print(f"[SEND] user={self.user} id={req.id} ts={req.timestamp} clock={req.clock}")

        reply = self.req_socket.recv()
        res = chat_pb2.Response()
        res.ParseFromString(reply)
        self.clock = max(self.clock, res.clock)

        print(f"[RECV] user={self.user} id={res.id} ts={res.timestamp} clock={self.clock}")
        return res

    def listen_sub(self):
        while True:
            try:
                topic = self.sub_socket.recv()
                data = self.sub_socket.recv()
                pub = chat_pb2.Publication()
                pub.ParseFromString(data)
                self.clock = max(self.clock, pub.clock)
                recv_ts = self.get_timestamp()

                print(f"[SUB {self.user}] canal={pub.channel} msg=\"{pub.content}\" ts_envio={pub.timestamp} ts_recebimento={recv_ts} clock={self.clock}")
            except Exception:
                pass

    def login(self):
        for attempt in range(1, 6):
            req = chat_pb2.Request()
            req.login.id = self.user_id
            req.login.user = self.user

            res = self.send_req(req)
            if res.login.status:
                print(f"[INFO] {res.login.message}")
                return

            print(f"[WARN] {res.login.message} Retrying ({attempt}/5)...")
            self.user_id = self.gen_id()
            time.sleep(2)

    def list_channels(self) -> list:
        req = chat_pb2.Request()
        req.list.SetInParent()
        res = self.send_req(req)
        return list(res.list.channels)

    def create_channel(self, name: str):
        req = chat_pb2.Request()
        req.channel.id = self.gen_id()
        req.channel.name = name

        res = self.send_req(req)
        print(f"[INFO] Channel '{name}': {res.channel.message}")

    def subscribe_channel(self, channel: str):
        if channel not in self.subscribed:
            self.subscribed.add(channel)
            self.sub_socket.setsockopt_string(zmq.SUBSCRIBE, channel)
            print(f"[INFO] Subscribed to channel: {channel}")

    def publish(self, channel: str, text: str):
        req = chat_pb2.Request()
        req.publish.id = self.gen_id()
        req.publish.user = self.user
        req.publish.channel = channel
        req.publish.content = text

        res = self.send_req(req)
        print(f"[INFO] Publish to '{channel}': {res.publish.message}")

    def run(self):
        self.login()

        sub_thread = threading.Thread(target=self.listen_sub, daemon=True)
        sub_thread.start()

        count = 1
        while True:
            channels = self.list_channels()

            if len(channels) < 5:
                self.create_channel(f"canal_{self.user}_{count}")
                count += 1
                channels = self.list_channels()

            if len(self.subscribed) < 3 and channels:
                for ch in channels:
                    if ch not in self.subscribed:
                        self.subscribe_channel(ch)
                        break

            if channels:
                target = random.choice(channels)
                for i in range(1, 11):
                    self.publish(target, f"Msg {i} from {self.user}")
                    time.sleep(1)

if __name__ == "__main__":
    host = os.getenv("HOSTNAME", "local")
    random.seed(int(time.time()))
    client = Client("tcp://broker:5555", "tcp://proxy:5558", f"bot_py_{host}")
    client.run()
