import os
import sys
import time
import random
import sqlite3
import threading
import zmq
import chat_pb2

class Database:
    def __init__(self, path: str):
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        self.conn = sqlite3.connect(path, check_same_thread=False)
        with self.conn:
            self.conn.execute("CREATE TABLE IF NOT EXISTS logins (id TEXT PRIMARY KEY, user TEXT, timestamp INT);")
            self.conn.execute("CREATE TABLE IF NOT EXISTS channels (id TEXT PRIMARY KEY, name TEXT UNIQUE, timestamp INT);")
            self.conn.execute("CREATE TABLE IF NOT EXISTS messages (id TEXT PRIMARY KEY, user TEXT, channel TEXT, content TEXT, timestamp INT);")

    def save_login(self, login_id: str, user: str, ts: int) -> bool:
        try:
            with self.conn:
                self.conn.execute("INSERT OR IGNORE INTO logins VALUES (?, ?, ?);", (login_id, user, ts))
            return True
        except Exception:
            return False

    def channel_exists(self, name: str) -> bool:
        try:
            cur = self.conn.cursor()
            cur.execute("SELECT 1 FROM channels WHERE name = ?;", (name,))
            return cur.fetchone() is not None
        except Exception:
            return False

    def create_channel(self, channel_id: str, name: str, ts: int) -> bool:
        try:
            with self.conn:
                self.conn.execute("INSERT OR IGNORE INTO channels VALUES (?, ?, ?);", (channel_id, name, ts))
            return True
        except Exception:
            return False

    def save_message(self, msg_id: str, user: str, channel: str, content: str, ts: int) -> bool:
        try:
            with self.conn:
                self.conn.execute("INSERT OR IGNORE INTO messages VALUES (?, ?, ?, ?, ?);", (msg_id, user, channel, content, ts))
            return True
        except Exception:
            return False

    def list_channels(self) -> list[str]:
        try:
            cur = self.conn.cursor()
            cur.execute("SELECT name FROM channels;")
            return [row[0] for row in cur.fetchall()]
        except Exception:
            return []

class Server:
    def __init__(self, broker_uri: str, proxy_pub_uri: str, proxy_sub_uri: str,
                 ref_uri: str, name: str, host_id: str, db_path: str):
        self.server_name = name
        self.endpoint = f"tcp://{host_id}:6000"
        self.proxy_sub_uri = proxy_sub_uri
        self.coordinator = ""
        self.rank = 0
        self.clock = 0
        self.clock_offset = 0
        self.req_count = 0
        self.running = True

        self.context = zmq.Context()
        self.rep_socket = self.context.socket(zmq.REP)
        self.rep_socket.connect(broker_uri)

        self.pub_socket = self.context.socket(zmq.PUB)
        self.pub_socket.connect(proxy_pub_uri)

        self.ref_socket = self.context.socket(zmq.REQ)
        self.ref_socket.connect(ref_uri)

        self.db = Database(db_path)

    def get_system_time(self) -> int:
        return int(time.time() * 1000)

    def get_timestamp(self) -> int:
        return self.get_system_time() + self.clock_offset

    def gen_id(self) -> str:
        return f"{self.get_timestamp()}_{random.randint(0, 9999)}"

    def register_in_reference(self):
        req = chat_pb2.ReferenceRequest()
        self.clock += 1
        req.id = self.gen_id()
        req.timestamp = self.get_timestamp()
        req.clock = self.clock
        req.reg.name = self.server_name
        req.reg.endpoint = self.endpoint

        self.ref_socket.send(req.SerializeToString())
        reply = self.ref_socket.recv()

        res = chat_pb2.ReferenceResponse()
        res.ParseFromString(reply)
        self.clock = max(self.clock, res.clock)
        self.rank = res.reg.rank

        print(f"[REF] Registrado: nome={self.server_name} rank={self.rank} endpoint={self.endpoint}")

    def fetch_server_list(self) -> list:
        req = chat_pb2.ReferenceRequest()
        self.clock += 1
        req.id = self.gen_id()
        req.timestamp = self.get_timestamp()
        req.clock = self.clock
        req.list.SetInParent()

        self.ref_socket.send(req.SerializeToString())
        reply = self.ref_socket.recv()

        res = chat_pb2.ReferenceResponse()
        res.ParseFromString(reply)
        self.clock = max(self.clock, res.clock)
        return list(res.list.servers)

    def announce_coordinator(self):
        self.coordinator = self.server_name
        ann = chat_pb2.CoordinatorAnnouncement()
        ann.coordinator = self.server_name
        ann.rank = self.rank
        ann.timestamp = self.get_timestamp()
        self.clock += 1
        ann.clock = self.clock

        self.pub_socket.send(b"servers", zmq.SNDMORE)
        self.pub_socket.send(ann.SerializeToString())

        print(f"[ELECTION] Eleito coordenador: {self.server_name} (rank {self.rank})")

    def start_election(self):
        print("[ELECTION] Iniciando eleicao por bullying...")
        servers = self.fetch_server_list()
        higher_alive = False

        for s in servers:
            if s.rank > self.rank and s.name != self.server_name and s.endpoint:
                try:
                    req_sock = self.context.socket(zmq.REQ)
                    req_sock.setsockopt(zmq.RCVTIMEO, 1000)
                    req_sock.setsockopt(zmq.SNDTIMEO, 1000)
                    req_sock.connect(s.endpoint)

                    msg = chat_pb2.ServerMessage()
                    self.clock += 1
                    msg.id = self.gen_id()
                    msg.timestamp = self.get_timestamp()
                    msg.clock = self.clock
                    msg.election.server = self.server_name
                    msg.election.rank = self.rank

                    req_sock.send(msg.SerializeToString())
                    reply = req_sock.recv()

                    rep = chat_pb2.ServerMessage()
                    rep.ParseFromString(reply)
                    self.clock = max(self.clock, rep.clock)
                    if rep.HasField("election_res") and rep.election_res.status:
                        higher_alive = True
                        print(f"[ELECTION] Servidor superior {s.name} respondeu OK")
                    req_sock.close()
                except Exception:
                    pass

        if not higher_alive:
            self.announce_coordinator()

    def sync_clock_with_coordinator(self):
        if self.coordinator == self.server_name:
            return

        if not self.coordinator:
            self.start_election()
            return

        coord_endpoint = ""
        servers = self.fetch_server_list()
        for s in servers:
            if s.name == self.coordinator:
                coord_endpoint = s.endpoint
                break

        if not coord_endpoint:
            self.start_election()
            return

        try:
            req_sock = self.context.socket(zmq.REQ)
            req_sock.setsockopt(zmq.RCVTIMEO, 1500)
            req_sock.setsockopt(zmq.SNDTIMEO, 1500)
            req_sock.connect(coord_endpoint)

            msg = chat_pb2.ServerMessage()
            self.clock += 1
            msg.id = self.gen_id()
            msg.timestamp = self.get_timestamp()
            msg.clock = self.clock
            msg.clock_sync.server = self.server_name

            req_sock.send(msg.SerializeToString())
            reply = req_sock.recv()

            rep = chat_pb2.ServerMessage()
            rep.ParseFromString(reply)
            self.clock = max(self.clock, rep.clock)
            if rep.HasField("clock_res"):
                coord_time = rep.clock_res.physical_time
                sys_now = self.get_system_time()
                self.clock_offset = coord_time - sys_now
                print(f"[BERKELEY] Sincronizado com coordenador {self.coordinator} | Offset: {self.clock_offset}ms | Hora: {self.get_timestamp()}")
                req_sock.close()
                return
            req_sock.close()
        except Exception:
            pass

        print(f"[WARN] Coordenador {self.coordinator} nao respondeu ao relogio.")
        self.start_election()

    def send_heartbeat(self):
        req = chat_pb2.ReferenceRequest()
        self.clock += 1
        req.id = self.gen_id()
        req.timestamp = self.get_timestamp()
        req.clock = self.clock
        req.heartbeat.name = self.server_name

        self.ref_socket.send(req.SerializeToString())
        reply = self.ref_socket.recv()

        res = chat_pb2.ReferenceResponse()
        res.ParseFromString(reply)
        self.clock = max(self.clock, res.clock)

        print(f"[HEARTBEAT] Enviado (msg #{self.req_count}) | Clock: {self.clock}")

    def replicate_event(self, rep_type, ev_id: str, user="", channel="", content="", name="", ts=0):
        ev = chat_pb2.ReplicationEvent()
        ev.sender = self.server_name
        ev.type = rep_type
        ev.id = ev_id
        ev.timestamp = ts
        self.clock += 1
        ev.clock = self.clock
        if user:
            ev.user = user
        if channel:
            ev.channel = channel
        if content:
            ev.content = content
        if name:
            ev.name = name

        self.pub_socket.send(b"replication", zmq.SNDMORE)
        self.pub_socket.send(ev.SerializeToString())

    def listen_peer(self):
        peer_rep = self.context.socket(zmq.REP)
        peer_rep.bind("tcp://0.0.0.0:6000")

        while self.running:
            try:
                raw = peer_rep.recv()
                req = chat_pb2.ServerMessage()
                req.ParseFromString(raw)
                self.clock = max(self.clock, req.clock)

                rep = chat_pb2.ServerMessage()
                rep.id = self.gen_id()
                rep.timestamp = self.get_timestamp()

                if req.HasField("clock_sync"):
                    rep.clock_res.server = self.server_name
                    rep.clock_res.physical_time = self.get_timestamp()
                elif req.HasField("election"):
                    rep.election_res.server = self.server_name
                    rep.election_res.status = True

                self.clock += 1
                rep.clock = self.clock
                peer_rep.send(rep.SerializeToString())
            except Exception:
                pass

    def listen_servers_sub(self):
        sub = self.context.socket(zmq.SUB)
        sub.connect(self.proxy_sub_uri)
        sub.setsockopt_string(zmq.SUBSCRIBE, "servers")

        while self.running:
            try:
                topic = sub.recv()
                data = sub.recv()
                ann = chat_pb2.CoordinatorAnnouncement()
                ann.ParseFromString(data)
                self.clock = max(self.clock, ann.clock)
                self.coordinator = ann.coordinator

                print(f"[ELECTION] Coordenador atualizado via 'servers': {self.coordinator} (rank {ann.rank})")
            except Exception:
                pass

    def listen_replication(self):
        sub = self.context.socket(zmq.SUB)
        sub.connect(self.proxy_sub_uri)
        sub.setsockopt_string(zmq.SUBSCRIBE, "replication")

        while self.running:
            try:
                topic = sub.recv()
                data = sub.recv()
                ev = chat_pb2.ReplicationEvent()
                ev.ParseFromString(data)
                if ev.sender == self.server_name:
                    continue

                self.clock = max(self.clock, ev.clock)

                if ev.type == chat_pb2.REP_LOGIN:
                    self.db.save_login(ev.id, ev.user, ev.timestamp)
                elif ev.type == chat_pb2.REP_CHANNEL:
                    self.db.create_channel(ev.id, ev.name, ev.timestamp)
                elif ev.type == chat_pb2.REP_MESSAGE:
                    self.db.save_message(ev.id, ev.user, ev.channel, ev.content, ev.timestamp)

                print(f"[REPLICA] Sincronizado: tipo={ev.type} id={ev.id} de={ev.sender}")
            except Exception:
                pass

    def get_login(self, req, ts: int, res):
        user = req.user
        res.login.id = req.id
        if self.db.save_login(req.id, user, ts):
            self.replicate_event(chat_pb2.REP_LOGIN, req.id, user=user, ts=ts)
            res.login.status = True
            res.login.message = "Login Successfully."
            print(f"[INFO] New session: {user}")
        else:
            res.login.status = False
            res.login.message = "Login Error."
            print(f"[WARN] Login failed: {user}")

    def create_channel(self, req, ts: int, res):
        name = req.name
        res.channel.id = req.id
        res.channel.name = name

        if self.db.channel_exists(name):
            res.channel.status = False
            res.channel.message = "Channel already exists."
            print(f"[WARN] Channel already exists: {name}")
        elif self.db.create_channel(req.id, name, ts):
            self.replicate_event(chat_pb2.REP_CHANNEL, req.id, name=name, ts=ts)
            res.channel.status = True
            res.channel.message = "Channel created."
            print(f"[INFO] Channel created: {name}")
        else:
            res.channel.status = False
            res.channel.message = "Channel already exists."
            print(f"[WARN] Channel already exists: {name}")

    def list_channels(self, res):
        for ch in self.db.list_channels():
            res.list.channels.append(ch)

    def publish_message(self, req, ts: int, res):
        res.publish.id = req.id
        if self.db.save_message(req.id, req.user, req.channel, req.content, ts):
            self.replicate_event(chat_pb2.REP_MESSAGE, req.id, user=req.user,
                                 channel=req.channel, content=req.content, ts=ts)

            pub = chat_pb2.Publication()
            pub.id = req.id
            pub.user = req.user
            pub.channel = req.channel
            pub.content = req.content
            pub.timestamp = self.get_timestamp()
            self.clock += 1
            pub.clock = self.clock

            self.pub_socket.send(req.channel.encode(), zmq.SNDMORE)
            self.pub_socket.send(pub.SerializeToString())

            res.publish.status = True
            res.publish.message = "Published OK."
            print(f"[PUB] canal={req.channel} user={req.user} msg=\"{req.content}\" ts={pub.timestamp} clock={pub.clock}")
        else:
            res.publish.status = False
            res.publish.message = "Publish Error."

    def run(self):
        self.register_in_reference()

        peer_t = threading.Thread(target=self.listen_peer, daemon=True)
        peer_t.start()

        sub_t = threading.Thread(target=self.listen_servers_sub, daemon=True)
        sub_t.start()

        repl_t = threading.Thread(target=self.listen_replication, daemon=True)
        repl_t.start()

        time.sleep(0.5)
        self.start_election()

        while True:
            msg = self.rep_socket.recv()
            req = chat_pb2.Request()
            req.ParseFromString(msg)
            self.clock = max(self.clock, req.clock)
            self.req_count += 1

            print(f"[RECV] id={req.id} ts={req.timestamp} clock={req.clock}")

            if self.req_count % 10 == 0:
                self.send_heartbeat()

            if self.req_count % 15 == 0:
                self.sync_clock_with_coordinator()

            res = chat_pb2.Response()
            res.id = self.gen_id()
            res.timestamp = self.get_timestamp()

            if req.HasField("login"):
                self.get_login(req.login, req.timestamp, res)
            elif req.HasField("channel"):
                self.create_channel(req.channel, req.timestamp, res)
            elif req.HasField("list"):
                self.list_channels(res)
            elif req.HasField("publish"):
                self.publish_message(req.publish, req.timestamp, res)

            self.clock += 1
            res.clock = self.clock

            self.rep_socket.send(res.SerializeToString())
            print(f"[SEND] id={res.id} ts={res.timestamp} clock={res.clock}")

if __name__ == "__main__":
    host = os.getenv("HOSTNAME", "local")
    random.seed(int(time.time()))
    server = Server(
        "tcp://broker:5556",
        "tcp://proxy:5557",
        "tcp://proxy:5558",
        "tcp://reference:5559",
        f"srv_py_{host}",
        host,
        f"/data/server_{host}.db"
    )
    server.run()
