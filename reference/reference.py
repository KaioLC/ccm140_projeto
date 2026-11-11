import time
import zmq
import chat_pb2 as chat

def main():
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind("tcp://*:5559")

    servers = {}          # {server_name: rank}
    last_heartbeat = {}   # {server_name: timestamp_seconds}
    next_rank = 1
    clock = 0
    TIMEOUT = 45.0        # Timeout em segundos para considerar servidor inativo

    print("[REFERENCE] Servico de referencia ativo na porta 5559", flush=True)

    while True:
        msg = socket.recv()
        now_ts = int(time.time() * 1000)
        now_sec = time.time()

        # Evicção de servidores sem heartbeat recente
        expired = [s for s, t in last_heartbeat.items() if now_sec - t > TIMEOUT]
        for s in expired:
            print(f"[REFERENCE] Servidor '{s}' inativo removido da lista (timeout > {TIMEOUT}s)", flush=True)
            del last_heartbeat[s]
            if s in servers:
                del servers[s]

        req = chat.ReferenceRequest()
        req.ParseFromString(msg)
        clock = max(clock, req.clock) + 1

        res = chat.ReferenceResponse()
        res.id = f"{now_ts}_{int(time.time() * 1000) % 10000}"
        res.timestamp = now_ts
        res.clock = clock

        payload_type = req.WhichOneof("payload")

        if payload_type == "reg":
            name = req.reg.name
            if name not in servers:
                servers[name] = next_rank
                next_rank += 1
                print(f"[REFERENCE] Novo servidor registrado: '{name}' com rank {servers[name]}", flush=True)
            else:
                print(f"[REFERENCE] Servidor reconectado: '{name}' (rank {servers[name]})", flush=True)

            last_heartbeat[name] = now_sec
            res.reg.name = name
            res.reg.rank = servers[name]

        elif payload_type == "list":
            for s_name, s_rank in servers.items():
                s_info = res.list.servers.add()
                s_info.name = s_name
                s_info.rank = s_rank
            print(f"[REFERENCE] Listagem de servidores enviada: {len(servers)} ativo(s)", flush=True)

        elif payload_type == "heartbeat":
            name = req.heartbeat.name
            if name not in servers:
                servers[name] = next_rank
                next_rank += 1
            last_heartbeat[name] = now_sec

            res.heartbeat.status = True
            res.heartbeat.physical_time = now_ts
            print(f"[REFERENCE] Heartbeat recebido de '{name}'. Hora fisica: {now_ts}", flush=True)

        socket.send(res.SerializeToString())

if __name__ == "__main__":
    main()
