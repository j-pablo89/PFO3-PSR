import argparse
import itertools
import queue
import socket
import threading
import time
from common import Conn

pending_queue = queue.Queue()    
workers = {}                       
workers_lock = threading.Lock()
clients = {}                      
clients_lock = threading.Lock()
worker_ids = itertools.count(1)
client_ids = itertools.count(1)
stats = {"recibidas": 0, "completadas": 0}


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def pick_worker():
    if not workers:
        return None
    return min(workers, key=lambda w: len(workers[w]["inflight"]))


def dispatcher_loop():
    while True:
        task = pending_queue.get()
        while True:
            with workers_lock:
                wid = pick_worker()
                if wid is not None:
                    w = workers[wid]
                    w["inflight"][task["task_id"]] = task
                    conn = w["conn"]
                else:
                    conn = None
            if conn is None:
                time.sleep(0.2) 
                continue
            if conn.send({"type": "task", **task}):
                log(f"tarea {task['task_id']} ({task['op']}) -> worker {wid}")
                break
            with workers_lock:
                workers.get(wid, {"inflight": {}})["inflight"].pop(task["task_id"], None)


def send_to_client(client_id, msg):
    with clients_lock:
        conn = clients.get(client_id)
    if conn:
        conn.send(msg)


def handle_worker(sock, addr):
    conn = Conn(sock)
    hello = conn.recv()
    if not hello or hello.get("type") != "register":
        conn.close()
        return
    wid = next(worker_ids)
    with workers_lock:
        workers[wid] = {"conn": conn, "inflight": {}}
    log(f"worker {wid} registrado desde {addr[0]}:{addr[1]} (hilos={hello.get('threads')})")
    try:
        while True:
            msg = conn.recv()
            if msg is None:
                break
            if msg.get("type") == "result":
                with workers_lock:
                    task = workers[wid]["inflight"].pop(msg["task_id"], None)
                if task is None:
                    continue
                stats["completadas"] += 1
                send_to_client(task["client_id"], {
                    "type": "result", "task_id": msg["task_id"],
                    "ok": msg["ok"], "result": msg.get("result"),
                    "error": msg.get("error"), "worker": wid,
                })
    finally:
        with workers_lock:
            lost = workers.pop(wid, {"inflight": {}})["inflight"]
        conn.close()
        log(f"worker {wid} desconectado; reencolando {len(lost)} tareas")
        for t in lost.values():
            pending_queue.put(t)


def handle_client(sock, addr):
    conn = Conn(sock)
    cid = next(client_ids)
    with clients_lock:
        clients[cid] = conn
    log(f"cliente {cid} conectado desde {addr[0]}:{addr[1]}")
    try:
        while True:
            msg = conn.recv()
            if msg is None:
                break
            if msg.get("type") != "task" or "op" not in msg or "task_id" not in msg:
                conn.send({"type": "error", "error": "mensaje inválido"})
                continue
            stats["recibidas"] += 1
            pending_queue.put({"task_id": msg["task_id"], "op": msg["op"],
                               "payload": msg.get("payload"), "client_id": cid})
    finally:
        with clients_lock:
            clients.pop(cid, None)
        conn.close()
        log(f"cliente {cid} desconectado")


def accept_loop(port, handler, name, host):
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((host, port))
    srv.listen(128)
    log(f"escuchando {name} en {host}:{port}")
    while True:
        s, a = srv.accept()
        threading.Thread(target=handler, args=(s, a), daemon=True).start()


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--host", default="0.0.0.0")
    p.add_argument("--client-port", type=int, default=5000)
    p.add_argument("--worker-port", type=int, default=5001)
    a = p.parse_args()
    threading.Thread(target=dispatcher_loop, daemon=True).start()
    threading.Thread(target=accept_loop, args=(a.worker_port, handle_worker, "workers", a.host), daemon=True).start()
    try:
        accept_loop(a.client_port, handle_client, "clientes", a.host)
    except KeyboardInterrupt:
        log("servidor detenido")


if __name__ == "__main__":
    main()
