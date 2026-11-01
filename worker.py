import argparse
import math
import time
from concurrent.futures import ThreadPoolExecutor

from common import Conn
import socket

def es_primo(n):
    if n < 2:
        return False
    for i in range(2, int(math.isqrt(n)) + 1):
        if n % i == 0:
            return False
    return True

OPERACIONES = {
    "suma": lambda p: sum(p),
    "factorial": lambda p: math.factorial(int(p)),
    "primo": lambda p: es_primo(int(p)),
    "mayusculas": lambda p: str(p).upper(),
    "invertir": lambda p: str(p)[::-1],
    "dormir": lambda p: (time.sleep(float(p)), f"dormí {p}s")[1], 
}

def ejecutar(conn, wid_label, msg):
    try:
        fn = OPERACIONES[msg["op"]]
        res = fn(msg.get("payload"))
        conn.send({"type": "result", "task_id": msg["task_id"], "ok": True, "result": res})
    except KeyError:
        conn.send({"type": "result", "task_id": msg["task_id"], "ok": False,
                   "error": f"operación desconocida: {msg['op']}"})
    except Exception as e:  # noqa: BLE001 - se informa cualquier error al cliente
        conn.send({"type": "result", "task_id": msg["task_id"], "ok": False, "error": str(e)})

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=5001)
    p.add_argument("--threads", type=int, default=4)
    a = p.parse_args()

    sock = socket.create_connection((a.host, a.port))
    conn = Conn(sock)
    conn.send({"type": "register", "threads": a.threads})
    print(f"worker conectado a {a.host}:{a.port} con pool de {a.threads} hilos", flush=True)

    with ThreadPoolExecutor(max_workers=a.threads) as pool:
        try:
            while True:
                msg = conn.recv()
                if msg is None:
                    break
                if msg.get("type") == "task":
                    pool.submit(ejecutar, conn, "w", msg)
        except KeyboardInterrupt:
            pass
    print("worker finalizado")

if __name__ == "__main__":
    main()
