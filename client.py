import argparse
import itertools
import json
import socket
import threading
from common import Conn

DEMO = [
    ("suma", [1, 2, 3, 4, 5]),
    ("factorial", 20),
    ("primo", 1000003),
    ("mayusculas", "hola sistemas distribuidos"),
    ("invertir", "socket"),
    ("dormir", 1),
    ("dormir", 1),
    ("dormir", 1),
    ("factorial", -1),      
    ("raiz", 9),             
]

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=5000)
    p.add_argument("--task", nargs=2, metavar=("OP", "PAYLOAD_JSON"),
                   help='ej: --task factorial 10   |   --task suma "[1,2,3]"')
    a = p.parse_args()

    if a.task:
        try:
            payload = json.loads(a.task[1])
        except json.JSONDecodeError:
            payload = a.task[1]
        tareas = [(a.task[0], payload)]
    else:
        tareas = DEMO

    conn = Conn(socket.create_connection((a.host, a.port)))
    ids = itertools.count(1)
    enviadas = {}
    for op, payload in tareas:
        tid = next(ids)
        enviadas[tid] = (op, payload)
        conn.send({"type": "task", "task_id": tid, "op": op, "payload": payload})
        print(f"-> tarea {tid}: {op}({payload})")

    pendientes = len(enviadas)
    while pendientes:
        msg = conn.recv()
        if msg is None:
            print("conexión cerrada por el servidor")
            break
        if msg.get("type") == "result":
            op, _ = enviadas[msg["task_id"]]
            estado = msg["result"] if msg["ok"] else f"ERROR: {msg['error']}"
            print(f"<- tarea {msg['task_id']} ({op}) [worker {msg['worker']}]: {estado}")
            pendientes -= 1
    conn.close()

if __name__ == "__main__":
    main()
