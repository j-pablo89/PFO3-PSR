import json
import socket
import threading

class Conn:
   
    def __init__(self, sock: socket.socket):
        self.sock = sock
        self.reader = sock.makefile("r", encoding="utf-8", newline="\n")
        self._send_lock = threading.Lock()

    def send(self, obj: dict) -> bool:
        data = (json.dumps(obj) + "\n").encode("utf-8")
        try:
            with self._send_lock:
                self.sock.sendall(data)
            return True
        except OSError:
            return False

    def recv(self):
        try:
            line = self.reader.readline()
        except (OSError, ValueError):
            return None
        if not line:
            return None
        try:
            return json.loads(line)
        except json.JSONDecodeError:
            return {"type": "invalid"}

    def close(self) -> None:
        try:
            self.sock.close()
        except OSError:
            pass
