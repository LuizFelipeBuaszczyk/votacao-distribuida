import json
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from client import CLOCK, NODES, cast_vote, node_state

PORT = 8080
MAX_BODY = 1024
STATE_INTERVAL = 0.3
PAGE = (Path(__file__).parent / "panel.html").read_bytes()

CONFIG = {
    "name": os.environ.get("CLIENT_NAME", "Cliente"),
    "entry_node": int(os.environ.get("ENTRY_NODE", min(NODES))),
    "first_voter": int(os.environ.get("FIRST_VOTER", 1)),
    "candidates": int(os.environ.get("CANDIDATES", 3)),
    "nodes": sorted(NODES),
}

next_voter = CONFIG["first_voter"]

states = {node_id: {"id": node_id, "online": False} for node_id in NODES}


def watch_node(node_id):
    while True:
        states[node_id] = node_state(node_id)
        time.sleep(STATE_INTERVAL)


def known_offline():
    return {node_id for node_id, state in states.items() if not state["online"]}


def read_vote(body):
    """Valida o voto recebido da página. Retorna (eleitor, candidato, nó de entrada)."""
    data = json.loads(body)
    voter = data["voter"]
    candidate = data["candidate"]
    entry = data.get("entry")

    if not isinstance(voter, int) or voter <= 0:
        raise ValueError("O número do eleitor deve ser maior que zero.")
    if not isinstance(candidate, int) or not 1 <= candidate <= CONFIG["candidates"]:
        raise ValueError(f"Candidato inválido. Escolha de 1 a {CONFIG['candidates']}.")
    if entry not in NODES:
        entry = CONFIG["entry_node"]

    return voter, candidate, entry


class PanelHandler(BaseHTTPRequestHandler):

    def send_body(self, status, body, content_type):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def send_json(self, status, data):
        self.send_body(status, json.dumps(data).encode("utf-8"), "application/json; charset=utf-8")

    def do_GET(self):
        if self.path == "/":
            self.send_body(200, PAGE, "text/html; charset=utf-8")
        elif self.path == "/api/state":
            nodes = [states[node_id] for node_id in sorted(NODES)]
            self.send_json(200, {"config": CONFIG, "next_voter": next_voter, "client_clock": CLOCK.value, "nodes": nodes})
        else:
            self.send_json(404, {"message": "Página não encontrada."})

    def do_POST(self):
        global next_voter

        if self.path != "/api/vote":
            self.send_json(404, {"message": "Página não encontrada."})
            return

        length = int(self.headers.get("Content-Length", 0))
        if length > MAX_BODY:
            self.send_json(413, {"success": False, "message": "Pedido grande demais.", "steps": []})
            return

        try:
            voter, candidate, entry = read_vote(self.rfile.read(length))
        except ValueError as error:
            self.send_json(400, {"success": False, "message": str(error), "steps": []})
            return
        except (KeyError, TypeError):
            self.send_json(400, {"success": False, "message": "Informe o eleitor e o candidato.", "steps": []})
            return

        next_voter = max(next_voter, voter + 1)
        self.send_json(200, cast_vote(voter, candidate, entry, known_offline))

    def log_message(self, format, *args):
        pass


if __name__ == "__main__":
    for node_id in NODES:
        threading.Thread(target=watch_node, args=(node_id,), daemon=True).start()

    print(f"{CONFIG['name']}: abra {os.environ.get('PANEL_URL', f'http://localhost:{PORT}')}")
    ThreadingHTTPServer(("0.0.0.0", PORT), PanelHandler).serve_forever()
