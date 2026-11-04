import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import grpc

from protocols import voting_pb2
from protocols import voting_pb2_grpc

VOTE_TIMEOUT = 2.5
STATE_TIMEOUT = 0.8
RETRY_WINDOW = 10
RETRY_DELAY = 0.5


def load_nodes():
    """Lê os nós da variável NODES, no formato id=host:porta,id=host:porta."""
    if "NODES" not in os.environ:
        sys.exit("Defina a variável NODES. No Docker: docker compose exec client-a python client.py results")

    nodes = {}
    for item in os.environ["NODES"].split(","):
        node_id, address = item.split("=")
        nodes[int(node_id)] = address
    return nodes


NODES = load_nodes()


class LamportClock:

    def __init__(self):
        self.value = 0
        self.lock = threading.Lock()

    def tick(self):
        with self.lock:
            self.value += 1
            return self.value

    def observe(self, received):
        """Consultar o estado de um nó não é um evento da votação: só acompanha o relógio."""
        with self.lock:
            self.value = max(self.value, received)


CLOCK = LamportClock()


def call(node_id, method, request, timeout):
    with grpc.insecure_channel(NODES[node_id]) as channel:
        stub = voting_pb2_grpc.VotingServiceStub(channel)
        return getattr(stub, method)(request, timeout=timeout)


def cast_vote(voter_id, candidate_id, entry_node=None, known_offline=set):
    """Envia o voto pelo nó de entrada e segue o líder indicado.

    Durante uma eleição ninguém aceita o voto, então tenta de novo por alguns segundos.
    known_offline informa os nós já vistos fora do ar: não vale esperar o timeout deles.
    """
    deadline = time.monotonic() + RETRY_WINDOW

    while True:
        offline = known_offline()
        targets = sorted((node_id for node_id in NODES if node_id not in offline), key=lambda node_id: node_id != entry_node)
        tried = set()
        steps = [f"Nó {entry_node}: fora do ar"] if entry_node in offline else []

        while targets:
            node_id = targets.pop(0)
            if node_id in tried:
                continue
            tried.add(node_id)

            request = voting_pb2.VoteRequest(
                voter_id=voter_id,
                candidate_id=candidate_id,
                lamport_timestamp=CLOCK.tick(),
            )
            try:
                response = call(node_id, "CastVote", request, VOTE_TIMEOUT)
            except grpc.RpcError:
                steps.append(f"Nó {node_id}: fora do ar")
                continue

            steps.append(f"Nó {node_id}: {response.message}")

            if not response.retry:
                return {"success": response.success, "message": response.message, "steps": steps}

            if response.leader_id in NODES and response.leader_id not in offline:
                targets.insert(0, response.leader_id)

        if time.monotonic() >= deadline:
            return {"success": False, "message": "Nenhum nó aceitou o voto. Tente novamente.", "steps": steps}

        time.sleep(RETRY_DELAY)


def node_state(node_id):
    try:
        response = call(node_id, "GetResults", voting_pb2.ResultsRequest(), STATE_TIMEOUT)
    except grpc.RpcError:
        return {"id": node_id, "online": False}

    CLOCK.observe(response.lamport_clock)
    return {
        "id": node_id,
        "online": True,
        "leader_id": response.leader_id,
        "accepting_votes": response.accepting_votes,
        "total_votes": response.total_votes,
        "results": {result.candidate_id: result.votes for result in response.results},
        "lamport_clock": response.lamport_clock,
    }


def cluster_state():
    """Consulta todos os nós em paralelo: um nó fora do ar não atrasa os outros."""
    with ThreadPoolExecutor(max_workers=len(NODES)) as pool:
        return list(pool.map(node_state, sorted(NODES)))


def print_results():
    for node in cluster_state():
        if not node["online"]:
            print(f"Nó {node['id']}: fora do ar")
            continue

        role = "líder" if node["leader_id"] == node["id"] else "seguidor"
        counts = ", ".join(f"candidato {candidate}: {votes}" for candidate, votes in sorted(node["results"].items()))
        print(f"Nó {node['id']} ({role}): {node['total_votes']} votos | {counts} | Lamport {node['lamport_clock']}")


if __name__ == "__main__":
    if len(sys.argv) == 4 and sys.argv[1] == "vote":
        result = cast_vote(int(sys.argv[2]), int(sys.argv[3]))
        print("\n".join(result["steps"]))
        if not result["steps"] or result["message"] not in result["steps"][-1]:
            print(result["message"])
        sys.exit(0 if result["success"] else 1)

    if len(sys.argv) == 2 and sys.argv[1] == "results":
        print_results()
        sys.exit(0)

    print("Uso: python client.py vote <eleitor> <candidato>")
    print("     python client.py results")
    sys.exit(2)
