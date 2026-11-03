import sys

import grpc

import voting_pb2
import voting_pb2_grpc

NODES = {1: "node1:50051", 2: "node2:50051", 3: "node3:50051"}


def call(node_id, method, request):
    with grpc.insecure_channel(NODES[node_id]) as channel:
        stub = voting_pb2_grpc.VotingServiceStub(channel)
        return getattr(stub, method)(request, timeout=3)


def vote(voter_id, candidate_id):
    request = voting_pb2.VoteRequest(voter_id=voter_id, candidate_id=candidate_id)
    targets = list(NODES)
    tried = set()

    while targets:
        node_id = targets.pop(0)
        if node_id in tried:
            continue
        tried.add(node_id)

        try:
            response = call(node_id, "CastVote", request)
        except grpc.RpcError:
            print(f"Nó {node_id} fora do ar")
            continue

        if response.retry:
            print(f"Nó {node_id}: {response.message}")
            if response.leader_id in NODES:
                targets.insert(0, response.leader_id)
            continue

        print(f"Nó {node_id}: {response.message}")
        return response.success

    print("Nenhum nó aceitou o voto. Tente novamente.")
    return False


def results():
    for node_id in NODES:
        try:
            response = call(node_id, "GetResults", voting_pb2.ResultsRequest())
        except grpc.RpcError:
            print(f"Nó {node_id}: fora do ar")
            continue

        role = "líder" if response.leader_id == response.node_id else "seguidor"
        counts = ", ".join(f"candidato {r.candidate_id}: {r.votes}" for r in response.results)
        print(f"Nó {node_id} ({role}): {response.total_votes} votos | {counts}")


if __name__ == "__main__":
    if len(sys.argv) == 4 and sys.argv[1] == "vote":
        sys.exit(0 if vote(int(sys.argv[2]), int(sys.argv[3])) else 1)
    if len(sys.argv) == 2 and sys.argv[1] == "results":
        results()
        sys.exit(0)

    print("Uso: python client.py vote <eleitor> <candidato>")
    print("     python client.py results")
    sys.exit(2)
