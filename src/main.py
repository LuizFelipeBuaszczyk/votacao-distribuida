import grpc

from concurrent import futures

from classes.voting_servicer import VotingServicer
from classes.node import Node

import voting_pb2_grpc

def run():
    node_id = 1
    peers = []
    port = 50050

    node = Node(node_id, peers)

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=3))
    voting_pb2_grpc.add_VotingServiceServicer_to_server(VotingServicer(node), server)
    server.add_insecure_port(f"[::]:{port}")
    server.start()

    server.wait_for_termination()

if __name__ == "__main__":
    run()
