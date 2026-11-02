import os
import threading
import grpc
import time
from concurrent import futures

from voting_servicer import VotingServicer
from node import Node
from jobs import monitor, heartbeat
from utils import run_bully_election

import voting_pb2_grpc

def run():
    node_id = int(os.environ["NODE_ID"])
    peers = os.environ["PEERS"].split(",")
    port = 50051

    node = Node(node_id, peers)

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=3))
    voting_pb2_grpc.add_VotingServiceServicer_to_server(VotingServicer(node), server)
    server.add_insecure_port(f"[::]:{port}")
    server.start()

    t_monitor = threading.Thread(target=monitor, args=(node,), daemon=True)
    t_heartbeat = threading.Thread(target=heartbeat, args=(node,), daemon=True)
    t_monitor.start()
    t_heartbeat.start()

    time.sleep(2)
    run_bully_election(node)

    server.wait_for_termination()

print(f"Iniciando node {os.getenv('NODE_ID', None)}")

if __name__ == "__main__":
    run()
