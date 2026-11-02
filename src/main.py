import threading
import grpc
import time

from concurrent import futures

import voting_pb2
import voting_pb2_grpc

class Node:

    def __init__(self, node_id: int, peers: list):
        self.node_id = node_id
        self.peers = peers

        self.current_leader = None
        self.lamport_clock = 0
        self.lock = threading.Lock()

        self.voted_voters = set()
        self.votes_count = {}

        self.last_heartbeat_time = time.time()
        self.is_election_in_progress = False


class VotingServicer(voting_pb2_grpc.VotingServiceServicer):

    def __init__ (self, node: Node):
        self.node = node

    def CastVote(self, request, context):
        with self.node.lock:
            self.node.lamport_clock = max(self.node.lamport_clock, request.lamport_timestamp) + 1
            

            if request.voter_id in self.node.voted_voters:
                return {"success": False, "message": "Eleitor já realizou o voto!"}
            
            self.state.voted_voters.add(request.voted_id)
            self.votes_count[request.candidate_id] += 1

            # TODO Mandar ReplicateState
            return {"success": True, "message": "Voto confirmado"}

    def StartElection(self, request, context):
        # Algoritmo Bully
        if self.node.node_id > request.sender_id:
            return {"success": True}
        return {"success": False}

    def HeartBeat(self, request, context):
        with self.node.lock:
            if request.leader_id == self.node.current_leader:
                self.node.current_leader = request.leader_id
                self.node.last_heartbeat_time = time.time()
                self.node.lamport_clock = max(self.state.lamport_clock, request.lamport_timestamp) + 1
            return {"acknowledged": True}
        return {"acknowledged": False}

        
    def DeclareCoordinator(self, request, context):
        with self.node.lock:
            self.node.current_leader = request.new_leader_id
            self.node.is_election_in_progress = False
            self.node.last_heartbeat_time = time.time()
        return {"acknowledged": True}


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
