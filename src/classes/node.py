import time
import threading

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

