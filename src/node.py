import time
import threading

class Node:

    def __init__(self, node_id: int, peers: list):
        self.node_id = node_id
        self.peers = peers

        self.current_leader = None
        self.lamport_clock = 0
        self.lock = threading.Lock()

        # voter_id -> (candidate_id, lamport_timestamp)
        self.votes = {}
        # Eleitores com voto em replicação
        self.pending_voters = set()

        # O líder só aceita votos depois de sincronizar com a maioria dos nós
        self.synced = False

        self.last_heartbeat_time = time.time()
        self.is_election_in_progress = False

    @property
    def majority(self) -> int:
        return (len(self.peers) + 1) // 2 + 1

    def add_vote(self, voter_id: int, candidate_id: int, lamport_timestamp: int):
        """Registra um voto. Em conflito, vale o mais antigo pelo relógio de Lamport.
        Deve ser chamado com o lock adquirido."""
        self.lamport_clock = max(self.lamport_clock, lamport_timestamp)
        current = self.votes.get(voter_id)
        if current is None or (lamport_timestamp, candidate_id) < (current[1], current[0]):
            self.votes[voter_id] = (candidate_id, lamport_timestamp)
