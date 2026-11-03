import time
from collections import Counter
from node import Node
from utils import replicate_vote, vote_records
import voting_pb2
import voting_pb2_grpc

class VotingServicer(voting_pb2_grpc.VotingServiceServicer):

    def __init__ (self, node: Node):
        self.node = node

    def CastVote(self, request, context):
        with self.node.lock:
            self.node.lamport_clock = max(self.node.lamport_clock, request.lamport_timestamp) + 1

            if self.node.current_leader != self.node.node_id:
                return voting_pb2.VoteResponse(
                    success=False,
                    message="Este nó não é o líder",
                    retry=True,
                    leader_id=self.node.current_leader or 0,
                )

            if not self.node.synced:
                return voting_pb2.VoteResponse(
                    success=False,
                    message="Líder ainda não sincronizou com a maioria dos nós",
                    retry=True,
                )

            if request.voter_id in self.node.votes:
                return voting_pb2.VoteResponse(success=False, message="Eleitor já realizou o voto!")

            if request.voter_id in self.node.pending_voters:
                return voting_pb2.VoteResponse(
                    success=False,
                    message="Voto deste eleitor em processamento",
                    retry=True,
                )

            self.node.pending_voters.add(request.voter_id)
            lamport_timestamp = self.node.lamport_clock

        replicated = replicate_vote(self.node, request.voter_id, request.candidate_id, lamport_timestamp)

        with self.node.lock:
            self.node.pending_voters.discard(request.voter_id)

            if not replicated:
                return voting_pb2.VoteResponse(
                    success=False,
                    message="Sem maioria dos nós, voto não confirmado",
                    retry=True,
                )

            self.node.add_vote(request.voter_id, request.candidate_id, lamport_timestamp)
            return voting_pb2.VoteResponse(success=True, message="Voto confirmado")

    def ReplicateState(self, request, context):
        with self.node.lock:
            self.node.lamport_clock = max(self.node.lamport_clock, request.lamport_timestamp) + 1

            # Recusa um voto que contradiz o já registrado para o eleitor
            current = self.node.votes.get(request.voter_id)
            if current is not None and current[0] != request.candidate_id:
                return voting_pb2.ReplicationResponse(success=False)

            self.node.add_vote(request.voter_id, request.candidate_id, request.lamport_timestamp)
            return voting_pb2.ReplicationResponse(success=True)

    def SyncState(self, request, context):
        with self.node.lock:
            for vote in request.votes:
                self.node.add_vote(vote.voter_id, vote.candidate_id, vote.lamport_timestamp)
            return voting_pb2.SyncResponse(votes=vote_records(self.node))

    def GetResults(self, request, context):
        with self.node.lock:
            counts = Counter(candidate_id for candidate_id, _ in self.node.votes.values())
            return voting_pb2.ResultsResponse(
                node_id=self.node.node_id,
                leader_id=self.node.current_leader or 0,
                accepting_votes=self.node.current_leader == self.node.node_id and self.node.synced,
                total_votes=len(self.node.votes),
                results=[
                    voting_pb2.CandidateResult(candidate_id=candidate_id, votes=votes)
                    for candidate_id, votes in sorted(counts.items())
                ],
            )

    def StartElection(self, request, context):
        # Algoritmo Bully
        if self.node.node_id > request.sender_id:
            return voting_pb2.ElectionResponse(success=True)
        return voting_pb2.ElectionResponse(success=False)

    def Heartbeat(self, request, context):
        with self.node.lock:
            self.node.lamport_clock = max(self.node.lamport_clock, request.lamport_timestamp) + 1

            current_leader = self.node.current_leader
            if current_leader is not None and request.leader_id < current_leader:
                return voting_pb2.HeartbeatResponse(acknowledged=False)

            if request.leader_id != current_leader:
                print(f"[{self.node.node_id}] Reconhece o nó {request.leader_id} como líder.")
            self.node.current_leader = request.leader_id
            self.node.last_heartbeat_time = time.time()
            return voting_pb2.HeartbeatResponse(acknowledged=True, vote_count=len(self.node.votes))
        
    def DeclareCoordinator(self, request, context):
        with self.node.lock:
            self.node.current_leader = request.new_leader_id
            self.node.is_election_in_progress = False
            self.node.last_heartbeat_time = time.time()
        return voting_pb2.CoordinatorResponse(acknowledged=True)


