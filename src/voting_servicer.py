import time
from node import Node
from utils import replicate_vote
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

            if request.voter_id in self.node.voted_voters:
                return voting_pb2.VoteResponse(success=False, message="Eleitor já realizou o voto!")
            
            # Reserva o eleitor para impedir outro voto dele durante a replicação
            self.node.voted_voters.add(request.voter_id)
            lamport_timestamp = self.node.lamport_clock

        replicated = replicate_vote(self.node, request.voter_id, request.candidate_id, lamport_timestamp)

        with self.node.lock:
            if not replicated:
                self.node.voted_voters.discard(request.voter_id)
                return voting_pb2.VoteResponse(
                    success=False,
                    message="Sem maioria dos nós, voto não confirmado",
                    retry=True,
                )

            self.node.votes_count[request.candidate_id] = self.node.votes_count.get(request.candidate_id, 0) + 1
            return voting_pb2.VoteResponse(success=True, message="Voto confirmado")

    def ReplicateState(self, request, context):
        with self.node.lock:
            self.node.lamport_clock = max(self.node.lamport_clock, request.lamport_timestamp) + 1

            if request.voter_id not in self.node.voted_voters:
                self.node.voted_voters.add(request.voter_id)
                self.node.votes_count[request.candidate_id] = self.node.votes_count.get(request.candidate_id, 0) + 1

            return voting_pb2.ReplicationResponse(success=True)

    def GetResults(self, request, context):
        with self.node.lock:
            return voting_pb2.ResultsResponse(
                node_id=self.node.node_id,
                leader_id=self.node.current_leader or 0,
                accepting_votes=self.node.current_leader == self.node.node_id,
                total_votes=sum(self.node.votes_count.values()),
                results=[
                    voting_pb2.CandidateResult(candidate_id=candidate, votes=votes)
                    for candidate, votes in sorted(self.node.votes_count.items())
                ],
            )

    def StartElection(self, request, context):
        # Algoritmo Bully
        if self.node.node_id > request.sender_id:
            return voting_pb2.ElectionResponse(success=True)
        return voting_pb2.ElectionResponse(success=False)

    def Heartbeat(self, request, context):
        with self.node.lock:
            if request.leader_id == self.node.current_leader:
                self.node.current_leader = request.leader_id
                self.node.last_heartbeat_time = time.time()
                self.node.lamport_clock = max(self.node.lamport_clock, request.lamport_timestamp) + 1
            return voting_pb2.HeartbeatResponse(acknowledged=True)
        return voting_pb2.HeartbeatResponse(acknowledged=False)
        
    def DeclareCoordinator(self, request, context):
        with self.node.lock:
            self.node.current_leader = request.new_leader_id
            self.node.is_election_in_progress = False
            self.node.last_heartbeat_time = time.time()
        return voting_pb2.CoordinatorResponse(acknowledged=True)


