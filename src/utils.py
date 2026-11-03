import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from node import Node

def replicate_vote(node: Node, voter_id: int, candidate_id: int, lamport_timestamp: int) -> bool:
    """Envia o voto a todos os seguidores e retorna True assim que a maioria dos nós o tiver."""
    import voting_pb2
    import voting_pb2_grpc
    import grpc

    request = voting_pb2.ReplicationRequest(
        voter_id=voter_id,
        candidate_id=candidate_id,
        lamport_timestamp=lamport_timestamp,
    )

    def send(peer):
        try:
            with grpc.insecure_channel(peer) as channel:
                stub = voting_pb2_grpc.VotingServiceStub(channel)
                return stub.ReplicateState(request, timeout=1.0).success
        except grpc.RpcError:
            print(f"[{node.node_id}] Falha ao replicar o voto para {peer}")
            return False

    confirmed = 1  # o líder

    # Não espera um seguidor fora do ar quando a maioria já confirmou
    pool = ThreadPoolExecutor(max_workers=len(node.peers))
    try:
        for future in as_completed([pool.submit(send, peer) for peer in node.peers]):
            if future.result():
                confirmed += 1
            if confirmed >= node.majority:
                return True
        return False
    finally:
        pool.shutdown(wait=False)

def vote_records(node: Node) -> list:
    """Converte os votos do nó para o formato do gRPC. Deve ser chamado com o lock adquirido."""
    import voting_pb2

    return [
        voting_pb2.VoteRecord(voter_id=voter_id, candidate_id=candidate_id, lamport_timestamp=lamport_timestamp)
        for voter_id, (candidate_id, lamport_timestamp) in node.votes.items()
    ]

def exchange_votes(node: Node, peers: list) -> int:
    """Troca votos com os peers: os dois lados ficam com a união. Retorna quantos responderam."""
    import voting_pb2
    import voting_pb2_grpc
    import grpc

    with node.lock:
        request = voting_pb2.SyncRequest(sender_id=node.node_id, votes=vote_records(node))

    responded = 0
    for peer in peers:
        try:
            with grpc.insecure_channel(peer) as channel:
                stub = voting_pb2_grpc.VotingServiceStub(channel)
                response = stub.SyncState(request, timeout=1.0)
        except grpc.RpcError:
            continue

        with node.lock:
            for vote in response.votes:
                node.add_vote(vote.voter_id, vote.candidate_id, vote.lamport_timestamp)
        responded += 1

    return responded

def sync_leader(node: Node):
    """Busca os votos nos outros nós antes de o líder aceitar novos.

    O Bully elege o maior ID, não o nó mais atualizado. Todo voto confirmado está na
    maioria dos nós, então consultar a maioria garante encontrar todos eles.
    """
    responded = exchange_votes(node, node.peers)
    with node.lock:
        if node.current_leader == node.node_id and responded + 1 >= node.majority:
            node.synced = True
            print(f"[{node.node_id}] Sincronizado com {responded} nó(s): {len(node.votes)} votos.")

def run_bully_election(node: Node):
    import voting_pb2
    import voting_pb2_grpc
    import grpc
    
    higher_nodes_responded = False
    for peer in node.peers:
        peer_name, peer_port = peer.split(":")
        peer_id = int(peer_name.replace("node", ""))


        if peer_id > node.node_id:
            try:
                channel = grpc.insecure_channel(peer)
                stub = voting_pb2_grpc.VotingServiceStub(channel)
                res = stub.StartElection(
                    voting_pb2.ElectionRequest(sender_id=node.node_id),
                    timeout=1.0
                )
                if res.success:
                    higher_nodes_responded = True
            except Exception as e:
                print(str(e))
                pass

    if higher_nodes_responded:
        with node.lock:
            node.is_election_in_progress = False
            node.last_heartbeat_time = time.time()
        return

    with node.lock:
        node.current_leader = node.node_id
        node.synced = False
        print(node.current_leader)
        node.is_election_in_progress = False
        print(f"[{node.node_id}] é o novo líder.")
        
    for peer in node.peers:
        try:
            channel = grpc.insecure_channel(peer)
            stub = voting_pb2_grpc.VotingServiceStub(channel)
            res = stub.DeclareCoordinator(
                voting_pb2.CoordinatorMessage(new_leader_id=node.node_id),
                timeout=1.0
            )
        except Exception as e:
            print(str(e))
            pass

    sync_leader(node)
