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

    majority = (len(node.peers) + 1) // 2 + 1
    confirmed = 1  # o líder

    # Não espera um seguidor fora do ar quando a maioria já confirmou
    pool = ThreadPoolExecutor(max_workers=len(node.peers))
    try:
        for future in as_completed([pool.submit(send, peer) for peer in node.peers]):
            if future.result():
                confirmed += 1
            if confirmed >= majority:
                return True
        return False
    finally:
        pool.shutdown(wait=False)

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
        return

    with node.lock:
        node.current_leader = node.node_id
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

