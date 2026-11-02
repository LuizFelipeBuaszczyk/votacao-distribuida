
from classes.node import Node

def run_bully_election(node: Node):
    import voting_pb2
    import voting_pb2_grpc
    import grpc
    
    for peer in node.peers:
        peer_name, peer_port = peer.split(":")
        peer_id = int(peer.replace("node", ""))

        if peer_id > node.node_id:
            try:
                channel = grpc.insecure_channel(peer)
                stub = voting_pb2_grpc.VotingServiceStub(channel)
                res = stub.StartElection(
                    voting_pb2.ElectionMessage(sender_id=node.node_id),
                    timeout=1.0
                )
                if res.success:
                    higher_nodes_responded = True
            except Exception:
                pass

    if higher_nodes_responded:
        return

    with node.lock:
        node.current_leader = node.node_id
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
        except Exception:
            pass

