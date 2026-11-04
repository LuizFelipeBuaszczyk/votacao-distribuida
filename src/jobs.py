import time

from node import Node 

def monitor(node: Node):
    """
    Monitora se o líder está enviando heartbeats
    """
    from utils import run_bully_election
    print("Iniciando tarefa de monitoramento")

    while True:
        time.sleep(1)
        if node.current_leader == node.node_id:
            continue
        
        # Verifica se o líder caiu -- 3 segundos sem heartbeats
        if time.time() - node.last_heartbeat_time > 3:
            with node.lock:
                if node.is_election_in_progress:
                    continue
                node.is_election_in_progress = True
                old_leader = node.current_leader
                node.current_leader = None

            print(f"[{node.node_id}] Timeout do líder ({old_leader})! Iniciando nova eleição.")
            run_bully_election(node)


def heartbeat(node: Node):
    from protocols import voting_pb2_grpc
    from protocols import voting_pb2
    import grpc
    from utils import exchange_votes, sync_leader
    print("Iniciando tarefa de heartbeat")

    while True:
        time.sleep(1)
        if node.current_leader != node.node_id:
            continue
       
        # Sem maioria na eleição, o líder segue tentando sincronizar
        if not node.synced:
            sync_leader(node)

        with node.lock:
            node.lamport_clock += 1
            current_clock = node.lamport_clock
            vote_count = len(node.votes)

        diverged = []
        for peer in node.peers:
            try:
                print(f"Mandando heartbeat para {peer}")
                channel = grpc.insecure_channel(peer)
                stub = voting_pb2_grpc.VotingServiceStub(channel)
                response = stub.Heartbeat(
                    voting_pb2.HeartbeatRequest(
                        leader_id=node.node_id,
                        lamport_timestamp=current_clock
                    ),
                    timeout=1
                )
                # Contagem diferente indica um nó que voltou ou perdeu uma réplica
                if response.acknowledged and response.vote_count != vote_count:
                    diverged.append(peer)
            except Exception as e:
                print(str(e))
                pass

        if diverged:
            exchange_votes(node, diverged)


