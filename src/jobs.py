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
        if time.time() - node.last_heartbeat_time > 3 and node.current_leader is not None:
            with node.lock:
                if node.is_election_in_progress:
                    continue
                node.is_election_in_progress = True

            print(f"[{node.node_id}] Timeout do líder ({node.current_leader})! Iniciando nova eleição.")
            run_bully_election(node)


def heartbeat(node: Node):
    import voting_pb2_grpc
    import voting_pb2
    import grpc
    print("Iniciando tarefa de heartbeat")

    while True:
        if node.current_leader != node.node_id:
            continue
       
        with node.lock:
            node.lamport_clock += 1
            current_clock = node.lamport_clock

            for peer in node.peers:
                try:
                    print(f"Mandando heartbeat para {peer}")
                    channel = grpc.insecure_channel(peer)
                    stub = voting_pb2_grpc.VotingServiceStub(channel)
                    stub.Heartbeat(
                        voting_pb2.HeartbeatRequest(
                            leader_id=node.node_id,
                            lamport_timestamp=current_clock
                        ),
                        timeout=1
                    )
                except Exception as e:
                    print(str(e))
                    pass


