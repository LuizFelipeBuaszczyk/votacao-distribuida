# Votação distribuída

Sistema de contagem de votos com 3 nós em Docker. Usa gRPC na comunicação, Bully na eleição do líder, heartbeat na detecção de falhas, replicação por maioria e relógio de Lamport.

## Como subir

Requisito: Docker com Docker Compose.

```
docker compose up --build
```

Abra os painéis no navegador:

- Cliente A: http://localhost:8081
- Cliente B: http://localhost:8082

Para parar:

```
docker compose down
```

Os votos ficam só em memória: parar os três nós apaga a contagem.

## Como testar uma queda

Com os painéis abertos, derrube o líder:

```
docker compose kill node3
```

Em poucos segundos, o nó 2 assume, sem perder votos. Para religar:

```
docker compose start node3
```

O nó 3 recupera os votos e retoma a liderança. Com dois nós fora, os votos são recusados até um deles voltar.

## Client pelo terminal

```
docker compose exec client-a python client.py vote <eleitor> <candidato>
docker compose exec client-a python client.py results
```

## Protocolo

O contrato gRPC está em `src/protocols/voting.proto`.
