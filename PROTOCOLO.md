# Protocolo de mensagens

Toda a comunicação do sistema (cliente ↔ nó e nó ↔ nó) usa **gRPC** com mensagens **Protocol Buffers (proto3)**. O contrato está em [`src/protocols/voting.proto`](src/protocols/voting.proto); este documento descreve cada mensagem e como ela é usada.

## Transporte

| Item | Valor |
| --- | --- |
| Serviço | `voting.VotingService` |
| Porta | `50051` em todos os nós |
| Endereçamento | Nome do serviço Docker (`node1:50051`), resolvido pelo DNS do Docker |
| Segurança | Canal sem TLS (`insecure_channel`) |
| Estilo | Todas as chamadas são unárias (uma requisição, uma resposta) |
| Timeouts | 1 s entre nós · 2,5 s para `CastVote` e 0,8 s para `GetResults` no cliente |

Um erro de transporte do gRPC (`UNAVAILABLE`, `DEADLINE_EXCEEDED`) é interpretado como **nó fora do ar**.

## Visão geral dos RPCs

| RPC | Origem → destino | Requisição | Resposta | Quando é usado |
| --- | --- | --- | --- | --- |
| `CastVote` | Cliente → qualquer nó | `VoteRequest` | `VoteResponse` | Eleitor registra um voto |
| `GetResults` | Cliente → cada nó | `ResultsRequest` | `ResultsResponse` | Painel/terminal consulta o estado |
| `StartElection` | Nó → nós de ID maior | `ElectionRequest` | `ElectionResponse` | Início do cluster ou queda do líder (Bully) |
| `DeclareCoordinator` | Novo líder → todos os pares | `CoordinatorMessage` | `CoordinatorResponse` | Fim da eleição |
| `Heartbeat` | Líder → seguidores | `HeartbeatRequest` | `HeartbeatResponse` | A cada 1 s |
| `ReplicateState` | Líder → seguidores | `ReplicationRequest` | `ReplicationResponse` | A cada voto aceito pelo líder |
| `SyncState` | Líder → seguidores | `SyncRequest` | `SyncResponse` | Novo líder ou contagem divergente |

## Mensagens

### Votação (cliente ↔ nó)

**`VoteRequest`**

| Campo | Tipo | Nº | Descrição |
| --- | --- | --- | --- |
| `voter_id` | int32 | 1 | Identificador do eleitor (> 0) |
| `candidate_id` | int32 | 2 | Candidato escolhido (1 a 3 no painel) |
| `lamport_timestamp` | int64 | 3 | Relógio de Lamport do cliente no envio |

**`VoteResponse`**

| Campo | Tipo | Nº | Descrição |
| --- | --- | --- | --- |
| `success` | bool | 1 | `true` só quando o voto foi confirmado pela maioria |
| `message` | string | 2 | Texto para o usuário |
| `retry` | bool | 3 | `true` = falha temporária, o cliente deve tentar de novo |
| `leader_id` | int32 | 4 | Líder conhecido pelo nó (`0` = desconhecido) |

Respostas possíveis:

| `success` | `retry` | `message` | Ação do cliente |
| --- | --- | --- | --- |
| true | false | `Voto confirmado` | Fim (sucesso) |
| false | false | `Eleitor já realizou o voto!` | Fim (voto duplicado) |
| false | true | `Este nó não é o líder` | Reenvia para `leader_id` |
| false | true | `Líder ainda não sincronizou com a maioria dos nós` | Tenta de novo após 0,5 s |
| false | true | `Voto deste eleitor em processamento` | Tenta de novo após 0,5 s |
| false | true | `Sem maioria dos nós, voto não confirmado` | Tenta de novo, por até 10 s |

**`ResultsRequest`** — vazia.

**`ResultsResponse`** — visão **local** do nó consultado.

| Campo | Tipo | Nº | Descrição |
| --- | --- | --- | --- |
| `node_id` | int32 | 1 | Nó que respondeu |
| `leader_id` | int32 | 2 | Líder conhecido (`0` = desconhecido) |
| `accepting_votes` | bool | 3 | `true` se é o líder e já sincronizou |
| `total_votes` | int64 | 4 | Quantidade de votos no nó |
| `results` | repeated `CandidateResult` | 5 | Contagem por candidato |
| `lamport_clock` | int64 | 6 | Relógio de Lamport do nó |

**`CandidateResult`**: `candidate_id` (int32, 1), `votes` (int64, 2).

### Eleição (Bully)

| Mensagem | Campos | Semântica |
| --- | --- | --- |
| `ElectionRequest` | `sender_id` (int32, 1) | ID de quem iniciou a eleição |
| `ElectionResponse` | `success` (bool, 1) | `true` se o receptor tem ID maior (o remetente desiste) |
| `CoordinatorMessage` | `new_leader_id` (int32, 1) | Anuncia o novo líder |
| `CoordinatorResponse` | `acknowledged` (bool, 1) | Receptor passou a reconhecer o líder |

### Heartbeat

| Mensagem | Campos | Semântica |
| --- | --- | --- |
| `HeartbeatRequest` | `leader_id` (int32, 1), `lamport_timestamp` (int64, 2) | Líder avisa que está vivo |
| `HeartbeatResponse` | `acknowledged` (bool, 1), `vote_count` (int64, 2) | `false` se o seguidor conhece um líder de ID maior; `vote_count` permite ao líder detectar divergência |

Um seguidor que fica **mais de 3 s** sem heartbeat válido inicia uma eleição.

### Replicação e sincronização

| Mensagem | Campos | Semântica |
| --- | --- | --- |
| `ReplicationRequest` | `voter_id` (int32, 1), `candidate_id` (int32, 2), `lamport_timestamp` (int64, 3) | Voto aceito pelo líder, com o carimbo de Lamport do líder |
| `ReplicationResponse` | `success` (bool, 1) | `false` se o seguidor já tem outro candidato para o mesmo eleitor |
| `VoteRecord` | `voter_id` (1), `candidate_id` (2), `lamport_timestamp` (3) | Um voto armazenado |
| `SyncRequest` | `sender_id` (int32, 1), `votes` (repeated `VoteRecord`, 2) | Todos os votos de quem chama |
| `SyncResponse` | `votes` (repeated `VoteRecord`, 1) | Todos os votos do receptor, já unidos aos recebidos |

Regra de junção: os dois lados ficam com a **união** dos votos. Se houver dois votos do mesmo eleitor, vale o de menor `(lamport_timestamp, candidate_id)`.

## Relógio de Lamport

- Evento local de envio (rodada de heartbeat, voto do cliente): `clock += 1`.
- Recebimento de mensagem com carimbo `t`: `clock = max(clock, t) + 1`.
- Campos que transportam o carimbo: `VoteRequest`, `ReplicationRequest`, `VoteRecord`, `HeartbeatRequest`; o valor atual é exposto em `ResultsResponse.lamport_clock`.

## Fluxos principais

### Voto entrando por um seguidor

```
Cliente        node1 (seguidor)      node3 (líder)        node2 (seguidor)
  | CastVote ------>|                     |                     |
  |<-- retry, leader_id=3                 |                     |
  | CastVote ------------------------------>|                     |
  |                 |<-- ReplicateState ----|-- ReplicateState -->|
  |                 |--- success ---------->|<-- success ---------|
  |                                (maioria: 2 de 3)            |
  |<-------------------- Voto confirmado ---|                     |
```

### Queda do líder

```
node3 (líder) cai
node1/node2: 3 s sem Heartbeat → monitor inicia eleição
node1 → StartElection → node2 : success=true  (node1 desiste)
node2 → StartElection → node3 : sem resposta (timeout 1 s)
node2 → DeclareCoordinator → node1, node3
node2 → SyncState → node1, node3 (precisa da maioria) → synced = true
node2 passa a aceitar votos e enviar Heartbeat
```

### Retorno do nó 3

```
node3 sobe com memória vazia → StartElection (ninguém com ID maior)
node3 → DeclareCoordinator → node1, node2
node3 → SyncState → node1, node2 → recebe todos os votos → synced = true
node3 volta a ser o líder
```
