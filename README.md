# Projeto Sistemas Distribuídos

Sistema distribuído para troca de mensagens instantâneas utilizando Docker, ZeroMQ (padrão Request-Reply via Broker e Pub/Sub via Proxy), serialização binária com Protobuf e persistência local em SQLite.

## Integrantes e Linguagens
- **Kaio**: C++
- **Rafa**: Python
- **Daniel**: Java

## Mensagens Trocadas (`chat.proto`)

Todas as mensagens enviadas contêm obrigatoriamente o identificador único (`id`), o `timestamp` de envio e o relógio lógico de Lamport (`clock`):

1. **Login (`LoginRequest` -> `LoginResponse`)**: O bot envia seu `user` e `id` para iniciar sessão. O servidor registra o histórico do login com seu `timestamp` no banco SQLite (`logins`) e retorna `status` e `message`. Se falhar, o bot gera um novo `id` e repete a tentativa (com limite máximo de 5 tentativas).
2. **Listagem de Canais (`ChannelListRequest` -> `ChannelListResponse`)**: O bot pede os canais disponíveis e o servidor retorna a lista de nomes (`channels`) cadastrados no SQLite.
3. **Criação de Canal (`ChannelRequest` -> `ChannelResponse`)**: Se existirem menos de 5 canais, o bot envia o `name` de um novo canal. O servidor grava na tabela `channels` do SQLite e responde se foi criado com sucesso.
4. **Publicação em Canais (`PublishRequest` -> `Publication` -> `PublishResponse`)**: O bot se inscreve em canais via `proxy` (`SUB` na porta `5558`) e envia mensagens ao servidor (`PublishRequest`). O servidor grava na tabela `messages` do SQLite, publica a mensagem (`Publication`) no tópico do canal (`PUB` na porta `5557`) e retorna o status (`PublishResponse`).
5. **Registro de Servidor e Heartbeat (`ReferenceRequest` -> `ReferenceResponse`)**: O servidor se registra no serviço de referência (`tcp://reference:5559`) informando seu endpoint (`tcp://<hostname>:6000`), obtém seu `rank` e consulta a lista de servidores ativos. A cada 10 mensagens de clientes processadas, o servidor envia um *heartbeat* à referência para manter-se ativo na lista de servidores disponíveis.
6. **Sincronização de Relógio com Coordenador (`ClockSyncRequest` -> `ClockSyncResponse`)**: A cada 15 mensagens de clientes, os servidores sincronizam seu relógio físico diretamente com o servidor eleito como coordenador (Algoritmo de Berkeley).
7. **Eleição de Coordenador (`ElectionRequest` -> `ElectionResponse` -> `CoordinatorAnnouncement`)**: Se o coordenador não responder à sincronização de relógio, é disparada a eleição por Bullying (Valentão) entre os servidores de rank superior. O vencedor anuncia sua liderança a todos os servidores publicando no tópico `servers`.

### Fluxo das Mensagens

| Etapa | Origem -> Destino | Protocolo / Porta | Mensagem Protobuf | Dados Enviados | Ação no Servidor (SQLite / Memória) |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **1. Login (Req)** | Cliente -> Broker -> Servidor | ZeroMQ `REQ/REP` (`5555`/`5556`) | `LoginRequest` | `id`, `user`, `timestamp`, `clock` | `INSERT` na tabela `logins` |
| **1. Login (Rep)** | Servidor -> Broker -> Cliente | ZeroMQ `REP/REQ` (`5556`/`5555`) | `LoginResponse` | `id`, `status`, `message`, `timestamp`, `clock` | — |
| **2. Listar (Req)** | Cliente -> Broker -> Servidor | ZeroMQ `REQ/REP` (`5555`/`5556`) | `ChannelListRequest` | `id`, `timestamp`, `clock` | `SELECT name` na tabela `channels` |
| **2. Listar (Rep)** | Servidor -> Broker -> Cliente | ZeroMQ `REP/REQ` (`5556`/`5555`) | `ChannelListResponse` | `id`, `channels[]`, `timestamp`, `clock` | — |
| **3. Criar Canal (Req)** | Cliente -> Broker -> Servidor | ZeroMQ `REQ/REP` (`5555`/`5556`) | `ChannelRequest` | `id`, `name`, `timestamp`, `clock` | `INSERT` na tabela `channels` |
| **3. Criar Canal (Rep)** | Servidor -> Broker -> Cliente | ZeroMQ `REP/REQ` (`5556`/`5555`) | `ChannelResponse` | `id`, `name`, `status`, `message`, `timestamp`, `clock` | — |
| **4. Publicar (Req)** | Cliente -> Broker -> Servidor | ZeroMQ `REQ/REP` (`5555`/`5556`) | `PublishRequest` | `id`, `user`, `channel`, `content`, `timestamp`, `clock` | `INSERT` na tabela `messages` |
| **4. Canal (Pub/Sub)** | Servidor -> Proxy -> Clientes | ZeroMQ `PUB/SUB` (`5557`/`5558`) | `Publication` | `id`, `user`, `channel`, `content`, `timestamp`, `clock` | — |
| **4. Publicar (Rep)** | Servidor -> Broker -> Cliente | ZeroMQ `REP/REQ` (`5556`/`5555`) | `PublishResponse` | `id`, `status`, `message`, `timestamp`, `clock` | — |
| **5. Registro (Req)** | Servidor -> Referência | ZeroMQ `REQ/REP` (`5559`) | `ReferenceRequest` (`reg`) | `id`, `name`, `endpoint`, `timestamp`, `clock` | Cadastra servidor e atribui `rank` |
| **5. Registro (Rep)** | Referência -> Servidor | ZeroMQ `REP/REQ` (`5559`) | `ReferenceResponse` (`reg`) | `id`, `name`, `rank`, `timestamp`, `clock` | Servidor armazena seu `rank` |
| **6. Listar Servidores (Req)** | Servidor -> Referência | ZeroMQ `REQ/REP` (`5559`) | `ReferenceRequest` (`list`) | `id`, `timestamp`, `clock` | Consulta servidores ativos |
| **6. Listar Servidores (Rep)** | Referência -> Servidor | ZeroMQ `REP/REQ` (`5559`) | `ReferenceResponse` (`list`) | `id`, `servers[]`, `timestamp`, `clock` | Servidor obtém lista de pares e endpoints |
| **7. Heartbeat (Req)** | Servidor -> Referência | ZeroMQ `REQ/REP` (`5559`) | `ReferenceRequest` (`heartbeat`) | `id`, `name`, `timestamp`, `clock` | Atualiza disponibilidade (a cada 10 msgs) |
| **7. Heartbeat (Rep)** | Referência -> Servidor | ZeroMQ `REP/REQ` (`5559`) | `ReferenceResponse` (`heartbeat`) | `id`, `status`, `timestamp`, `clock` | Confirma disponibilidade sem hora física |
| **8. Relógio Berkeley (Req)** | Servidor -> Coordenador | ZeroMQ `REQ/REP` (`6000`) | `ServerMessage` (`clock_sync`) | `id`, `server`, `timestamp`, `clock` | Pede hora ao coordenador (a cada 15 msgs) |
| **8. Relógio Berkeley (Rep)** | Coordenador -> Servidor | ZeroMQ `REP/REQ` (`6000`) | `ServerMessage` (`clock_res`) | `id`, `server`, `physical_time`, `timestamp`, `clock` | Servidor ajusta relógio físico |
| **9. Eleição (Req)** | Servidor A -> Servidor B | ZeroMQ `REQ/REP` (`6000`) | `ServerMessage` (`election`) | `id`, `server`, `rank`, `timestamp`, `clock` | Envia eleição para servidores de rank superior |
| **9. Eleição (Rep)** | Servidor B -> Servidor A | ZeroMQ `REP/REQ` (`6000`) | `ServerMessage` (`election_res`)| `id`, `server`, `status`, `timestamp`, `clock` | Servidor superior responde OK |
| **10. Anúncio Coordenador** | Coordenador -> Proxy -> Servidores | ZeroMQ `PUB/SUB` (`5557`/`5558`) | `CoordinatorAnnouncement` | `coordinator`, `rank`, `timestamp`, `clock` | Atualiza coordenador ativo no tópico `servers` |
