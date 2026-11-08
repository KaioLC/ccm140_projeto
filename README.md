# Projeto Sistemas Distribuídos (CC7261) - Parte 1

Sistema distribuído para troca de mensagens instantâneas utilizando Docker, ZeroMQ (padrão Request-Reply via Broker), serialização binária com Protobuf e persistência local em SQLite.

## Integrantes e Linguagens
- **Kaio**: C++
- **Rafa**: Python
- **Daniel**: *(adicionar linguagem)*

## Mensagens Trocadas (`chat.proto`)

Todas as mensagens são enviadas dentro dos envelopes `Request` e `Response`, que carregam obrigatoriamente o `id` da mensagem e o `timestamp` do envio:

1. **Login (`LoginRequest` -> `LoginResponse`)**: O bot envia seu `user` e `id` para iniciar sessão. O servidor registra o histórico do login com seu `timestamp` no banco SQLite (`logins`) e retorna `status` e `message`. Se falhar, o bot gera um novo `id` e repete a tentativa (com limite máximo de 5 tentativas).
2. **Listagem de Canais (`ChannelListRequest` -> `ChannelListResponse`)**: O bot pede os canais disponíveis e o servidor retorna a lista de nomes (`channels`) cadastrados no SQLite.
3. **Criação de Canal (`ChannelRequest` -> `ChannelResponse`)**: Se existirem menos de 5 canais, o bot envia o `name` de um novo canal. O servidor grava na tabela `channels` do SQLite e responde se foi criado com sucesso.

### Fluxo das Mensagens

| Etapa | Origem -> Destino | Protocolo / Porta | Mensagem Protobuf | Dados Enviados | Ação no Servidor (SQLite) |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **1. Login (Req)** | Cliente -> Broker -> Servidor | ZeroMQ `REQ/REP` (`5555`/`5556`) | `LoginRequest` | `id`, `user`, `timestamp` | `INSERT` na tabela `logins` |
| **1. Login (Rep)** | Servidor -> Broker -> Cliente | ZeroMQ `REP/REQ` (`5556`/`5555`) | `LoginResponse` | `id`, `status`, `message`, `timestamp` | — |
| **2. Listar (Req)** | Cliente -> Broker -> Servidor | ZeroMQ `REQ/REP` (`5555`/`5556`) | `ChannelListRequest` | `id`, `timestamp` | `SELECT name` na tabela `channels` |
| **2. Listar (Rep)** | Servidor -> Broker -> Cliente | ZeroMQ `REP/REQ` (`5556`/`5555`) | `ChannelListResponse` | `id`, `channels[]`, `timestamp` | — |
| **3. Criar Canal (Req)** | Cliente -> Broker -> Servidor | ZeroMQ `REQ/REP` (`5555`/`5556`) | `ChannelRequest` | `id`, `name`, `timestamp` | `INSERT` na tabela `channels` |
| **3. Criar Canal (Rep)** | Servidor -> Broker -> Cliente | ZeroMQ `REP/REQ` (`5556`/`5555`) | `ChannelResponse` | `id`, `name`, `status`, `message`, `timestamp` | — |
