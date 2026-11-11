# Implementação Java

A implementação Java está em `java_server_client/`. Ela reutiliza o broker,
proxy e nó de referência já existentes no repositório, e usa o contrato
compartilhado em `contracts/chat.proto`. Não há cópias Java ou Python dos
serviços de infraestrutura nesta entrega.

## Executar

Para executar também os serviços Java definidos no `docker-compose.yml`:

```powershell
docker compose up --build --scale java_server=2 --scale java_client=2
```

Os serviços `java_server` e `java_client` usam a infraestrutura compartilhada:

- clientes: broker `tcp://broker:5555`, proxy `tcp://proxy:5558`;
- servidores: broker `tcp://broker:5556`, proxy `tcp://proxy:5557`,
  referência `tcp://reference:5559`;
- comunicação direta entre servidores: porta `6000`, igual à usada pelos
  servidores C++.

Os SQLite ficam em `data/java/server_<id>.db`, um arquivo por réplica. A pasta
`data/` é ignorada pelo Git e não deve ser commitada.

## Protocolo e compatibilidade

O Maven gera as classes Java a partir de `contracts/chat.proto`. O contrato
mantém os campos comuns ao C++ e acrescenta os campos de sincronização Berkeley
e snapshot de replicação usados pelos servidores Java. Campos Protobuf
desconhecidos são ignorados por leitores antigos.

As publicações de replicação Java usam o tópico `replication`, compartilhado
com o servidor C++. Os eventos binários `ReplicationEvent` usam os mesmos
campos existentes no contrato. O broker apenas encaminha quadros, sem
interpretar o conteúdo.

## Comportamento

- `ClienteBot` faz login, lista/cria canais, inscreve-se via SUB e publica
  mensagens sem interação humana; REQ e SUB rodam em threads separadas.
- `Servidor` persiste logins, canais e mensagens no SQLite local, envia
  heartbeats e participa das eleições/Berkeley.
- A replicação PUB/SUB é complementada por snapshots periódicos entre servidores
  Java para recuperar eventos perdidos. A convergência é eventual; PUB/SUB não
  garante entrega ou consistência linearizável durante partições.
- Mensagens incluem timestamp e relógio de Lamport. Os logs exibem o Protobuf
  em formato legível, embora o tráfego continue binário.

Para compilar localmente, a partir de `java_server_client/`:

```powershell
mvn -B -DskipTests package
```
