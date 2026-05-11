## Projeto de Mensageria com ZeroMQ e Protobuf

Este repositório implementa o projeto de mensageria distribuída usando ZeroMQ, Protocol Buffers e quatro linguagens de programação: Python, Go, Java e C#. A versão atual cobre as partes 1 a 5 do trabalho: requisições request/reply, publicações pub/sub, relógio lógico em todas as mensagens, eleição de coordenador para sincronização física e replicação dos dados entre os servidores.

### Arquitetura

- `broker.py`: broker ZeroMQ com `ROUTER` em `5555` para clientes e `DEALER` em `5556` para servidores. Ele apenas encaminha request/reply e não foi alterado na parte 3.
- `proxy.py`: proxy `XSUB/XPUB` para pub/sub em `5557/5558`, também mantido sem alterações na parte 3.
- `referencia.py`: novo serviço de referência da parte 3, acessado pelos servidores em `tcp://referencia:5559`. Ele atribui ranks estáveis, mantém a lista de servidores ativos, remove entradas expiradas por heartbeat e devolve a hora de referência em todos os replies.
- `servidores/python`, `servidores/go`, `servidores/java`, `servidores/csharp`: processam login, criação/listagem de canais e publicação, persistem dados em disco e sincronizam seus relógios físicos via offset local.
- os servidores também expõem portas internas para eleição, sincronização de relógio, encaminhamento de escrita ao coordenador e aplicação de réplica, com retry interno quando o coordenador muda durante a escrita.
- `clientes/python`, `clientes/go`, `clientes/java`, `clientes/csharp`: bots que fazem login, criam canais quando necessário, se inscrevem em até 3 tópicos e publicam mensagens continuamente.

### Contrato e relógios

- Todas as mensagens trafegadas usam o `Envelope` definido em [`contratos/contrato.proto`](contratos/contrato.proto).
- O `Cabecalho` de cada mensagem agora inclui:
  - `id_transacao`
  - `linguagem_origem`
  - `timestamp_envio`
  - `relogio_logico`
- O relógio lógico segue exatamente a regra do enunciado:
  - incrementa antes de cada envio
  - ao receber uma mensagem, atualiza para `max(local, recebido)`
- As publicações pub/sub (`ChannelMessage`) também carregam `timestamp_envio` e `relogio_logico`.
- Apenas os servidores ajustam relógio físico. Cada servidor calcula um offset local com base na hora retornada pela referência e no ponto médio entre envio e recebimento da requisição ao serviço de referência.

### Serviço de referência

O serviço de referência implementa três operações protobuf:

- `RegisterServerRequest/Response`: registra o nome do servidor e retorna um rank estável.
- `ListServersRequest/Response`: devolve a lista de servidores ativos com nome e rank.
- `HeartbeatRequest/Response`: renova a presença do servidor e serve como ponto de sincronização do relógio físico.

Regras aplicadas:

- ranks são sequenciais e estáveis por `SERVER_NAME`
- nomes não são duplicados
- a lista ativa expira entradas sem heartbeat por 30 segundos
- cada servidor envia heartbeat a cada 10 mensagens de clientes processadas
- se o heartbeat for rejeitado pela referência, o servidor se registra novamente e refaz a leitura da lista ativa

### Persistência

Cada servidor usa um volume próprio em `./dados/<nome_do_servidor>`:

- `logins.jsonl`: logins recebidos
- `canais.json`: canais conhecidos naquele servidor
- `publicacoes.jsonl`: publicações realizadas pelo servidor

O formato de persistência é JSON/JSONL. A restrição de mensagens binárias vale apenas para o tráfego entre processos.
Para validações limpas, o `docker-compose.yml` aceita `DATA_ROOT`, permitindo apontar os volumes para outro diretório sem alterar o código.

### Execução

Pré-requisitos:

- Docker
- Docker Compose

Para subir todo o ambiente:

```bash
docker compose up --build
```

O `docker-compose.yml` sobe:

- `orquestrador`
- `proxy`
- `referencia`
- `servidor_python`, `servidor_go`, `servidor_java`, `servidor_csharp`
- `cliente_python`, `cliente_go`, `cliente_java`, `cliente_csharp`

### Geração de Protobuf

- Python: `shared/protos/contrato_pb2.py` é gerado no `Dockerfile.python`.
- Go: `shared/go/protos/contrato.pb.go` fica versionado no repositório.
- Java: o `Dockerfile.java` executa `protoc` durante o build.
- C#: `Grpc.Tools` gera as classes a partir do `.proto` durante o build do projeto.

### Observabilidade

Os processos registram em log:

- tipo da mensagem enviada/recebida
- `timestamp_envio`
- `relogio_logico`
- publicações recebidas por canal
- atualização de ranks, lista de servidores e heartbeats

Isso facilita validar a sincronização lógica e física exigida pela parte 3.

### Replicação e consistência

Para a parte 5 foi escolhida a forma mais simples de replicação dentre as vistas em aula: **replicação passiva primário-seguidor**, com um único servidor líder serializando as escritas.

No projeto, o líder é o **coordenador já eleito na parte 4**. Isso evita introduzir um novo mecanismo de descoberta de líder e reaproveita a infraestrutura que já existia para eleição e comunicação entre servidores.

Funcionamento:

- operações de escrita (`login`, `create_channel` e `publish`) continuam chegando ao servidor escolhido pelo broker em round-robin;
- se esse servidor **não** for o coordenador, ele encaminha a requisição original ao coordenador por uma porta interna (`5562`);
- o coordenador processa a escrita e a replica para todos os servidores ativos por outra porta interna (`5563`);
- as réplicas aplicam a mesma mutação localmente;
- em `publish`, apenas o coordenador envia a mensagem ao proxy PUB/SUB; os seguidores apenas persistem o evento em disco.

Essa escolha resolve o problema do projeto porque:

- todos os servidores ativos passam a armazenar os mesmos `logins`, `canais` e `publicacoes`;
- uma leitura de canais pode continuar local, porque toda criação de canal só é considerada concluída depois que a réplica é aplicada nos demais servidores;
- a perda de um servidor deixa de significar perda imediata do histórico já replicado nos outros nós.

Adaptações feitas para caber no projeto:

- a replicação usa os mesmos `Envelope` e requests protobuf já existentes, sem criar um segundo contrato;
- o coordenador usa o nome dos containers já conhecido pela parte 4 para falar diretamente com os outros servidores na rede do Docker Compose;
- em réplicas de `create_channel`, a resposta “canal já existe” é tratada como sucesso interno para tolerar reaplicação da mesma mutação.

Limitações assumidas nesta versão:

- a implementação replica apenas para os **servidores ativos no momento da escrita**;
- se uma réplica estiver fora do ar, a escrita pode falhar até a lista ativa ser atualizada pela referência e pela eleição;
- quando um servidor volta depois de ficar offline, ele **não** recebe automaticamente o histórico perdido durante sua ausência.

### Validação da parte 5

Para validar a parte 5, use o script [`scripts/validar_parte5.py`](scripts/validar_parte5.py), que sobe a stack em um `DATA_ROOT` temporário, executa o fluxo nominal e depois derruba um servidor para checar convergência dos remanescentes.

O cenário de failover agora segue este comportamento:

- o servidor que perde o coordenador refaz a consulta de lista ativa e tenta a escrita novamente uma vez;
- o coordenador replica para os ativos do snapshot atual e repete a tentativa apenas após refresh da lista;
- o Java não encerra mais o serviço interno por exceção isolada de escrita ou réplica.
