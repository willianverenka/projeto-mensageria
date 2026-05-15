# Projeto Mensageria

Sistema distribuído de troca de mensagens com clientes/bots e servidores em
Python, Go, Java e C#. A comunicação entre clientes e servidores usa ZeroMQ
com mensagens binárias em Protocol Buffers. As publicações em canais usam o
proxy PUB/SUB e as requisições diretas usam o broker DEALER/ROUTER.

## Parte 5: consistência e replicação

Para a última parte do projeto foi escolhida a estratégia de **réplica ativa**
com **consistência eventual**, conforme as alternativas vistas na aula de
consistência e replicação. Nesta abordagem, cada servidor continua aceitando
requisições dos clientes, aplica localmente as operações de escrita e propaga a
operação para as demais réplicas.

O projeto usa dois mecanismos complementares:

- **Push das escritas novas:** operações de `login`, `create_channel` e
  `publish` aceitas por qualquer servidor são publicadas no tópico interno
  `__replica__` do proxy PUB/SUB. Todos os servidores assinam esse tópico e
  aplicam a operação recebida em seu armazenamento local.
- **Pull de recuperação:** cada servidor expõe um serviço interno na porta
  `5562`. Ao iniciar e durante a manutenção periódica, um servidor pede um
  snapshot ao coordenador atual ou a outro servidor ativo. O snapshot é enviado
  como uma resposta multipart com as operações necessárias para reconstruir o
  estado.

As mensagens de replicação reutilizam o `Envelope` definido em
`contratos/contrato.proto`, portanto o contrato público dos clientes não foi
alterado. O tópico `__replica__` é interno aos servidores.

### Aplicação das réplicas

Os servidores armazenam três conjuntos de dados em disco:

- `logins.jsonl`, com usuário e timestamp;
- `canais.json`, com os nomes dos canais;
- `publicacoes.jsonl`, com canal, mensagem, remetente e timestamp.

As operações replicadas são aplicadas de forma idempotente. Canais são tratados
como conjunto, e logins/publicações são identificados pela combinação de seus
campos persistidos. Assim, receber a mesma operação pelo push e novamente por
snapshot não duplica o dado.

Quando uma publicação replicada chega antes da criação do canal correspondente,
o servidor cria o canal localmente antes de registrar a publicação. A publicação
replicada é persistida, mas não é republicada no canal dos clientes; isso evita
que os bots recebam a mesma mensagem várias vezes.

### Garantia de consistência

A implementação não busca linearização nem sincronização global. A garantia é
de **consistência eventual**: se novas escritas pararem por algum tempo e os
servidores estiverem disponíveis, as operações propagadas por push e as
sincronizações por snapshot fazem todos convergirem para o mesmo histórico.

Essa escolha combina com o projeto porque o broker faz balanceamento entre
servidores e cada servidor possui seu próprio disco. A réplica ativa permite que
qualquer servidor continue atendendo clientes, enquanto o push reduz o tempo de
propagação e o snapshot cobre falhas temporárias ou servidores que entram depois.

### Modo de apresentação

O `docker-compose.yml` usa `SERVER_LOG_MODE=presentation` e
`CLIENT_LOG_MODE=presentation` por padrão. Nesse modo, o broker, a referência e
os clientes reduzem os logs de tráfego contínuo para que a demonstração destaque
os eventos de eleição dos servidores. Para depuração detalhada de mensagens, use
`SERVER_LOG_MODE=verbose CLIENT_LOG_MODE=verbose docker compose up --build`.
