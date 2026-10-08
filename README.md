# Jungle Gaming — Distributed Wagering Processor

Processador financeiro com Bun, NestJS/Fastify, MikroORM e PostgreSQL. HTTP e SQS usam o mesmo caso de uso; o banco preserva saldo, idempotência, ledger e outbox em uma única transação.

Implementação do [desafio oficial](https://github.com/junglegaming/backend-challenge), consultado no commit `c7143e6d041585d6913f56eb7415105e074f83dc`. A [matriz de requisitos](docs/REQUIREMENTS.md) aponta implementação e provas. As decisões, o modelo de falhas e as limitações estão em [ARCHITECTURE.md](ARCHITECTURE.md).

## Executar em cinco minutos

Requisitos: **Bun 1.4.2**, Docker com Compose v2+ e portas locais `55432`/`4566` livres. Todas as dependências têm versões exatas e lockfile; as imagens externas estão fixadas por digest. O serviço também pode executar inteiramente em containers.

```bash
bun install --frozen-lockfile
cp .env.example .env.local
chmod 600 .env.local
docker compose --profile portable up -d --wait postgres ministack
bun run db:migrate
bun run queues:init
bun run demo
```

O demo inicia **três processos reais**, abre uma wallet, aposta, faz replay em outra instância, recebe uma BET por SQS após uma REFUND pendente e reconcilia o saldo. Ele encerra seus processos ao terminar. Os dados financeiros de demonstração permanecem para auditoria; execuções seguintes usam novas identidades.

Para deixar a API e os workers ativos:

```bash
bun run build
bun run start
```

API: `http://localhost:3000`; OpenAPI: [http://localhost:3000/docs](http://localhost:3000/docs); JSON: `/docs-json`; métricas: `/metrics`.

**Autenticação:** `AUTH_MODE=development` é explícito e não autentica usuários. Um guard global chama `IdentityPort` em todas as rotas de negócio e em `/metrics`; os health checks permanecem públicos. A documentação Swagger também fica aberta no perfil local. `IdentityPort` é o ponto de integração com um IdP OIDC. Outro modo exige um adaptador e impede o startup enquanto ele não existir. Este perfil local atende à extensão de identidade permitida pelo enunciado; não representa uma API pronta para exposição pública em produção.

## LocalStack: ambiente principal

Crie um Developer Auth Token no [portal LocalStack](https://app.localstack.cloud/) e configure somente o valor de `LOCALSTACK_AUTH_TOKEN` em `.env.local`. Consulte a [documentação de autenticação](https://docs.localstack.cloud/aws/getting-started/auth-token/). O arquivo é ignorado pelo Git e pelo contexto Docker. Não coloque o token em comandos versionados ou em logs.

```bash
docker compose --profile portable stop ministack
docker compose --env-file .env.local --profile reference up -d --wait postgres localstack
bun run queues:init
TEST_BROKER=localstack bun run verify
BROKER_LABEL=LocalStack bun run test:load
```

O Compose usa snapshots no encerramento e restauração no startup para evitar interferência entre snapshots periódicos e long polling. Encerrar normalmente preserva o estado; matar o **emulador** com SIGKILL pode perder mensagens ainda não salvas. Os testes de crash financeiro matam os processos da aplicação. Veja as limitações de durabilidade do emulador em [ARCHITECTURE.md](ARCHITECTURE.md).

Para voltar ao perfil portátil:

```bash
docker compose --profile reference stop localstack
docker compose --profile portable up -d --wait postgres ministack
bun run queues:init
TEST_BROKER=ministack bun run verify
```

As mesmas suítes são usadas em ambos. Os brokers compartilham a porta `4566`; execute um perfil de cada vez. Seus volumes de estado são independentes.

## Aplicação inteira em Docker e três instâncias

MiniStack:

```bash
docker compose --profile portable --profile application up -d --build --scale app=3
docker compose port --index 1 app 3000
```

LocalStack:

```bash
BROKER_HOST=localstack docker compose --env-file .env.local --profile reference --profile application up -d --build --scale app=3
docker compose port --index 1 app 3000
```

O job `setup` aplica migrations com o papel proprietário e cria as filas. A aplicação executa como usuário não privilegiado do container e como `jungle_app` no PostgreSQL. Cada instância recebe uma porta HTTP diferente, exibida pelo comando `port`. Use essa porta em `API_URL=http://127.0.0.1:PORTA bun run demo` para direcionar o demo ao serviço existente.

## Testes e verificação

```bash
bun run typecheck
bun run test
bun run test:integration
TEST_BROKER=localstack bun run verify
bun run test:load
```

A integração cria um **banco isolado**, aplica **up/down completo/up**, cria filas com prefixo exclusivo, inicia pelo menos três processos Bun e verifica diretamente os registros SQL. Limpa seus bancos, filas e processos ao terminar. Requer o PostgreSQL e o broker em execução; não usa mocks dessas dependências.

`TEST_BROKER=localstack` ou `ministack` habilita também os testes que **param e reiniciam os containers deste Compose**, verificando indisponibilidade real, persistência SQS, readiness e recuperação. Execute esses testes sem outros clientes usando este ambiente de desenvolvimento. Sem essa variável, esses dois cenários são indicados como skipped e os demais continuam executando.

Provas incluem 50 submissões simultâneas, apostas de 80 contra saldo de 100, wallets distintas, HTTP/SQS, reversões concorrentes, referências fora de ordem, snapshots de replay, constraints, corrupção administrativa isolada, morte antes do commit/depois do commit/antes do ack/depois do envio, leases expiradas, publisher antigo, dois publishers, erros permanentes, retry, DLQ e shutdown.

O teste de carga inicia três processos por padrão, compara uma wallet com 24 wallets, aquece cada topologia, faz três repetições de 600 requisições com concorrência 24 e inclui 10% de rejeições deliberadas. Reporta ambiente, throughput, p50/p95/p99, erros técnicos, rejeições esperadas, conflitos, espera de lock e atraso da outbox. Confere reconciliação e drenagem da outbox ao final. O resultado é gravado em `artifacts/load.json`; uma execução registrada está em [docs/results](docs/results/README.md). Não estabelece capacidade de produção.

Configuração opcional: `LOAD_REQUESTS`, `LOAD_CONCURRENCY`, `LOAD_REPETITIONS`, `LOAD_DRAIN_SECONDS` (180), `LOAD_REPORT`, `API_URLS` (URLs separadas por vírgula), `BROKER_LABEL`.

## Contrato HTTP

Dinheiro sempre é `{ "amount": "25.00", "currency": "BRL" }`. Valores negativos, casas excedentes, notação científica e valores fora de `NUMERIC(38,2)` são recusados; não existe arredondamento financeiro silencioso. Zeros à esquerda são normalizados. Moedas diferentes são suportadas pelo domínio, mas a API pública deste desafio aceita BRL.

```bash
curl -sS http://localhost:3000/wallets \
  -H 'Content-Type: application/json' \
  -d '{"playerId":"72b2b8c2-c64d-493c-84b5-08859a69bd93","initialBalance":{"amount":"100.00","currency":"BRL"}}'
```

Use o `id` retornado como `walletId`:

```bash
curl -sS http://localhost:3000/wagering/transactions \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: provider-a:bet-001' \
  -d '{"providerId":"provider-a","externalTransactionId":"bet-001","playerId":"72b2b8c2-c64d-493c-84b5-08859a69bd93","walletId":"WALLET_UUID","roundId":"round-001","gameId":"fortune-chimp","kind":"BET","money":{"amount":"25.00","currency":"BRL"}}'
```

| Rota                                                                      | Resultado                                                                            |
| ------------------------------------------------------------------------- | ------------------------------------------------------------------------------------ |
| `POST /wallets`                                                           | Abertura com versão 1; positiva cria crédito OPENING interno, zerada não cria ledger |
| `GET /wallets/:walletId`                                                  | Saldo atual e versão                                                                 |
| `GET /wallets/:walletId/ledger?limit=50&cursor=...`                       | Ledger decrescente, cursor com wallet/última versão/limite superior; máximo 100      |
| `POST /wagering/transactions`                                             | Decisão persistida, saldo observado, versão e `idempotentReplay`                     |
| `GET /wagering/transactions/:transactionId`                               | Entrada e resultado persistidos                                                      |
| `GET /providers/:providerId/wagering/transactions/:externalTransactionId` | Consulta pela identidade externa                                                     |
| `POST /wallets/:walletId/reconciliation`                                  | Saldo armazenado, reconstruído, diferença assinada e consistência                    |
| `GET /health/live`, `GET /health/ready`                                   | Processo vivo; dependências PostgreSQL/SQS disponíveis                               |
| `GET /metrics`                                                            | Prometheus, sem identificadores individuais como labels                              |

Estados terminais: `PROCESSED`, `REJECTED`, `FAILED`. Uma referência ausente produz `PENDING_REFERENCE` e HTTP 202; consulte até a finalização. O worker usa TTL de 24 h. A primeira resposta pendente mantém seu snapshot em replays enquanto o estado não mudar. Respostas terminais preservam o saldo e a versão originais mesmo após novas movimentações.

| HTTP            | Significado                                                                                      |
| --------------- | ------------------------------------------------------------------------------------------------ |
| 201 / 200 / 202 | Wallet criada / operação processada / referência pendente                                        |
| 400 / 404 / 409 | Contrato inválido / consulta inexistente / conflito de identidade ou chave                       |
| 422             | Rejeição de negócio persistida com `failureCode`                                                 |
| 500             | FAILED permanente registrado ou erro interno identificado como `INTERNAL_ERROR`                  |
| 503             | Dependência indisponível ou resultado inconclusivo; **reenviar a mesma chave e o mesmo payload** |

503 não prova que o commit deixou de acontecer. Reusar a chave permite recuperar a decisão persistida. `X-Correlation-Id` válido é propagado; na ausência dele, a aplicação gera um UUID.

## SQS, outbox e operação

Filas `wager-transactions.fifo`, `wager-transactions-dlq.fifo` e `wager-events.fifo`. `MessageGroupId=walletId`; identificadores de deduplicação de transporte não substituem inbox/idempotência SQL.

```json
{
  "messageId": "msg-123",
  "type": "WagerTransactionRequested",
  "occurredAt": "2026-10-07T12:00:00.000Z",
  "data": {
    "providerId": "provider-a",
    "externalTransactionId": "bet-001",
    "idempotencyKey": "provider-a:bet-001",
    "playerId": "72b2b8c2-c64d-493c-84b5-08859a69bd93",
    "walletId": "WALLET_UUID",
    "roundId": "round-001",
    "gameId": "fortune-chimp",
    "kind": "BET",
    "money": { "amount": "25.00", "currency": "BRL" }
  }
}
```

Eventos têm `eventId`, `eventType`, `aggregateId`, `correlationId`, `causationId` quando disponível, `occurredAt`, `version` e `data`. A publicação é **at least once**; o consumidor de exemplo (`APP_ROLES=events`) deduplica persistentemente por eventId. As versões da wallet ajudam consumidores que precisem reconstruir estado; não há garantia de ordem financeira global dos eventos publicados por instâncias diferentes.

`APP_ROLES` seleciona `api`, `consumer`, `references`, `publisher`, `dlq`, `events`, combinados por vírgula. Exemplo: `APP_ROLES=api,publisher PORT=3001 bun run start`. Adicione `api` às instâncias das quais deseja expor health/metrics; processos com apenas workers não abrem porta HTTP.

Defaults: long polling 20 s, visibilidade 30 s, heartbeat 10 s, redrive após 5 recebimentos; referência polling 1 s/backoff 1–60 s/TTL 24 h; outbox polling ocioso 500 ms/lote 20/concorrência 5/lease 60 s/deadline SQS 5 s/backoff até 5 min; shutdown 25 s. `SIGTERM` interrompe novas recepções, drena e devolve visibilidade restante. Fault injection e barreiras IPC exigem `NODE_ENV=test` e não têm endpoint público.

Logs JSON contêm contexto de correlação e decisões, sem payload financeiro completo. Rejeições de negócio recebem ack; transitórios recebem retry; permanentes vão à DLQ. O auditor persiste a mensagem antes de removê-la. Exaustão de transporte não converte automaticamente indisponibilidade em FAILED.

## CI e entrega

GitHub Actions executa `bun audit` e verificação com MiniStack e LocalStack, incluindo reinício real das dependências. O job LocalStack usa o secret `LOCALSTACK_AUTH_TOKEN`; tokens nunca entram em código. Pull requests externos executam somente a validação portátil, sem acesso ao secret.

Envie **o link deste repositório por e-mail**, conforme a mensagem do recrutamento. A submissão deve incluir o commit entregue, instruções de execução e acesso dos avaliadores; não há formulário ou comando de upload prescrito no README oficial. Um texto de entrega e roteiro de apresentação estão em [docs/DELIVERY.md](docs/DELIVERY.md).

Para parar o ambiente preservando dados: `docker compose --profile portable --profile reference --profile application down`. Remover volumes apaga os dados de desenvolvimento e deve ser uma ação administrativa deliberada. Migrations de rollback são disponibilizadas para ambientes descartáveis; `bun run db:rollback` reverte a última migration.
