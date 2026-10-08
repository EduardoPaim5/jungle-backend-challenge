# Arquitetura, garantias e decisões

Este documento descreve a implementação do desafio e suas fronteiras de confiança. O [README](README.md) contém os comandos de execução; a [matriz de requisitos](docs/REQUIREMENTS.md) vincula os critérios às provas; os [resultados registrados](docs/results/README.md) preservam ambiente, metodologia e limitações das execuções. As extensões necessárias para produção estão identificadas abaixo e não são apresentadas como funcionalidades implementadas.

## Fluxo e fronteiras

```mermaid
flowchart LR
  HTTP[HTTP / NestJS + Fastify] --> UC[Wagering: caso de uso financeiro]
  SQS[SQS input] --> Consumer[Consumer + inbox]
  Consumer --> UC
  References[Worker de referências] --> UC
  UC --> Domain[Money / Wallet / WagerTransaction / Ledger / Eventos]
  UC --> PG[(PostgreSQL: wallet + transaction + ledger + inbox + outbox)]
  PG --> Publisher[Publisher com lease e fencing]
  Publisher --> Events[SQS integration events]
  Events --> Receipt[Consumidor exemplo: recibo por eventId]
  Consumer --> DLQ[DLQ]
  DLQ --> Auditor[Auditor persistente]
```

O domínio não importa Nest, MikroORM, PostgreSQL ou AWS. Classes controlam dinheiro, transições, referências e aritmética do ledger por factories, métodos explícitos e reidratação. O controller valida o transporte e passa comando e contexto separados. O mesmo `Wagering.process` é chamado por HTTP, consumidor e worker de referências. Queries não alteram o domínio.

O adaptador `Database` delimita EntityManagers, transações e locks. MikroORM hidrata a wallet e acompanha suas alterações. SQL explícito dentro do mesmo EntityManager implementa uniques, inbox/outbox, leasing e checks que exigem controle preciso de concorrência. Os casos de uso assumem PostgreSQL; não prometemos substituição transparente por outro banco. Essa escolha mantém as garantias visíveis e verificáveis, sem um repositório genérico que esconda locks ou constraints. `Queues` encapsula o SDK e adapta URLs retornadas por ambos os emuladores. `IdentityPort` reserva a fronteira de identidade.

O TypeScript compila os decorators e seus metadados; Bun executa o JavaScript resultante. O smoke de Nest, Fastify, Swagger e MikroORM é exercitado em todos os testes de integração. Versões exatas, lockfile e digests delimitam a combinação validada.

Nest fornece módulos, injeção de dependências, guard global e OpenAPI; Fastify é o adaptador HTTP. MikroORM fornece o contexto de persistência, migrations e o acompanhamento da wallet, enquanto PostgreSQL concentra as garantias de concorrência e integridade. O AWS SDK v3 mantém o contrato SQS comum aos emuladores. Pino e prom-client produzem logs estruturados e métricas por processo. A escolha de SQL explícito para inbox/outbox e claims permite conferir a atomicidade dessas operações diretamente nas migrations e nos testes.

`APP_ROLES` permite combinar ou separar os participantes do fluxo em processos independentes:

| Papel        | Responsabilidade                                                        |
| ------------ | ----------------------------------------------------------------------- |
| `api`        | HTTP, Swagger, health e métricas                                        |
| `consumer`   | Receber comandos SQS, registrar inbox e chamar o caso de uso            |
| `references` | Reivindicar e reprocessar decisões com referências pendentes            |
| `publisher`  | Reivindicar outbox e publicar eventos no SQS                            |
| `dlq`        | Auditar persistentemente mensagens antes de removê-las da DLQ           |
| `events`     | Exemplo de deduplicação persistente dos eventos de integração recebidos |

Todos compartilham PostgreSQL e as filas, sem compartilhar memória para idempotência. Um processo sem `api` não abre porta HTTP. O pool PostgreSQL é limitado a oito conexões por processo; aumentar a quantidade de instâncias exige dimensionar o total de conexões no banco. O Compose demonstra três instâncias com portas próprias; não inclui um balanceador de carga.

## Dinheiro e operações

Money guarda centavos em bigint e é imutável. Conversão decimal usa exclusivamente strings. `NUMERIC(38,2)` admite até 36 dígitos inteiros; limites são verificados antes da escrita. Diferenças de reconciliação podem ser negativas, embora valores públicos e saldos persistidos não possam. Nenhum cálculo financeiro usa number; contadores, tempos e versões podem usá-lo.

| Operação         | Efeito e interpretação                                                                     |
| ---------------- | ------------------------------------------------------------------------------------------ |
| OPENING positivo | Crédito inicial interno, ledger versão 1 e eventos no mesmo commit                         |
| OPENING zero     | Wallet versão 1, sem transação financeira/ledger/evento de saldo                           |
| BET              | Débito positivo, rejeitado se saldo insuficiente                                           |
| WIN              | Crédito positivo; referência opcional exige BET processada, valor pode diferir             |
| LOSS             | Valor não negativo, decisão e evento; saldo/versão/ledger permanecem iguais                |
| REFUND           | Crédito integral de BET processada                                                         |
| ROLLBACK         | Inverso integral de BET, WIN ou REFUND processada; crédito ou débito conforme a referência |

Referências exigem mesmo provider, player, wallet, moeda e rodada. Game não é critério adicional de igualdade, seguindo o enunciado. BET/LOSS recusam referências; OPENING e provider `__system__` são internos. Reversões são exclusivas **por referência e tipo**: REFUND e ROLLBACK separados sobre a mesma BET são permitidos pelo texto do desafio, apesar de não ser uma regra usual em todo produto financeiro. Não inventamos uma exclusividade global diferente da especificada. Reversões parciais são rejeitadas.

A consequência dessa interpretação é explícita: saldo 100.00 → BET 30.00 → 70.00 → REFUND da BET → 100.00 → ROLLBACK da mesma BET → 130.00. O ledger permanece consistente, mas o efeito líquido é um crédito de 30.00. A regra adicional 4 da seção 7 restringe repetição pelo mesmo tipo de operação; uma exclusividade entre tipos exigiria outro contrato de negócio. Essa distinção precisa ser confirmada com o provedor antes de uma implantação real.

PENDING transiciona para PROCESSED, PENDING_REFERENCE, REJECTED ou FAILED. PENDING_REFERENCE pode continuar pendente ou terminar; terminações não reabrem. Reidratação reconstrói o estado existente e não reaplica regras de criação.

A [taxonomia de falhas](docs/FAILURE_CODES.md) especifica os códigos estáveis, diferencia rejeição persistida de erro de contrato/transporte e orienta a decisão do cliente sobre retry.

## Commit, concorrência e schema

Usamos READ COMMITTED e FOR UPDATE por wallet. Ordem: **wallet → inbox → transação**. Uma tentativa recebe um EntityManager exclusivo. Lock timeout 1 s, statement timeout 5 s e até três tentativas com backoff/jitter. Uma wallet bloqueada não serializa as outras. Referências são lidas sem lock de outra wallet; uma referência financeiramente válida necessariamente pertence à wallet já bloqueada. Estados terminais são imutáveis.

No commit ficam juntos: decisão/snapshot, wallet, ledger, inbox e eventos da outbox. Rejeições de negócio são dados persistidos. Erros técnicos abortam a transação; retry usa outro contexto. Falha permanente identificada, como permissão negada no ledger, pode ser registrada como FAILED em uma nova transação sem efeito financeiro. Se o próprio armazenamento da decisão estiver indisponível, mantemos retry/DLQ; não fingimos ter registrado FAILED.

O banco mantém uniques para player/moeda, chave idempotente global, provider/externalId, ledger por transação/wallet e wallet/versão, inbox por consumer/messageId, outbox eventId, auditoria DLQ e recibos de eventos. A identidade de uma wallet solicitada não tem FK na transação: uma wallet inexistente pode gerar rejeição auditável. Ledger exige wallet/transação existentes e mesma moeda/identidade.

Checks barram saldo negativo, valores inválidos/NaN e ledger aritmeticamente incorreto. Triggers barram mutação do ledger, alterações do comando, reabertura terminal, alteração de identidade inbox/outbox e resultado publicado. Constraints diferidas comparam saldo/versão, continuidade, operação/direção, referência e snapshot. Checam apenas os lançamentos relevantes, com índices por wallet/versão e transação; não somam todo o histórico em cada pagamento. A API aplica uma operação financeira por transação SQL.

`jungle_app` não é proprietário das tabelas e não tem UPDATE/DELETE/TRUNCATE no ledger. Migrations usam `jungle_owner`. Um administrador PostgreSQL continua podendo desabilitar triggers ou corromper dados; a reconciliação detecta esse cenário numa fixture isolada. Essa separação não pretende impedir um superuser.

A geração dos eventos correspondentes à decisão é responsabilidade do caso de uso, comprovada nos testes de contagens e conteúdo da outbox. As constraints protegem a identidade e a imutabilidade do envelope, mas não comparam todos os campos de cada evento ao ledger nem comprovam que um envio externo ao SQS aconteceu. O papel da aplicação precisa poder escrever snapshots pendentes, agendamentos, leases e marcas de publicação. Essas permissões pressupõem código e credenciais da aplicação confiáveis; não constituem uma proteção contra seu comprometimento. Essa fronteira também está registrada no [tratamento da auditoria independente](docs/results/audit-follow-up.md).

## Idempotência e snapshots

SHA-256 sobre JSON com chaves ordenadas recursivamente. Campos de negócio do README entram; chave idempotente e contexto de transporte ficam fora. Money é normalizado e referência ausente vira null. UUIDs de jogador/wallet são normalizados para minúsculas.

Mesma chave/payload retorna o snapshot persistido. Mesma chave/payload divergente e identidade externa com nova chave retornam 409 em HTTP, sem tocar a operação original. Uniques e locks garantem esse resultado entre processos. A inbox verifica separadamente o hash canônico do envelope validado: `messageId`, `type`, `occurredAt` e `data`. O schema descarta campos extras no topo do envelope; todos os campos recebidos dentro de `data` participam desse hash, inclusive a chave idempotente. Mudanças de ordem das chaves JSON preservam o hash; alterações no conteúdo validado sob o mesmo messageId geram conflito. Essa comparação do transporte é distinta da normalização usada no hash de negócio.

O saldo retornado é o saldo observado na decisão, e não o saldo atual em qualquer replay futuro. PENDING_REFERENCE guarda o primeiro snapshot; retries operacionais não o substituem. Na finalização, grava o snapshot terminal. HTTP 503 é inconclusivo: um cliente deve repetir a mesma chave/payload, pois pode haver commit seguido de falha de comunicação.

## Referências, transporte e DLQ

A inbox usa o messageId **do envelope**, não o dedupId FIFO nem exclusivamente o MessageId do broker. Mensagens pendentes são confirmadas depois do commit para não bloquear uma BET posterior no mesmo grupo FIFO. Workers persistentes reivindicam referências em uma transação curta com SKIP LOCKED, liberam-na e depois executam o caso financeiro na ordem de locks normal. Leases usam horário do banco e token de posse. Retries preservam snapshot e evento pendente único; TTL 24 h distingue referência inexistente de não processada. Referência terminal inválida rejeita imediatamente.

Recepção SQS: long polling 20 s, visibilidade 30 s, renovação 10 s, redrive 5 recebimentos. Negócio rejeitado recebe ack; transitório tem retry; permanente é encaminhado à DLQ antes do ack. Quando há inbox, o encaminhamento pendente fica persistido para retomada. O DLQ auditor grava raw body, hash e motivo antes de removê-lo, deduplicando a origem. Ele nunca reabre uma operação financeira confirmada. Esgotar entregas não torna uma indisponibilidade transitória automaticamente FAILED.

Conflitos de chave idempotente ou identidade externa em SQS são confirmados com uma inbox processada, conservando a decisão financeira original. Não criam uma nova transação rejeitada. Já um conflito de conteúdo sob o mesmo messageId vai à DLQ sem alterar a inbox original. Envelopes inválidos podem chegar à DLQ antes de existir uma inbox; nesses casos, a auditoria usa uma identidade válida do envelope ou a identidade segura do transporte, com o hash do raw body.

O adaptador aplica deadlines de 5 s à resolução de URLs, verificação de filas, envios, acks e alterações de visibilidade. Bootstrap é uma operação de setup separada. A recepção usa long polling e o sinal de cancelamento do consumidor. O SDK tem `maxAttempts: 1`; o código aplica o retry operacional. Retry de transporte, retry de banco e retry de referência são contadores separados. Backoff de banco, referência e outbox tem jitter; a retentativa de visibilidade SQS usa espera exponencial limitada a 60 s. As janelas de visibilidade/lease não são substitutos para idempotência.

## Outbox, duplicatas e shutdown

Outbox contém o envelope imutável criado dentro da transação financeira. Publishers reivindicam lote de até 20, com SKIP LOCKED e lease de 60 s. Enviam no máximo cinco simultaneamente, fora da transação financeira, com deadline de 5 s. Quando existe backlog, continuam drenando; quando o lote está vazio, esperam 500 ms. Isso evita uma pausa artificial em cada lote, um gargalo identificado no teste de carga.

Publicação confirmada exige o token de posse ainda vigente e lease não expirada. Worker antigo não sobrescreve sucessor. Falhas reagendam em até 5 min; o registro confirmado permanece até envio bem-sucedido. Morte depois do envio e antes da marcação pode reenviar o evento: eventId permanece igual. FIFO dedup é uma otimização limitada no tempo. Consumidores precisam dedup persistente; o exemplo usa consumer/eventId. Eventos de saldo incluem walletVersion; múltiplos publishers podem publicar fora de ordem financeira, então consumidores de projeção devem tratar versões/gaps.

SIGTERM para novas recepções, cancela long polling e drena trabalho por até 25 s. Depois cancela as renovações de visibilidade e devolve as mensagens restantes, com deadline adicional de 1 s e o cliente SQS ainda aberto. O encerramento reserva essa etapa antes de fechar as dependências; workers de outbox/referência também têm drenagem limitada. Se SQS estiver indisponível durante a devolução, a visibilidade original expira normalmente. Transações inacabadas são revertidas quando a conexão/processo termina; leases abandonadas expiram. Todas as garantias dependem da durabilidade do commit PostgreSQL e da confirmação durável de SQS.

| Momento da falha                                      | Estado durável e recuperação                                                                                                              |
| ----------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------- |
| Antes do commit financeiro                            | Escritas da tentativa são revertidas; reenviar com a mesma chave permite processar a operação                                             |
| Depois do commit, antes da resposta HTTP              | Decisão e snapshot já existem; replay pela mesma chave recupera o resultado original                                                      |
| Depois do commit, antes do ack SQS                    | Redelivery consulta idempotência/inbox e confirma a mensagem sem repetir o movimento                                                      |
| Depois do envio do evento, antes de marcar outbox     | O evento pode ser reenviado; eventId estável permite deduplicação persistente no consumidor                                               |
| Depois do envio à DLQ, antes de registrar confirmação | Quando há inbox correspondente, mantém encaminhamento pendente; redelivery retoma o envio, e a auditoria deduplica origem e hash do corpo |
| Após reivindicar uma referência, antes de processá-la | Lease expira; outro worker retoma a decisão pendente usando os locks e validações do caso de uso                                          |

## Consultas e diagnóstico

Ledger usa cursor base64url com walletId, último item e limite superior fixado na primeira página; movimentos novos não se misturam às páginas seguintes. O cursor organiza paginação e não é um mecanismo de autorização. Reconciliação usa REPEATABLE READ, soma decimal exata e diferença assinada; responde, loga e incrementa métrica quando há divergência. Não corrige automaticamente.

Logs registram correlationId, messageId quando aplicável, transactionId, walletId e providerId, sem payload financeiro completo. Métricas não usam identidades individuais como labels. Liveness não chama dependências; readiness verifica PostgreSQL e as três filas. Instâncias somente de workers podem adicionar o papel api para expor health/metrics.

`wager_retries_total{component="reference"}` conta cada tentativa reivindicada de reprocessamento de uma decisão pendente, inclusive a que a finaliza com sucesso. É uma contagem de retentativas, não de falhas. Nos demais componentes, o contador é incrementado ao tratar uma falha para nova tentativa. Um primeiro reprocessamento bem-sucedido de referência continua sendo uma retentativa da submissão original.

## Identidade, ambientes e limites

O enunciado permite focar nos critérios financeiros e documentar a identidade. DevelopmentIdentity é deliberadamente sem autenticação. `IdentityGuard` é registrado globalmente e consulta `IdentityPort` uma vez em cada rota de negócio e em `/metrics`; apenas os handlers de health têm exceção explícita. O adaptador pode ser fornecido ao configurar `ApiModule`. Um teste HTTP real com adaptador restritivo comprova recusa de todas essas rotas e health público. Swagger é servido separadamente pelo plugin e fica aberto neste perfil local.

Para implantação exposta, implementar adapter OIDC/JWKS com verificação de issuer, audience, assinatura, expiração e rotação, e autorização de provider/player/wallet; derivar o provider do principal verificado e não confiar no body. O guard global é a fronteira existente, mas não implementa essas verificações no modo development. Credenciais locais do banco e AWS fictícias são exclusivas do Compose; produção precisa secrets, TLS, grants e observabilidade próprios.

LocalStack é a validação principal; MiniStack executa a mesma suíte portátil. Snapshots no shutdown e load no startup usam a [configuração documentada de persistência](https://docs.localstack.cloud/aws/developer-tools/snapshots/persistence/). O Compose configura `PERSISTENCE=1`, `SNAPSHOT_SAVE_STRATEGY=ON_SHUTDOWN`, `SNAPSHOT_LOAD_STRATEGY=ON_STARTUP` e 30 s de grace period. Snapshots periódicos ficam desabilitados; o salvamento é solicitado no encerramento gracioso. A prova adicional de cinco ciclos por broker inclui long polling ativo e mensagens recebidas sem ack, conservando todas as 180 mensagens enviadas em cada execução registrada. Essa evidência cobre reinício **gracioso**, sem prometer durabilidade do emulador após SIGKILL.

Um incidente anterior de restauração local não reapareceu nas revalidações, mas sua causa permanece não confirmada. A configuração e os resultados posteriores não devem ser apresentados como prova de correção dessa causa. O [relatório pré-entrega](docs/results/pre-delivery.md) e a [validação adicional](docs/results/additional-validation.md) registram a observação e os limites da conclusão. A recuperação de crashes da aplicação é testada separadamente. Não foi realizada validação contra AWS real; a durabilidade do serviço SQS e a configuração de produção constituem fronteiras externas aos experimentos locais.

Sem poda automática de ledger, transações, inbox/outbox publicadas ou auditoria: preservar evidência é adequado ao desafio; produção exigiria política de retenção, partições e backups sem quebrar idempotência. Sem balanceador de carga, Kubernetes, cache financeiro, correção automática de saldo ou IdP ativo. Essas extensões não substituem as provas de consistência.

O benchmark local mede três processos e duas distribuições de wallet, com aquecimento, repetições, rejeições esperadas e outbox lag. Ele evidencia contenção por wallet e capacidade distinta do publisher; não define SLA ou RPS de produção. [Resultados](docs/results/README.md).

## Onde conferir as garantias

| Garantia                               | Implementação                                                                                                                       | Evidência reproduzível                                                                                            |
| -------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------- |
| Dinheiro exato e domínio               | [Money](src/domain/money.ts), [Wallet](src/domain/wallet.ts), [transação](src/domain/transaction.ts)                                | [Unidade](tests/unit/domain.test.ts): precisão acima de 2^53, limites, moedas, estados terminais e imutabilidade  |
| Atomicidade e integridade SQL          | [Caso de uso](src/application/wagering.ts), [Database](src/infrastructure/database.ts), [migrations](src/infrastructure/migrations) | [Integração](tests/integration/system.test.ts): up/down/up, violações SQL diretas e reconstrução de saldo/versões |
| Concorrência e idempotência            | [Wagering.process/apply](src/application/wagering.ts), [hashes e contratos](src/application/contracts.ts)                           | 50 submissões, duas BETs de 80, HTTP/SQS mistos, snapshots após outras movimentações e três seeds independentes   |
| Referências e recuperação              | [ReferenceWorker/QueueConsumer](src/infrastructure/workers.ts)                                                                      | Fora de ordem, TTL, lease vencida, crash antes/depois do commit e antes do ack, retry e auditoria DLQ             |
| Publicação e duplicatas                | [Publisher](src/infrastructure/workers.ts), [Queues](src/infrastructure/sqs.ts)                                                     | Dois publishers, crash após envio, fencing e confirmação de três cópias SQS com um recibo de evento               |
| Paginação e reconciliação              | [Queries](src/application/queries.ts)                                                                                               | Cursor durante novos movimentos, snapshot REPEATABLE READ com commit concorrente e corrupção isolada              |
| Identidade e observabilidade           | [Guard/API](src/http/api.ts), [métricas/logs](src/infrastructure/observability.ts), [runtime](src/main.ts)                          | Guard restritivo real, health durante indisponibilidade, divergências e métricas no teste de carga                |
| Persistência do broker e ambiente novo | [Isolated verify](scripts/isolated-verify.ts), [Recovery check](scripts/recovery-check.ts)                                          | `test:isolated` e `test:recovery` nos dois brokers, com relatórios e limpeza dos recursos exclusivos              |

A suíte completa contém 28 testes unitários e 42 de integração; `test:recovery` acrescenta cinco ciclos por broker fora dessa contagem. Os testes de reinício da integração só executam com `TEST_BROKER` configurado; `test:isolated` define essa variável e direciona os reinícios ao projeto exclusivo. Comandos completos estão no [README](README.md#testes-e-verificação); a [CI por commit](https://github.com/EduardoPaim5/jungle-backend-challenge/actions) e os relatórios indicam o que efetivamente foi executado. Nenhuma dessas provas cobre todas as combinações possíveis de falhas ou substitui a qualificação de um ambiente de produção.
