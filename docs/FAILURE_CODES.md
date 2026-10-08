# Códigos de falha e decisão do cliente

A API diferencia uma decisão financeira persistida de um erro na entrada ou no transporte. Em `POST /wagering/transactions`, decisões `REJECTED` usam HTTP 422 e `failureCode`; decisões `FAILED` usam HTTP 500 e `failureCode`. Erros tratados pelo filtro HTTP retornam `code`, `correlationId` e `retryable`. Consultas de transações existentes retornam HTTP 200 com o estado persistido, inclusive quando ele é `REJECTED` ou `FAILED`.

## Rejeições financeiras persistidas

| `failureCode`                 | Significado                                                                             |
| ----------------------------- | --------------------------------------------------------------------------------------- |
| `INSUFFICIENT_FUNDS`          | BET excede o saldo disponível no momento da decisão                                     |
| `REVERSAL_INSUFFICIENT_FUNDS` | Débito de ROLLBACK de WIN ou REFUND produziria saldo negativo                           |
| `AMOUNT_MUST_BE_POSITIVE`     | BET, WIN, REFUND ou ROLLBACK recebeu valor zero; LOSS aceita zero                       |
| `WALLET_NOT_FOUND`            | Wallet solicitada não existe; a identidade pedida permanece na transação para auditoria |
| `PLAYER_MISMATCH`             | Jogador do comando difere do proprietário da wallet                                     |
| `CURRENCY_MISMATCH`           | Moeda do comando difere da wallet; a API pública aceita apenas BRL                      |
| `MONEY_LIMIT_EXCEEDED`        | Movimento faria o saldo exceder NUMERIC(38,2)                                           |
| `REFERENCE_CONTEXT_MISMATCH`  | Referência difere em provedor, jogador, wallet, moeda ou rodada                         |
| `INVALID_REFERENCE_KIND`      | Referência tem tipo incompatível, ou a operação referencia a si mesma                   |
| `REFERENCE_AMOUNT_MISMATCH`   | Valor de REFUND ou ROLLBACK difere do valor integral da referência                      |
| `REVERSAL_ALREADY_APPLIED`    | Já existe uma reversão processada do mesmo tipo sobre essa referência                   |
| `REFERENCE_NOT_FOUND`         | TTL de 24 horas expirou e a referência ainda não existe                                 |
| `REFERENCE_NOT_PROCESSED`     | Referência terminou rejeitada/falha, ou o TTL expirou sem ela chegar a PROCESSED        |

Essas decisões são terminais: repetir a mesma chave e payload retorna o resultado original, sem reaplicar a operação. Uma mudança posterior no saldo ou a chegada tardia de uma referência não reabre uma rejeição. Para uma nova operação de negócio, o provedor deve emitir nova identidade externa e nova chave; isso exige uma decisão explícita do cliente, não um retry automático.

Referência ausente ou ainda não processada, dentro do TTL, produz `PENDING_REFERENCE`, HTTP 202 e nenhum `failureCode`. O worker reprocessa essa decisão. O cliente pode consultar o transactionId; reenviar a mesma chave também retorna seu snapshot pendente ou o resultado terminal que já tiver sido confirmado.

## Contrato, conflitos, consultas e indisponibilidade

| `code`                          | HTTP | Significado e ação                                                                                                    |
| ------------------------------- | ---: | --------------------------------------------------------------------------------------------------------------------- |
| `INVALID_REQUEST`               |  400 | Estrutura, identificadores, chave ou exigência de referência inválidos; corrigir a entrada                            |
| `INVALID_MONEY`                 |  400 | Dinheiro fora do formato decimal de duas casas, negativo ou não finito; corrigir a entrada                            |
| `UNSUPPORTED_CURRENCY`          |  400 | Moeda pública diferente de BRL; corrigir a entrada                                                                    |
| `MONEY_LIMIT_EXCEEDED`          |  400 | Valor de entrada excede NUMERIC(38,2); difere da rejeição de overflow do saldo já descrita acima                      |
| `INVALID_CURSOR`                |  400 | Cursor ou limite inválido, inclusive cursor de outra wallet; usar a paginação da wallet correta                       |
| `WALLET_ALREADY_EXISTS`         |  409 | Já existe wallet para jogador/moeda; conservar a associação da wallet existente no cliente                            |
| `IDEMPOTENCY_CONFLICT`          |  409 | Chave já associada a outro payload; consultar a decisão e corrigir a integração                                       |
| `EXTERNAL_TRANSACTION_CONFLICT` |  409 | Provedor/identidade externa já associado a outra chave; recuperar a identidade original                               |
| `WALLET_NOT_FOUND`              |  404 | Wallet ausente numa consulta ou reconciliação; conferir o identificador                                               |
| `TRANSACTION_NOT_FOUND`         |  404 | Transação ausente numa consulta por id ou identidade externa; conferir o identificador                                |
| `INFRASTRUCTURE_UNAVAILABLE`    |  503 | Conexão, lock ou tentativa técnica inconclusiva; reenviar a mesma chave e o mesmo payload                             |
| `DEPENDENCY_UNAVAILABLE`        |  503 | Readiness detectou PostgreSQL ou filas SQS indisponíveis; restaurar dependências antes de usar a API                  |
| `INTERNAL_ERROR`                |  500 | Erro interno sem decisão FAILED garantida; consultar a identidade e conservar a chave em reenvios                     |
| `HTTP_ERROR`                    |  4xx | Exceção HTTP do Nest, conservando seu status: JSON inválido, mídia incompatível, corpo grande ou recusa de identidade |

Os testes conferem HTTP 400/415/413 para JSON malformado, mídia incompatível e corpo grande, sem efeitos financeiros, preservando `correlationId` e `retryable: false`.

HTTP 503 inclui `Retry-After: 1`, `retryable: true` e a instrução de reenviar a mesma chave/payload. Ele não prova que o commit deixou de ocorrer. Chamar a mesma operação com uma chave diferente não é a estratégia de recuperação de uma resposta perdida.

## Falhas permanentes e SQS

`PERMANENT_INFRASTRUCTURE_ERROR` é um `failureCode` de `FAILED`. O consumidor identifica falhas permanentes específicas do PostgreSQL, como permissão negada, e tenta registrar essa decisão em uma nova transação sem movimento financeiro. A decisão e seu evento são persistidos antes do encaminhamento à DLQ. Se o armazenamento da decisão também falhar, não há confirmação fictícia de FAILED: a entrega continua sujeita a retry/redrive e auditoria.

No SQS, rejeições de negócio e conflitos de chave/identidade externa recebem ack após sua persistência. Conflito de conteúdo sob o mesmo messageId (`INBOX_PAYLOAD_CONFLICT`) vai à DLQ sem modificar a inbox original. Envelopes inválidos usam motivo `INVALID_ENVELOPE`; mensagens enviadas pelo redrive sem motivo explícito são auditadas como `DELIVERY_RETRIES_EXHAUSTED`. Esgotar recebimentos não transforma automaticamente uma indisponibilidade temporária em uma decisão financeira FAILED.

`LEASE_LOST` é um controle interno dos workers, sem reabrir ou sobrescrever uma decisão. `EVENT_PAYLOAD_CONFLICT` identifica conteúdo divergente sob um eventId já recebido pelo consumidor de exemplo; o recibo original é preservado. Esses controles operacionais não são rejeições financeiras do provedor.

Implementação: [contratos](../src/application/contracts.ts), [caso de uso](../src/application/wagering.ts), [regras de domínio](../src/domain/transaction.ts), [workers](../src/infrastructure/workers.ts) e [filtro HTTP](../src/http/api.ts). As regras de atomicidade e confiança estão em [ARCHITECTURE](../ARCHITECTURE.md).
