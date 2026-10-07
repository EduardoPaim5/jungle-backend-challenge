# Validação local registrada

Data: 7 de outubro de 2026. Bun 1.4.2, NestJS 12.1.2, MikroORM 7.2.4, TypeScript 5.9.3. PostgreSQL 17 em container com digest fixado. Dependências instaladas com `--frozen-lockfile`.

| Prova                                                                   | Resultado                                                                           |
| ----------------------------------------------------------------------- | ----------------------------------------------------------------------------------- |
| `bun run format:check`                                                  | Aprovada                                                                            |
| `bun run typecheck`                                                     | Aprovada, modo estrito                                                              |
| `bun run test`                                                          | 23 testes unitários aprovados                                                       |
| `TEST_BROKER=localstack bun run verify`                                 | 23 unitários + 22 integrações aprovados, sem skipped                                |
| `TEST_BROKER=ministack bun run verify`                                  | 23 unitários + 22 integrações aprovados, sem skipped                                |
| Migrações up/down completo/up                                           | Aprovadas em bancos isolados nos dois brokers                                       |
| Crashes, redelivery, publisher antigo e reinício real de SQS/PostgreSQL | Aprovados nos dois brokers                                                          |
| Compose com `--scale app=3`                                             | Três containers Bun executando como usuário `bun`; setup aplicou migrations e filas |
| Demo nas três instâncias Docker                                         | BET/replay/REFUND fora de ordem/SQS/reconciliação aprovados                         |
| `BROKER_LABEL=LocalStack bun run test:load`                             | 6 repetições medidas, zero erros técnicos, saldos consistentes e outbox drenada     |
| Revisão de segredo                                                      | Token ausente dos arquivos versionáveis; `.env.local` ignorado pelo Git e Docker    |

As integrações usam ao menos três processos Bun simultâneos e conexões PostgreSQL/SQS reais. Os checks finais verificam saldo reconstruído, ledger, versões e decisões, além dos checks específicos de inbox/outbox/DLQ. A recuperação do broker inclui persistência de mensagens após shutdown normal; não mede durabilidade do emulador após SIGKILL.

As ações remotas da CI são consultáveis na aba Actions do repositório. Este registro descreve o ambiente local; resultados da CI são evidência separada. Cada execução de integração usa um banco e filas próprios e remove seus recursos ao final.
