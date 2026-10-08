# Validação local registrada

Data: 7 de outubro de 2026. Bun 1.4.2, NestJS 12.1.2, MikroORM 7.2.4, TypeScript 5.9.3. PostgreSQL 17 em container com digest fixado. Dependências instaladas com `--frozen-lockfile`.

| Prova                                                                   | Resultado                                                                           |
| ----------------------------------------------------------------------- | ----------------------------------------------------------------------------------- |
| `bun run format:check`                                                  | Aprovada                                                                            |
| `bun run typecheck`                                                     | Aprovada, modo estrito                                                              |
| `bun audit`                                                             | Nenhuma vulnerabilidade conhecida reportada em 155 pacotes                          |
| `bun run test`                                                          | 25 testes unitários aprovados                                                       |
| `TEST_BROKER=localstack bun run verify`                                 | 25 unitários + 34 integrações aprovados, sem skipped                                |
| `TEST_BROKER=ministack bun run verify`                                  | 25 unitários + 34 integrações aprovados, sem skipped                                |
| Migrações up/down completo/up                                           | Aprovadas em bancos isolados nos dois brokers                                       |
| Crashes, redelivery, publisher antigo e reinício real de SQS/PostgreSQL | Aprovados nos dois brokers                                                          |
| Shutdown com devolução atrasada por barreira e visibilidade de 30 s     | Mensagem recuperada imediatamente após SIGTERM, sem esperar a expiração natural     |
| Compose com `--scale app=3`                                             | Três containers Bun executando como usuário `bun`; setup aplicou migrations e filas |
| Demo nas três instâncias Docker                                         | BET/replay/REFUND fora de ordem/SQS/reconciliação aprovados                         |
| `BROKER_LABEL=LocalStack bun run test:load`                             | 6 repetições medidas, zero erros técnicos, saldos consistentes e outbox drenada     |
| Revisão de segredo                                                      | Token ausente dos arquivos versionáveis; `.env.local` ignorado pelo Git e Docker    |

As integrações usam ao menos três processos Bun simultâneos e conexões PostgreSQL/SQS reais. Os checks finais verificam saldo reconstruído, ledger, versões e decisões, além dos checks específicos de inbox/outbox/DLQ. A recuperação do broker inclui persistência de mensagens após shutdown normal; não mede durabilidade do emulador após SIGKILL.

A [rodada adversarial adicional](adversarial.md) acrescentou contratos HTTP, precisão no caminho completo, identidades entre wallets, concorrência de referências, lease antiga, perda de resposta HTTP, snapshot de reconciliação, auditoria de envelopes com identidade inválida e um modelo financeiro independente com seed reproduzível. A regressão da identidade inválida falhou antes da correção e passou nos dois brokers após ela, sem efeitos financeiros.

As ações remotas da CI são consultáveis na aba Actions do repositório. Este registro descreve o ambiente local; resultados da CI são evidência separada. Cada execução de integração usa um banco e filas próprios e remove seus recursos ao final.

Após a auditoria independente, foram acrescentadas uma prova unitária de payload divergente com a mesma chave e uma integração do guard global com adaptador restritivo de identidade. As duas suítes completas foram repetidas com sucesso. O [tratamento dos achados](audit-follow-up.md) explica as correções e as decisões mantidas; `bun audit` também passou a ser executado nos dois jobs da CI.
