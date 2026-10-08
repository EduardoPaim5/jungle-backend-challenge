# Resultados registrados

As evidências são de execução local, com versões fixadas pelo repositório. Não constituem previsão de pontuação ou capacidade de produção.

A [validação adicional](additional-validation.md) registra três seeds do oráculo financeiro, operações mistas concorrentes por HTTP/SQS, novos limites de contrato, verificação completa em containers novos e cinco reinícios de persistência em cada broker. A suíte atual contém 25 testes unitários e 42 de integração; `test:recovery` é uma prova adicional na CI.

A [revisão pré-entrega de 8 de outubro](pre-delivery.md) registra as correções de limites das consultas, instalação em checkout limpo, execução com três containers e revisão de consistência/credenciais. Inclui também o incidente local de snapshots e sua revalidação, com os limites da conclusão explícitos.

A [carga de revalidação em 8 de outubro](load-pre-delivery.json) acrescenta três repetições por topologia: zero erros técnicos, consistência preservada e outbox sem pendências após a drenagem. O relatório anterior permanece disponível para comparação; variações entre execuções refletem também o uso compartilhado da máquina.

O [relatório JSON de carga](load-localstack.json) registra máquina, versões, topologia, concorrência, todas as repetições, percentis, rejeições deliberadas, erros técnicos, espera/conflitos e atraso da outbox. Para gerar evidência nova: `BROKER_LABEL=LocalStack bun run test:load`; o relatório local completo fica em `artifacts/load.json`.

## Carga em LocalStack — 7 de outubro de 2026

Três processos Bun, concorrência 24, aquecimento de 100 requisições por topologia e três repetições de 600. Ambiente local compartilhado, sem reserva de CPU. Cada repetição teve 540 operações processadas, 60 rejeições esperadas, **zero erros técnicos**, zero timeouts/conflitos de lock e reconciliação consistente.

| Topologia  | Repetição | req/s | p50 ms | p95 ms | p99 ms | Espera média de lock ms | Lag máximo observado s |
| ---------- | --------- | ----: | -----: | -----: | -----: | ----------------------: | ---------------------: |
| Uma wallet | 1         | 145,3 |  185,3 |  309,0 |  355,7 |                   139,8 |                   0,76 |
| 24 wallets | 1         | 495,3 |   46,6 |   69,7 |   80,0 |                     4,1 |                   1,02 |
| Uma wallet | 2         | 152,0 |  203,8 |  269,9 |  290,2 |                   132,6 |                   2,05 |
| 24 wallets | 2         | 611,4 |   38,5 |   52,1 |   56,9 |                     4,0 |                   1,17 |
| Uma wallet | 3         | 159,0 |  188,8 |  281,7 |  300,8 |                   127,3 |                   2,38 |
| 24 wallets | 3         | 596,0 |   38,2 |   55,0 |   62,8 |                     3,7 |                   1,66 |

Os números mostram a contenção esperada na wallet concentrada e a independência de wallets distintas. A outbox acumulou backlog durante os picos e chegou a **zero pendências** ao final da drenagem. A amostragem de lag é feita a cada 200 ms; valores são observações, não limites garantidos. O desempenho do emulador/publisher não acompanha necessariamente o pico de ingestão HTTP. Não há extrapolação de RPS para produção.

As contagens de testes e verificação dos containers estão em [validation.md](validation.md). As suítes podem ser reproduzidas em ambiente próprio usando as instruções do README.
