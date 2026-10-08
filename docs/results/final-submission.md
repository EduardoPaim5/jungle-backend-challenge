# Validação final para entrega — 8 de outubro de 2026

O [enunciado oficial](https://github.com/junglegaming/backend-challenge/blob/c7143e6d041585d6913f56eb7415105e074f83dc/README.md) foi relido integralmente e seu commit foi reconfirmado. Esta rodada revisou a implementação publicada em `c2c6531` e acrescentou apenas documentação e testes de unidade. Não foi identificado novo defeito no processamento financeiro nem impedimento eliminatório confirmado. A [matriz de requisitos](../REQUIREMENTS.md) relaciona as oito áreas pontuadas à implementação e às provas; a pontuação pertence aos avaliadores.

## Complementos da revisão

A seção 7.2 exige uma taxonomia documentada. A nova [tabela de códigos](../FAILURE_CODES.md) diferencia rejeições persistidas, erros de contrato/transporte, conflitos e falhas permanentes, com orientação sobre recuperação. A seção 13 exige testes unitários das cinco operações: três cenários adicionais exercitam BET/WIN/LOSS, REFUND integral e seus erros, e os três tipos de ROLLBACK, incluindo débito de reversão sem saldo. Essas regras já tinham cobertura de integração; agora têm provas diretas de domínio.

## Suíte completa em ambientes novos

```bash
TEST_BROKER=localstack bun run test:isolated
TEST_BROKER=ministack bun run test:isolated
RECOVERY_BROKER=localstack RECOVERY_CYCLES=5 bun run test:recovery
RECOVERY_BROKER=ministack RECOVERY_CYCLES=5 bun run test:recovery
```

Cada execução completa verificou formatação, tipos, build, **28 testes unitários e 42 de integração**, sem skipped e sem falhas. PostgreSQL, broker, portas e volumes eram próprios de cada experimento. Os testes incluíram migrations up/down/up, ataques diretos às constraints, três APIs, corridas financeiras, replay, referências fora de ordem, indisponibilidade, shutdown e crashes sincronizados antes/depois do commit e depois da publicação. Os três seeds do oráculo independente e o cenário concorrente HTTP/SQS também passaram.

| Broker     | Unidade | Integração | Suíte completa | Reinícios adicionais | Mensagens recuperadas |
| ---------- | ------: | ---------: | -------------: | -------------------: | --------------------: |
| LocalStack |      28 |         42 |       100,45 s |                    5 |               180/180 |
| MiniStack  |      28 |         42 |        75,01 s |                    5 |               180/180 |

Relatórios completos: [suíte LocalStack](final-isolated-localstack.json), [suíte MiniStack](final-isolated-ministack.json), [recuperação LocalStack](final-recovery-localstack.json) e [recuperação MiniStack](final-recovery-ministack.json). As filas de recuperação foram criadas uma vez; reinícios conservaram mensagens recebidas sem ack e ocorreram com long polling ativo nos ciclos 2 a 5. Todos os encerramentos tiveram exit code 0 e nenhum OOM. Os recursos exclusivos foram removidos ao final.

## Comandos do README e imagem de aplicação

Em outro projeto Compose novo, a imagem foi reconstruída e o setup aplicou migrations e criou as filas. Três containers da aplicação responderam HTTP 200 em `/health/live`, `/health/ready`, `/docs`, `/docs-json` e `/metrics`. Os 13 blocos Bash do README passaram por validação de sintaxe; scripts e links locais foram conferidos.

O bloco de demonstração Docker do README foi executado sem editar seus comandos, usando as três portas descobertas pelo Compose. Replay em outra instância, REFUND pendente antes da BET e finalização por SQS passaram. A reconciliação final retornou saldo armazenado e calculado de `75.00 BRL`, diferença `0.00` e quatro entradas. JSON malformado, mídia incompatível e corpo excedente retornaram respectivamente 400/415/413, com `HTTP_ERROR`, correlação preservada e `retryable: false`.

## Carga final

Depois de parar os três containers da aplicação, `bun run test:load` iniciou três processos Bun contra esse PostgreSQL/LocalStack novo. Foram 100 requisições de aquecimento por topologia, seguidas de três repetições de 600, com concorrência 24. O ambiente foi a máquina local compartilhada: Ryzen 5 4500, 12 CPUs lógicas, aproximadamente 15,5 GiB de RAM, Linux, Bun 1.4.2 e PostgreSQL 17.11. Não houve reserva de CPU nem controle experimental de outros serviços da máquina.

| Topologia  | Repetição | req/s | p50 ms | p95 ms | p99 ms | Espera média de lock ms | Lag máximo observado s |
| ---------- | --------- | ----: | -----: | -----: | -----: | ----------------------: | ---------------------: |
| Uma wallet | 1         | 114,6 |  245,0 |  365,3 |  396,5 |                   178,0 |                   0,50 |
| 24 wallets | 1         | 376,8 |   60,1 |   99,7 |  128,5 |                     7,3 |                   1,17 |
| Uma wallet | 2         | 118,6 |  250,4 |  361,1 |  383,8 |                   170,6 |                   2,21 |
| 24 wallets | 2         | 448,0 |   49,8 |   89,9 |  107,2 |                     4,6 |                   1,13 |
| Uma wallet | 3         | 108,9 |  245,7 |  460,8 |  551,2 |                   184,4 |                   3,21 |
| 24 wallets | 3         | 513,6 |   44,2 |   70,9 |   84,5 |                     4,6 |                   1,06 |

As 3.600 requisições medidas tiveram 3.240 operações processadas e 360 rejeições esperadas por saldo insuficiente, **zero erros técnicos**, zero timeouts/conflitos de lock e zero retries do banco. Todas as reconciliações foram consistentes. A espera de lock na wallet concentrada mostra contenção mesmo sem timeout; ausência de conflitos contabilizados não significa ausência de espera. Os percentis incluem rejeições esperadas.

A outbox chegou a 1.012 eventos pendentes durante um pico e terminou com **zero pendências**, após drenagem final de 1,82 s. O lag é amostrado a cada 200 ms; a drenagem não integra o throughput de ingestão. O [JSON completo](final-load-localstack.json) conserva ambiente, metodologia, todas as medições e backlog. Esses números não estimam capacidade de produção nem definem limites de latência garantidos.

## Integridade, credenciais e limites

A inspeção SQL final encontrou zero wallets divergentes e zero decisões incompatíveis com o ledger. O papel `jungle_app` não é superuser e não tem UPDATE, DELETE ou TRUNCATE no ledger. Instalação com lockfile congelado e `bun audit` passaram; a auditoria não reportou vulnerabilidades. O token configurado não foi encontrado nos arquivos versionados nem no histórico Git; também não foram encontrados padrões de credenciais AWS/GitHub ou chaves privadas. O repositório de entrega é público, com `main` como branch padrão.

O incidente anterior de restauração do emulador não reapareceu, mas sua causa continua não confirmada; esta rodada não é apresentada como correção dessa causa. Os [resultados anteriores](pre-delivery.md) e [ARCHITECTURE](../../ARCHITECTURE.md) preservam essa ressalva. Reinícios do broker comprovam encerramento gracioso; crashes da aplicação são provas separadas. Não houve teste contra AWS real. A extensão de identidade e o desenho de OIDC permanecem explícitos, conforme a opção aceita na seção 2 do desafio.

A [CI por commit](https://github.com/EduardoPaim5/jungle-backend-challenge/actions) repete a suíte e os cinco ciclos nos dois brokers. O resultado da CI do commit final deve ser conferido junto com estas evidências locais. A revisão não encontrou falha eliminatória confirmada, mas não prova todas as combinações possíveis de falhas.
