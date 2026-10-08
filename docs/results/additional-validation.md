# Validação adicional — 8 de outubro de 2026

Esta rodada aprofunda as provas após a revisão do commit `1f0af4c`. Não alterou o processamento financeiro ou os contratos de produção. O inventário passou a **25 testes unitários e 42 de integração**; as novas verificações de recuperação também são executadas pela CI.

## Operações financeiras e transporte

O oráculo independente executa três seeds reproduzíveis (`4a554e47`, `12345678`, `deadbeef`). Cada cenário tem uma BET inicial, mais 120 operações BET/WIN/LOSS/REFUND/ROLLBACK e 14 replays. São 363 comandos financeiros únicos e 42 replays, com saldo, versões, ledger, rejeições e eventos comparados usando centavos em bigint sem chamar a aritmética do domínio.

Um cenário concorrente adicional usa três wallets, três APIs, dois consumidores e dois publishers/consumidores de eventos. As 144 operações únicas geram 288 requisições HTTP e 288 envelopes SQS, com identificadores de transporte/deduplicação diferentes. Inclui apostas acima do saldo, créditos, LOSS zerada, reversões disputadas e valores de reversão divergentes.

A ordem confirmada é reconstruída a partir das versões do ledger. O oráculo verifica cada movimento, os snapshots das decisões, a insuficiência de saldo no momento de cada rejeição, a precedência da reversão vencedora, a ausência de lançamentos para LOSS/rejeições e as contagens exatas de transações, inbox e outbox. Todos os eventos confirmados precisam ter recibos persistentes. Replays posteriores devem conservar o resultado original.

## Contratos HTTP

Consultas foram exercitadas com barras codificadas, percentuais literais, pontuação de query, aspas, contrabarra, espaços e Unicode composto/decomposto, além de emojis no limite de comprimento. Chaves idempotentes de 256 caracteres funcionam; 257 retornam 400. Dinheiro com zeros à esquerda e UUIDs em maiúsculas preservam o replay da entrada canônica, sem novo crédito. Os cenários passaram sem nova correção de produção.

## Sincronização do teste de eventos

As primeiras execuções locais passaram nos dois ambientes. A [primeira CI desta rodada](https://github.com/EduardoPaim5/jungle-backend-challenge/actions/runs/37831483580) revelou uma espera frágil no teste existente de publishers: em MiniStack, a assertion exigia um recibo específico depois de apenas 100 ms, embora a condição anterior garantisse somente que algum evento já havia sido consumido. Essa execução teve 41 integrações aprovadas e uma falha; LocalStack passou também na prova adicional de recuperação.

O teste agora aguarda o recibo do eventId específico e observa confirmações SQS bem-sucedidas para pelo menos três MessageIds distintos desse evento: a publicação original e duas cópias com dedupIds diferentes. Só depois exige exatamente um recibo no PostgreSQL. Um proxy de teste encaminha chamadas ao broker real e atrasa a entrega desse evento em 250 ms, exercitando uma latência maior que a espera anterior. O consumo completo das cópias é comprovado, sem depender de um sleep curto ou relaxar a unicidade esperada. O proxy e os processos participantes são encerrados em finally.

## Persistência dos brokers

`bun run test:recovery` cria um projeto e volume próprios, usando a mesma imagem fixada e configuração de persistência do Compose. As filas são criadas uma vez. Cada ciclo envia 12 mensagens únicas para entrada, eventos e DLQ; cinco mensagens da entrada são recebidas sem ack antes do encerramento. Os ciclos 2 a 5 mantêm quatro long polls de 20 segundos ativos em uma fila vazia durante o shutdown.

Depois de cada reinício, um novo cliente SDK resolve as filas e verifica as mensagens. A prova não recria filas nem força snapshots ou restauração manual. Todas as 36 mensagens confirmadas pelo envio precisam ser recuperadas, incluindo as recebidas sem ack. A limpeza remove somente o projeto exclusivo do experimento.

| Broker     | Reinícios | Mensagens recuperadas | Exit code  | OOM | Encerramento observado |
| ---------- | --------: | --------------------: | ---------- | --- | ---------------------- |
| LocalStack |         5 |               180/180 | 0 em todos | Não | 2,11–4,80 s            |
| MiniStack  |         5 |               180/180 | 0 em todos | Não | 0,98–20,44 s           |

Relatórios: [LocalStack](recovery-localstack.json) e [MiniStack](recovery-ministack.json). O MiniStack esperou os long polls terminarem, dentro dos 30 segundos de grace period configurados. O script redescobre a porta dinâmica de Docker depois de cada reinício; portas efêmeras podem mudar quando um container parado inicia novamente.

O incidente de restauração relatado na [revisão anterior](pre-delivery.md) não se reproduziu nesses experimentos. A causa original continua não confirmada; resultados positivos posteriores não devem ser apresentados como uma correção dessa causa. Os logs de cada ciclo e o relatório de falha são preservados em `artifacts` para diagnóstico caso o problema reapareça. Esses resultados cobrem reinício gracioso, sem prometer durabilidade do emulador após SIGKILL.

## Ambiente novo

`bun run test:isolated` provisiona PostgreSQL e broker em um projeto Compose exclusivo, com volumes novos e portas fixas próprias. A suíte inteira executa migrations up/down/up, três APIs e os testes de parada/reinício reais contra esse projeto; o ambiente de desenvolvimento permanece em execução. Os recursos exclusivos são removidos ao terminar. Os resultados registrados estão em [LocalStack](isolated-localstack.json) e [MiniStack](isolated-ministack.json).

Não foi identificado novo defeito de produção nesta rodada. As provas aumentam a cobertura e a capacidade de reproduzir/diagnosticar falhas; não demonstram todas as combinações possíveis ou capacidade de produção.
