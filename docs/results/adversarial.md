# Validação adversarial adicional

Esta rodada acrescentou onze cenários de integração e uma prova de domínio à suíte original. Na execução registrada após o [tratamento da auditoria independente](audit-follow-up.md), a suíte continha **25 testes unitários e 34 de integração**, incluindo também payload divergente e guard global de identidade. O inventário atual, ampliado nas revisões posteriores, está na [matriz de requisitos](../REQUIREMENTS.md). Os testes continuam usando PostgreSQL e SQS reais e pelo menos três processos da aplicação.

| Cenário                        | Evidência verificada                                                                                                                                                                                                                                                           |
| ------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Contratos inválidos            | Valores monetários inválidos, tipos incorretos, identidade interna, UUIDs, referências obrigatórias/proibidas e header ausente são recusados sem efeito financeiro. BET/WIN/REFUND/ROLLBACK zeradas são rejeições persistidas.                                                 |
| Erros de transporte HTTP       | JSON malformado, mídia incompatível e payload acima do limite retornam 400/415/413, preservam correlação e não instruem retry de infraestrutura.                                                                                                                               |
| Precisão no caminho completo   | `9007199254740993.01` atravessa HTTP, ORM, PostgreSQL, ledger e replay sem perder centavos. UUIDs e dinheiro normalizados preservam o hash; o header é a fonte da chave.                                                                                                       |
| Identidades entre wallets      | Duas wallets disputando a mesma chave ou identidade externa produzem um sucesso e um conflito; provedores distintos podem usar o mesmo ID externo com chaves diferentes.                                                                                                       |
| Referências concorrentes       | WIN, duas REFUNDs e ROLLBACK chegam antes da BET. Dois workers finalizam as operações, somente uma REFUND é aplicada e cada evento pendente aparece uma vez.                                                                                                                   |
| Worker antigo de referências   | Uma barreira pausa o worker depois da claim, sem manter lock de wallet. Depois de expirar a lease, o sucessor processa a operação; o antigo retoma sem alterar a decisão terminal ou o saldo original.                                                                         |
| Consultas e cursor             | As duas consultas de transação preservam o resultado original. Recursos ausentes retornam 404; UUID, limites e cursor inválidos ou de outra wallet retornam 400.                                                                                                               |
| Reconciliação concorrente      | Uma barreira separa a leitura da wallet da soma do ledger. Outra instância confirma uma BET nesse intervalo; REPEATABLE READ mantém ambas as leituras no mesmo snapshot.                                                                                                       |
| Resposta HTTP perdida          | O processo morre depois do commit e antes da resposta. Reenviar a mesma chave em outra instância recupera a decisão original, mesmo após outra movimentação.                                                                                                                   |
| Identidade inválida no SQS     | Envelopes com messageId contendo controles, somente espaços, tamanho excessivo ou Unicode malformado são enviados à DLQ, auditados com identidade segura do broker e confirmados, sem transação financeira ou alteração de saldo/ledger.                                       |
| Modelo financeiro independente | 120 operações geradas com seed `0x4a554e47`, uma BET inicial e 14 replays são comparados com um oráculo de centavos em bigint, sem usar os cálculos do domínio/aplicação. Saldo, versões, cada linha do ledger, códigos de rejeição e contagens de eventos precisam coincidir. |

O caso com `messageId` contendo NUL reproduziu uma falha antes da correção: o identificador inválido chegava ao PostgreSQL e também impedia a auditoria da DLQ. A validação agora ocorre antes do processamento financeiro; o auditor aceita a identidade do envelope somente quando ela é válida, conservando a identidade segura de transporte como alternativa. Os identificadores de negócio e chaves idempotentes também recusam Unicode malformado para impedir conversão silenciosa durante a persistência.

O teste de domínio isola conflitos de provedor, jogador, wallet, moeda e rodada em referências. As barreiras adicionais ficam desabilitadas fora de `NODE_ENV=test` e não têm endpoints HTTP de controle.

Os cenários estão no final de `tests/integration/system.test.ts`; a prova de domínio está em `tests/unit/domain.test.ts`. Para reproduzir a suíte completa, use os comandos do README com `TEST_BROKER=localstack` ou `ministack`. Para executar somente o oráculo:

```bash
bun run build
bun test tests/integration/system.test.ts --test-name-pattern 'modelo financeiro independente' --timeout 120000
```

Essas provas aumentam a cobertura de combinações de estados e falhas. Não representam testes de todas as combinações possíveis nem capacidade de produção.
