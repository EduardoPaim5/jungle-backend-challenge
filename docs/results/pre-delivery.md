# Revisão pré-entrega — 8 de outubro de 2026

Referência do desafio: commit `c7143e6d041585d6913f56eb7415105e074f83dc`, reconfirmado nesta rodada. A revisão começou na implementação publicada em `493318a` e acrescentou as correções e provas descritas abaixo.

## Correções encontradas na revisão

A API aceitava identificadores de provedor/transação externa com 128 caracteres, mas o limite padrão do roteador recusava a consulta correspondente com HTTP 414. O limite de transporte agora permite alcançar a validação de negócio; identificadores ASCII e multibyte no limite do contrato são recuperados corretamente.

A consulta por identidade externa também passa pela mesma validação dos identificadores de entrada. Caracteres de controle, identificadores compostos apenas de espaços e tamanho acima de 128 retornam HTTP 400, com `INVALID_REQUEST`; uma identidade válida inexistente continua retornando 404. Os dois testes de regressão falharam antes da correção e passaram depois. O inventário foi atualizado para **25 testes unitários e 38 de integração**.

## Evidências

- Checkout limpo obtido do repositório público: instalação com `--frozen-lockfile`, compilação e demo com três processos reais aprovados.
- `TEST_BROKER=localstack bun run verify`: 63 testes aprovados, nenhuma falha ou cenário omitido na execução concluída, incluindo reinícios reais de PostgreSQL e SQS.
- Imagem Docker com dependências de produção: build aprovado; três containers executando como `bun`, conectados ao PostgreSQL como `jungle_app`. Demo HTTP/SQS, replay entre instâncias, REFUND fora de ordem e reconciliação aprovados. Consultas multibyte de 128 caracteres retornaram 200; identidade inválida retornou 400; health, OpenAPI e métricas disponíveis.
- Verificação SQL do banco de desenvolvimento: nenhuma wallet divergente ou negativa, nenhuma versão divergente e nenhuma decisão incompatível com o ledger. O papel da aplicação não é superuser e não possui UPDATE, DELETE ou TRUNCATE no ledger.
- `bun audit`: nenhuma vulnerabilidade conhecida reportada em 155 pacotes. Token LocalStack ausente dos arquivos versionados e dos patches do histórico; arquivos de credenciais ignorados pelo Git e pelo contexto Docker.
- `bun run test:load`: 3.800 requisições incluindo aquecimento, três processos, concorrência 24 e três repetições por topologia. Zero erros técnicos; todas as verificações de consistência aprovadas. A outbox terminou com zero pendências após 2,03 s de drenagem. O [relatório completo](load-pre-delivery.json) conserva ambiente, metodologia, percentis, rejeições esperadas e medições de lock/outbox.

Nesta rodada, o throughput observado variou de 116,9 a 155,4 req/s para uma wallet e de 476,8 a 584,0 req/s para 24 wallets. O lag máximo amostrado foi 2,23 s; houve backlog durante a ingestão, com pico observado de 1.137 eventos pendentes. A máquina é compartilhada e os números não são limites garantidos. Não houve timeouts de lock ou retries de banco nesta carga; os cenários adversariais da suíte exercitam esses caminhos separadamente.

As execuções remotas completas, com MiniStack e LocalStack, estão na [aba Actions](https://github.com/EduardoPaim5/jungle-backend-challenge/actions). A matriz de requisitos identifica as provas financeiras, de concorrência, idempotência e recuperação.

## Incidente observado no emulador

Uma tentativa local da suíte falhou porque as filas isoladas de teste não estavam disponíveis depois do reinício do LocalStack. Dois cenários posteriores falharam como consequência. A causa exata dessa restauração não foi confirmada; o incidente não deve ser apresentado como corrigido por uma mudança na aplicação.

Um reinício isolado posterior encerrou o emulador com exit code 0, sem OOM, e preservou uma mensagem de controle. A reexecução completa passou com o mesmo código e configuração. As invariantes SQL permaneceram consistentes e as filas principais continuaram disponíveis. Não foram recriadas filas para fazer o cenário de persistência passar, e não houve alteração das assertions ou exclusão do teste.

A durabilidade externa depende do commit PostgreSQL e da confirmação durável do broker. Os snapshots do emulador e seus limites estão descritos em ARCHITECTURE; os testes devem rodar com uso exclusivo desse ambiente. A CI em ambiente novo é evidência separada da máquina local compartilhada.

## Escopo da conclusão

A revisão não identificou falha financeira eliminatória. As limitações de autenticação, retenção, ordenação de eventos e durabilidade do emulador estão documentadas. A identidade permanece uma extensão de desenvolvimento deliberada, aceita pelo enunciado; expor a API em produção exige o adaptador OIDC e autorização descritos na arquitetura.

A aprovação técnica nesta rodada se apoia nos testes e evidências apresentados; não prevê a pontuação dos avaliadores nem capacidade de produção a partir do benchmark local.
