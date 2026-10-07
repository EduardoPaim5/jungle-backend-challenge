# Entrega e apresentação

## E-mail de entrega — rascunho

**Assunto:** Teste técnico Backend Developer — Eduardo Paim

Olá, equipe Jungle Gaming!

Segue minha implementação do desafio técnico:

https://github.com/EduardoPaim5/jungle-backend-challenge

O README contém setup, demo com três processos, comandos de teste e exemplos HTTP/SQS. ARCHITECTURE explica as garantias financeiras, decisões e limitações; a matriz de requisitos e os resultados de validação estão em docs.

As validações automatizadas estão disponíveis na aba Actions do repositório.

Fico à disposição para apresentar a solução e discutir as decisões técnicas.

Atenciosamente,
Eduardo Paim

O envio deve ser feito pelo candidato para o endereço do recrutamento. Nenhum e-mail é enviado automaticamente pela aplicação ou pelos scripts. O prazo informado na mensagem é de três dias a partir do envio do convite.

## Roteiro de apresentação — 15 minutos

1. **Problema e contrato (2 min):** dinheiro exato, tipos de operação, referência e resultados persistidos. Mostre abertura e BET no Swagger.
2. **Commit e concorrência (4 min):** wallet lock, únicos, ledger/inbox/outbox na mesma transação. Explique por que FIFO não protege HTTP e por que três instâncias precisam de garantias no banco. Mostre a corrida das duas BETs de 80.
3. **Idempotência (2 min):** o hash não inclui transporte/chave; replay retorna saldo original, não atual. Mostre um replay após WIN e um conflito 409.
4. **Mensageria e recuperação (4 min):** pendente recebe ack para liberar referência; crash depois de commit antes de ack; outbox at least once, eventId e fencing. Mostre um teste de SIGKILL e o consumidor de eventos.
5. **Diagnóstico e escolhas (3 min):** reconciliação, métricas/health, carga concentrada/distribuída, limites de emuladores e extensão OIDC. Use os dados registrados; não apresente RPS local como capacidade de produção.

## Perguntas que precisam de domínio real

- O que distingue REJECTED de FAILED e de erro transitório? Por que não registrar FAILED só por esgotar redelivery?
- Por que uma mesma BET pode ter uma REFUND e um ROLLBACK neste contrato? Onde isso está no enunciado?
- Como um commit confirmado pode gerar resposta 503? Como o cliente resolve a ambiguidade?
- Por que a outbox pode publicar duplicado após um crash? Como o consumidor evita repetir o efeito?
- O que acontece se uma referência chega antes da BET? Qual saldo um replay pendente devolve?
- Qual lock é obtido primeiro? Por que o worker de referência libera sua claim antes de executar finanças?
- Que corrupção os triggers impedem? O que um administrador PostgreSQL ainda pode fazer?
- O que precisaria mudar para expor essa API em produção: OIDC, autorização, TLS/secrets, retenção e SLOs?

Estude o código e execute as provas antes da entrevista de apresentação. A implementação e seus limites devem poder ser explicados com suas próprias palavras.
