import { withInstances, open, command, submit, request } from './runtime.js';
import { Queues } from '../src/infrastructure/sqs.js';
import { randomUUID } from 'node:crypto';
import type { ProcessingResult } from '../src/application/contracts.js';
await withInstances(3, 'api,consumer,references,publisher,dlq,events', async (urls) => {
  const wallet = await open(urls[0]!);
  const bet = command(wallet, 'BET', '25.00');
  const result = await submit(urls[0]!, bet);
  const replay = await submit(urls[1]!, bet);
  const q = new Queues();
  try {
    const futureBet = command(wallet, 'BET', '20.00');
    const refund = command(wallet, 'REFUND', '20.00', futureBet.externalTransactionId);
    const pending = await submit(urls[2]!, refund);
    await q.send(
      q.names.input,
      JSON.stringify({
        messageId: randomUUID(),
        type: 'WagerTransactionRequested',
        occurredAt: new Date().toISOString(),
        data: futureBet,
      }),
      wallet.id,
    );
    const deadline = Date.now() + 15000;
    let resolved: ProcessingResult;
    do {
      resolved = await request<ProcessingResult>(urls[0]!, `/wagering/transactions/${pending.transactionId}`);
      if (resolved.status === 'PROCESSED') break;
      await Bun.sleep(100);
    } while (Date.now() < deadline);
    if (resolved.status !== 'PROCESSED') throw new Error('Pending reference did not resolve');
    const reconciliation = await request<{ consistent: boolean }>(
      urls[0]!,
      `/wallets/${wallet.id}/reconciliation`,
      {},
    );
    if (!reconciliation.consistent) throw new Error('Reconciliation failed');
    console.log(
      JSON.stringify(
        {
          scenario: 'opening, BET, replay in another process, out-of-order REFUND, SQS BET, reconciliation',
          instances: urls.length,
          walletId: wallet.id,
          processed: result,
          replay,
          pending,
          resolved,
          reconciliation,
        },
        null,
        2,
      ),
    );
  } finally {
    q.close();
  }
});
