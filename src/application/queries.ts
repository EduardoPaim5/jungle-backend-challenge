import { z } from 'zod';
import { Database, WalletSchema } from '../infrastructure/database.js';
import { Observability, logger } from '../infrastructure/observability.js';
import { Money } from '../domain/money.js';
import { assertExternalIdentity, ServiceError } from './contracts.js';
import type { TransactionRow } from './wagering.js';
import { fault } from '../infrastructure/faults.js';

export function uuid(value: string): string {
  if (!z.uuid().safeParse(value).success) throw new ServiceError('INVALID_REQUEST', 400);
  return value.toLowerCase();
}
export class Queries {
  constructor(
    private readonly db: Database,
    private readonly metrics: Observability,
  ) {}
  async wallet(id: string) {
    const r = await this.db.orm.em.fork().findOne(WalletSchema, { id: uuid(id) });
    if (!r) throw new ServiceError('WALLET_NOT_FOUND', 404);
    return {
      id: r.id,
      playerId: r.playerId,
      balance: { amount: r.balance, currency: r.currency },
      version: r.version,
    };
  }
  async transaction(id: string) {
    return this.lookup('id=?', [uuid(id)]);
  }
  async external(provider: string, external: string) {
    assertExternalIdentity(provider, external);
    return this.lookup('provider_id=? AND external_transaction_id=?', [provider, external]);
  }
  private async lookup(where: string, params: string[]) {
    const [r] = await this.db.query<TransactionRow>(
      `SELECT * FROM wager_transactions WHERE ${where}`,
      params,
    );
    if (!r) throw new ServiceError('TRANSACTION_NOT_FOUND', 404);
    return {
      ...r.result_snapshot,
      providerId: r.provider_id,
      externalTransactionId: r.external_transaction_id,
      walletId: r.wallet_id,
      playerId: r.player_id,
      roundId: r.round_id,
      gameId: r.game_id,
      kind: r.kind,
      money: { amount: r.amount, currency: r.currency },
      referenceExternalTransactionId: r.reference_external_transaction_id,
      referenceTransactionId: r.reference_transaction_id,
      createdAt: new Date(r.created_at).toISOString(),
      processedAt: r.processed_at ? new Date(r.processed_at).toISOString() : null,
    };
  }
  async ledger(id: string, cursor: string | undefined, rawLimit: string | undefined) {
    id = uuid(id);
    const wallet = await this.wallet(id);
    const limit = rawLimit === undefined ? 50 : Number(rawLimit);
    if (!Number.isInteger(limit) || limit < 1 || limit > 100) throw new ServiceError('INVALID_CURSOR', 400);
    let upper = wallet.version,
      last = wallet.version + 1;
    if (cursor) {
      try {
        const p = z
          .object({
            v: z.literal(1),
            walletId: z.literal(id),
            upper: z.number().int().positive(),
            last: z.number().int().positive(),
          })
          .parse(JSON.parse(Buffer.from(cursor, 'base64url').toString()));
        if (p.upper > wallet.version || p.last > p.upper + 1) throw new Error();
        upper = p.upper;
        last = p.last;
      } catch {
        throw new ServiceError('INVALID_CURSOR', 400);
      }
    }
    const rows = await this.db.query<{
      id: string;
      transaction_id: string;
      direction: string;
      amount: string;
      currency: string;
      balance_before: string;
      balance_after: string;
      wallet_version: number;
      created_at: Date;
    }>(
      'SELECT * FROM wallet_ledger WHERE wallet_id=? AND wallet_version<=? AND wallet_version<? ORDER BY wallet_version DESC LIMIT ?',
      [id, upper, last, limit + 1],
    );
    const page = rows.slice(0, limit);
    return {
      items: page.map((r) => ({
        id: r.id,
        walletId: id,
        transactionId: r.transaction_id,
        direction: r.direction,
        money: { amount: r.amount, currency: r.currency },
        balanceBefore: { amount: r.balance_before, currency: r.currency },
        balanceAfter: { amount: r.balance_after, currency: r.currency },
        walletVersion: r.wallet_version,
        createdAt: new Date(r.created_at).toISOString(),
      })),
      nextCursor:
        rows.length > limit
          ? Buffer.from(
              JSON.stringify({ v: 1, walletId: id, upper, last: page.at(-1)!.wallet_version }),
            ).toString('base64url')
          : null,
    };
  }
  async reconciliation(id: string, correlationId: string) {
    uuid(id);
    const result = await this.db.transaction(async (em) => {
      const [w] = await this.db.query<{ balance: string; currency: string }>(
        'SELECT balance,currency FROM wallets WHERE id=?',
        [id],
        em,
      );
      if (!w) throw new ServiceError('WALLET_NOT_FOUND', 404);
      await fault('reconciliation-after-wallet');
      const [sum] = await this.db.query<{ balance: string; count: string }>(
        `SELECT COALESCE(sum(CASE direction WHEN 'CREDIT' THEN amount ELSE -amount END),0.00)::text as balance,
        count(*)::text as count FROM wallet_ledger WHERE wallet_id=?`,
        [id],
        em,
      );
      const stored = Money.rehydrate({ amount: w.balance, currency: w.currency });
      const calculated = Money.rehydrate({ amount: sum!.balance, currency: w.currency });
      return {
        walletId: id,
        storedBalance: stored.toJSON(),
        calculatedBalance: calculated.toJSON(),
        difference: stored.subtract(calculated).toJSON(),
        consistent: stored.equals(calculated),
        checkedEntries: Number(sum!.count),
      };
    }, true);
    if (!result.consistent) {
      this.metrics.reconciliations.inc();
      logger.error({ correlationId, walletId: id }, 'reconciliation_divergence');
    }
    return result;
  }
}
