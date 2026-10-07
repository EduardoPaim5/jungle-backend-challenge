import { randomUUID } from 'node:crypto';
import type { EntityManager } from '@mikro-orm/postgresql';
import { Database, sqlState, WalletSchema } from '../infrastructure/database.js';
import { Observability, logger } from '../infrastructure/observability.js';
import { fault } from '../infrastructure/faults.js';
import { DomainError } from '../domain/errors.js';
import { Money } from '../domain/money.js';
import { Wallet } from '../domain/wallet.js';
import {
  WagerTransaction,
  type WagerCommand,
  type TransactionStatus,
  type WagerKind,
} from '../domain/transaction.js';
import { WalletLedgerEntry } from '../domain/ledger.js';
import { InboxMessage, OutboxMessage } from '../domain/messaging.js';
import {
  WalletBalanceChanged,
  WagerTransactionProcessed,
  WagerTransactionRejected,
  WagerTransactionPendingReference,
  WagerTransactionFailed,
  type IntegrationEvent,
  type TransactionEventData,
} from '../domain/events.js';
import {
  businessHash,
  hash,
  normalizeMoney,
  walletInput,
  ServiceError,
  type ProcessingContext,
  type ProcessingResult,
} from './contracts.js';

export interface TransactionRow {
  id: string;
  provider_id: string;
  external_transaction_id: string;
  idempotency_key: string;
  payload_hash: string;
  wallet_id: string;
  player_id: string;
  round_id: string;
  game_id: string;
  kind: WagerKind;
  amount: string;
  currency: string;
  reference_external_transaction_id: string | null;
  reference_transaction_id: string | null;
  created_at: Date;
  status: TransactionStatus;
  failure_code: string | null;
  processed_at: Date | null;
  result_snapshot: ProcessingResult | null;
  correlation_id: string;
  causation_id: string | null;
  reference_attempts: number;
  lease_token: string | null;
}
export function rehydrateTransaction(r: TransactionRow): WagerTransaction {
  return WagerTransaction.rehydrate({
    id: r.id,
    providerId: r.provider_id,
    externalTransactionId: r.external_transaction_id,
    idempotencyKey: r.idempotency_key,
    payloadHash: r.payload_hash,
    playerId: r.player_id,
    walletId: r.wallet_id,
    roundId: r.round_id,
    gameId: r.game_id,
    kind: r.kind,
    money: Money.rehydrate({ amount: r.amount, currency: r.currency }),
    referenceExternalTransactionId: r.reference_external_transaction_id ?? undefined,
    createdAt: new Date(r.created_at),
    status: r.status,
    referenceTransactionId: r.reference_transaction_id ?? undefined,
    failureCode: r.failure_code ?? undefined,
    processedAt: r.processed_at ? new Date(r.processed_at) : undefined,
  });
}
export function commandFromRow(r: TransactionRow): WagerCommand {
  if (r.kind === 'OPENING') throw new Error('Internal operation cannot be retried as provider input');
  return {
    providerId: r.provider_id,
    externalTransactionId: r.external_transaction_id,
    idempotencyKey: r.idempotency_key,
    walletId: r.wallet_id,
    playerId: r.player_id,
    roundId: r.round_id,
    gameId: r.game_id,
    kind: r.kind,
    money: { amount: r.amount, currency: r.currency },
    ...(r.reference_external_transaction_id
      ? { referenceExternalTransactionId: r.reference_external_transaction_id }
      : {}),
  };
}
export function transient(error: unknown): boolean {
  return (
    [
      '40P01',
      '40001',
      '55P03',
      '57014',
      '08006',
      '08003',
      '57P01',
      'ECONNREFUSED',
      'ECONNRESET',
      'ETIMEDOUT',
    ].includes(sqlState(error) ?? '') ||
    (error instanceof Error && /connection|connect ECONN|timeout/i.test(error.message))
  );
}
export class Wagering {
  constructor(
    public readonly db: Database,
    public readonly metrics: Observability,
  ) {}

  async openWallet(body: unknown, correlationId: string) {
    const parsed = walletInput.safeParse(body);
    if (!parsed.success) throw new ServiceError('INVALID_REQUEST', 400);
    const money = Money.from(normalizeMoney(parsed.data.initialBalance));
    try {
      return await this.db.transaction(async (em) => {
        const at = await this.now(em);
        const wallet = Wallet.open(
          { id: randomUUID(), playerId: parsed.data.playerId.toLowerCase(), initialBalance: money },
          at,
        );
        const record = em.create(WalletSchema, {
          id: wallet.id,
          playerId: wallet.playerId,
          currency: wallet.currency,
          balance: wallet.balance.toJSON().amount,
          version: wallet.version,
          createdAt: at,
          updatedAt: at,
        });
        em.persist(record);
        await em.flush();
        if (money.isPositive()) {
          const id = randomUUID();
          const result: ProcessingResult = {
            transactionId: id,
            status: 'PROCESSED',
            balance: money.toJSON(),
            walletVersion: 1,
            idempotentReplay: false,
          };
          await this.db.query(
            `INSERT INTO wager_transactions(id,provider_id,external_transaction_id,idempotency_key,payload_hash,
            wallet_id,player_id,round_id,game_id,kind,amount,currency,status,created_at,processed_at,result_snapshot,correlation_id)
            VALUES (?,'__system__',?,?,?, ?,?,'__opening__','__opening__','OPENING',?,?,'PROCESSED',?,?,?::jsonb,?)`,
            [
              id,
              wallet.id,
              `__system__:opening:${wallet.id}`,
              hash({ walletId: wallet.id, money: money.toJSON() }),
              wallet.id,
              wallet.playerId,
              money.toJSON().amount,
              money.currency,
              at,
              at,
              JSON.stringify(result),
              correlationId,
            ],
            em,
          );
          const entry = WalletLedgerEntry.create({
            id: randomUUID(),
            walletId: wallet.id,
            transactionId: id,
            direction: 'CREDIT',
            money,
            balanceBefore: Money.zero(wallet.currency),
            balanceAfter: money,
            walletVersion: 1,
            createdAt: at,
          });
          await this.insertLedger(entry, em);
          await this.enqueue(
            WagerTransactionProcessed.create({
              eventId: randomUUID(),
              aggregateId: id,
              correlationId,
              occurredAt: at,
              data: {
                transactionId: id,
                walletId: wallet.id,
                providerId: '__system__',
                externalTransactionId: wallet.id,
                kind: 'OPENING',
                status: 'PROCESSED',
                money: money.toJSON(),
                balance: money.toJSON(),
                walletVersion: 1,
              },
            }),
            wallet.id,
            em,
          );
          await this.balanceEvent(entry, correlationId, undefined, em);
        }
        await fault('before-commit');
        return {
          id: wallet.id,
          playerId: wallet.playerId,
          balance: wallet.balance.toJSON(),
          version: wallet.version,
        };
      });
    } catch (e) {
      if (sqlState(e) === '23505') throw new ServiceError('WALLET_ALREADY_EXISTS', 409);
      if (transient(e)) throw new ServiceError('INFRASTRUCTURE_UNAVAILABLE', 503, true);
      throw e;
    }
  }

  async process(command: WagerCommand, context: ProcessingContext): Promise<ProcessingResult> {
    const end = this.metrics.latency.startTimer({ source: context.source });
    try {
      for (let attempt = 0; ; attempt++) {
        try {
          // A committed HTTP replay needs no wallet lock: the persisted decision is authoritative.
          let result: ProcessingResult | undefined;
          if (!context.inbox && !context.referenceLease && !context.permanentFailureCode) {
            const [cached] = await this.db.query<TransactionRow>(
              'SELECT * FROM wager_transactions WHERE idempotency_key=?',
              [command.idempotencyKey],
            );
            if (cached) {
              if (cached.payload_hash !== businessHash(command))
                throw new ServiceError('IDEMPOTENCY_CONFLICT', 409);
              if (!cached.result_snapshot) throw new Error('Committed transaction has no snapshot');
              result = { ...cached.result_snapshot, idempotentReplay: true };
            }
          }
          result ??= await this.db.transaction((em) => this.apply(command, context, em));
          if (result.idempotentReplay) this.metrics.duplicates.inc({ source: context.source });
          else this.metrics.transactions.inc({ status: result.status, kind: command.kind });
          logger.info(
            {
              correlationId: context.correlationId,
              messageId: context.inbox?.messageId,
              transactionId: result.transactionId,
              walletId: command.walletId,
              providerId: command.providerId,
              status: result.status,
              replay: result.idempotentReplay,
            },
            'wager_decided',
          );
          await fault('after-commit', {
            messageId: context.inbox?.messageId,
            transactionId: result.transactionId,
          });
          return result;
        } catch (e) {
          if (!transient(e)) throw e;
          if (['55P03', '40P01', '40001'].includes(sqlState(e) ?? '')) this.metrics.locks.inc();
          if (attempt >= 2) throw new ServiceError('INFRASTRUCTURE_UNAVAILABLE', 503, true);
          this.metrics.retries.inc({ component: 'database' });
          await Bun.sleep(25 * 2 ** attempt + Math.random() * 25);
        }
      }
    } finally {
      end();
    }
  }

  private async apply(
    command: WagerCommand,
    ctx: ProcessingContext,
    em: EntityManager,
  ): Promise<ProcessingResult> {
    const payloadHash = businessHash(command);
    const stopLock = this.metrics.lockWait.startTimer();
    let locked: Awaited<ReturnType<Database['lockWallet']>>;
    try {
      locked = await this.db.lockWallet(command.walletId, em);
    } finally {
      stopLock();
    }
    if (ctx.inbox) {
      await this.db.query(
        `INSERT INTO inbox_messages(consumer_name,message_id,payload_hash) VALUES ('wager-requests-v1',?,?) ON CONFLICT DO NOTHING`,
        [ctx.inbox.messageId, ctx.inbox.payloadHash],
        em,
      );
      const [inbox] = await this.db.query<{ payload_hash: string }>(
        `SELECT payload_hash FROM inbox_messages WHERE consumer_name='wager-requests-v1' AND message_id=? FOR UPDATE`,
        [ctx.inbox.messageId],
        em,
      );
      if (inbox?.payload_hash !== ctx.inbox.payloadHash)
        throw new ServiceError('INBOX_PAYLOAD_CONFLICT', 409);
    }
    const at = await this.now(em);
    const inserted = await this.db.query<TransactionRow>(
      `INSERT INTO wager_transactions(id,provider_id,external_transaction_id,
      idempotency_key,payload_hash,wallet_id,player_id,round_id,game_id,kind,amount,currency,reference_external_transaction_id,
      status,created_at,correlation_id,causation_id) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,'PENDING',?,?,?) ON CONFLICT DO NOTHING RETURNING *`,
      [
        randomUUID(),
        command.providerId,
        command.externalTransactionId,
        command.idempotencyKey,
        payloadHash,
        command.walletId,
        command.playerId,
        command.roundId,
        command.gameId,
        command.kind,
        command.money.amount,
        command.money.currency,
        command.referenceExternalTransactionId ?? null,
        at,
        ctx.correlationId,
        ctx.causationId ?? null,
      ],
      em,
    );
    let row = inserted[0];
    if (!row) {
      const [existing] = await this.db.query<TransactionRow>(
        'SELECT * FROM wager_transactions WHERE idempotency_key=? FOR UPDATE',
        [command.idempotencyKey],
        em,
      );
      if (!existing) throw new ServiceError('EXTERNAL_TRANSACTION_CONFLICT', 409);
      if (existing.payload_hash !== payloadHash) throw new ServiceError('IDEMPOTENCY_CONFLICT', 409);
      row = existing;
      if (
        (!ctx.referenceLease && !ctx.permanentFailureCode) ||
        ['PROCESSED', 'REJECTED', 'FAILED'].includes(row.status)
      ) {
        if (!row.result_snapshot) throw new Error('Committed transaction has no snapshot');
        await this.finishInbox(ctx, row.id, em);
        return { ...row.result_snapshot, idempotentReplay: true };
      }
    }
    if (ctx.referenceLease) {
      const owned = await this.db.query(
        `SELECT id FROM wager_transactions WHERE id=? AND lease_token=? AND lease_until>clock_timestamp()`,
        [row.id, ctx.referenceLease.token],
        em,
      );
      if (!owned.length || ctx.referenceLease.transactionId !== row.id)
        throw new ServiceError('LEASE_LOST', 409);
    }
    const tx = rehydrateTransaction(row);
    const before = locked?.wallet.balance;
    let entry: WalletLedgerEntry | undefined;
    let reference: WagerTransaction | undefined;
    try {
      if (!locked) throw new DomainError('WALLET_NOT_FOUND');
      if (locked.wallet.playerId !== command.playerId) throw new DomainError('PLAYER_MISMATCH');
      if (locked.wallet.currency !== command.money.currency) throw new DomainError('CURRENCY_MISMATCH');
      if (tx.affectsBalance() && !tx.money.isPositive()) throw new DomainError('AMOUNT_MUST_BE_POSITIVE');
      if (ctx.permanentFailureCode) tx.fail(ctx.permanentFailureCode);
      else {
        if (command.referenceExternalTransactionId) {
          const [r] = await this.db.query<TransactionRow>(
            'SELECT * FROM wager_transactions WHERE provider_id=? AND external_transaction_id=?',
            [command.providerId, command.referenceExternalTransactionId],
            em,
          );
          if (r) {
            reference = rehydrateTransaction(r);
            tx.validateReference(reference);
          }
          if (!reference || reference.status !== 'PROCESSED') {
            if (
              at.getTime() - new Date(row.created_at).getTime() >=
              Number(process.env.REFERENCE_TTL_SECONDS ?? 86400) * 1000
            )
              throw new DomainError(reference ? 'REFERENCE_NOT_PROCESSED' : 'REFERENCE_NOT_FOUND');
            tx.markPendingReference();
          }
        }
        if (tx.status !== 'PENDING_REFERENCE' || reference?.status === 'PROCESSED') {
          if (tx.requiresReference() && reference) {
            const reversals = await this.db.query(
              "SELECT id FROM wager_transactions WHERE reference_transaction_id=? AND kind=? AND status='PROCESSED'",
              [reference.id, tx.kind],
              em,
            );
            if (reversals.length) throw new DomainError('REVERSAL_ALREADY_APPLIED');
          }
          if (tx.affectsBalance()) {
            const direction = tx.ledgerDirectionFor(reference);
            if (direction === 'DEBIT')
              locked.wallet.debit(
                tx.money,
                tx.kind === 'ROLLBACK' ? 'REVERSAL_INSUFFICIENT_FUNDS' : 'INSUFFICIENT_FUNDS',
                at,
              );
            else locked.wallet.credit(tx.money, at);
            entry = WalletLedgerEntry.create({
              id: randomUUID(),
              walletId: locked.wallet.id,
              transactionId: tx.id,
              direction,
              money: tx.money,
              balanceBefore: before!,
              balanceAfter: locked.wallet.balance,
              walletVersion: locked.wallet.version,
              createdAt: at,
            });
          }
          tx.markProcessed(reference?.id, at);
        }
      }
    } catch (e) {
      if (
        !(e instanceof DomainError) ||
        e.code === 'UNBALANCED_LEDGER' ||
        e.code === 'INVALID_TRANSACTION_STATE'
      )
        throw e;
      tx.reject(e.code);
    }
    const result: ProcessingResult = {
      transactionId: tx.id,
      status: tx.status,
      idempotentReplay: false,
      ...(locked ? { balance: locked.wallet.balance.toJSON(), walletVersion: locked.wallet.version } : {}),
      ...(tx.failureCode ? { failureCode: tx.failureCode } : {}),
    };
    const saved = tx.status === 'PENDING_REFERENCE' && row.result_snapshot ? row.result_snapshot : result;
    const delay =
      Math.max(1, Math.min(60, 2 ** Math.min(row.reference_attempts, 6))) *
      Number(process.env.REFERENCE_BACKOFF_MS ?? 1000);
    await this.db.query(
      `UPDATE wager_transactions SET status=?,failure_code=?,processed_at=?,reference_transaction_id=?,result_snapshot=?::jsonb,
      next_attempt_at=CASE WHEN ?='PENDING_REFERENCE' THEN clock_timestamp()+(? * interval '1 millisecond') ELSE NULL END,
      lease_token=NULL,lease_until=NULL WHERE id=?`,
      [
        tx.status,
        tx.failureCode ?? null,
        tx.processedAt ?? null,
        tx.referenceTransactionId ?? null,
        JSON.stringify(saved),
        tx.status,
        delay + Math.random() * delay * 0.1,
        tx.id,
      ],
      em,
    );
    if (entry && locked) {
      await this.insertLedger(entry, em);
      em.assign(locked.record, {
        balance: locked.wallet.balance.toJSON().amount,
        version: locked.wallet.version,
        updatedAt: locked.wallet.updatedAt,
      });
      await em.flush();
      await this.balanceEvent(entry, row.correlation_id, row.causation_id ?? undefined, em);
    }
    if (row.status !== tx.status) {
      const data: TransactionEventData = {
        transactionId: tx.id,
        walletId: command.walletId,
        providerId: command.providerId,
        externalTransactionId: command.externalTransactionId,
        kind: tx.kind,
        status: tx.status,
        money: tx.money.toJSON(),
        ...(result.balance ? { balance: result.balance } : {}),
        ...(result.walletVersion !== undefined ? { walletVersion: result.walletVersion } : {}),
        ...(tx.failureCode ? { failureCode: tx.failureCode } : {}),
      };
      const props = {
        eventId: randomUUID(),
        aggregateId: tx.id,
        correlationId: row.correlation_id,
        ...(row.causation_id ? { causationId: row.causation_id } : {}),
        occurredAt: at,
        data,
      };
      const event =
        tx.status === 'PROCESSED'
          ? WagerTransactionProcessed.create(props)
          : tx.status === 'REJECTED'
            ? WagerTransactionRejected.create(props)
            : tx.status === 'FAILED'
              ? WagerTransactionFailed.create(props)
              : WagerTransactionPendingReference.create(props);
      await this.enqueue(event, command.walletId, em);
    }
    await this.finishInbox(ctx, tx.id, em);
    await fault('before-commit', { messageId: ctx.inbox?.messageId, transactionId: tx.id });
    return saved;
  }

  private async finishInbox(ctx: ProcessingContext, id: string, em: EntityManager) {
    if (!ctx.inbox) return;
    const [record] = await this.db.query<{ received_at: Date; processed_at: Date | null }>(
      `SELECT received_at,processed_at FROM inbox_messages WHERE consumer_name='wager-requests-v1' AND message_id=?`,
      [ctx.inbox.messageId],
      em,
    );
    if (!record) throw new Error('Locked inbox missing');
    const inbox = InboxMessage.rehydrate({
      consumerName: 'wager-requests-v1',
      ...ctx.inbox,
      receivedAt: record.received_at,
      processedAt: record.processed_at ?? undefined,
    });
    if (!inbox.isProcessed()) inbox.markProcessed(await this.now(em));
    await this.db.query(
      `UPDATE inbox_messages SET processed_at=?,transaction_id=?,
      disposition=CASE WHEN ? THEN 'DLQ' ELSE disposition END
      WHERE consumer_name='wager-requests-v1' AND message_id=?`,
      [inbox.processedAt, id, !!ctx.permanentFailureCode, ctx.inbox.messageId],
      em,
    );
  }
  private async now(em: EntityManager): Promise<Date> {
    const [r] = await this.db.query<{ now: Date }>('SELECT clock_timestamp() as now', [], em);
    return new Date(r!.now);
  }
  private async insertLedger(e: WalletLedgerEntry, em: EntityManager) {
    await this.db.query(
      `INSERT INTO wallet_ledger(id,wallet_id,transaction_id,direction,amount,currency,balance_before,balance_after,wallet_version,created_at)
      VALUES (?,?,?,?,?,?,?,?,?,?)`,
      [
        e.id,
        e.walletId,
        e.transactionId,
        e.direction,
        e.money.toJSON().amount,
        e.money.currency,
        e.balanceBefore.toJSON().amount,
        e.balanceAfter.toJSON().amount,
        e.walletVersion,
        e.createdAt,
      ],
      em,
    );
  }
  private async balanceEvent(
    e: WalletLedgerEntry,
    correlationId: string,
    causationId: string | undefined,
    em: EntityManager,
  ) {
    await this.enqueue(
      WalletBalanceChanged.create({
        eventId: randomUUID(),
        aggregateId: e.walletId,
        correlationId,
        ...(causationId ? { causationId } : {}),
        occurredAt: e.createdAt,
        data: {
          walletId: e.walletId,
          transactionId: e.transactionId,
          direction: e.direction,
          money: e.money.toJSON(),
          balanceBefore: e.balanceBefore.toJSON(),
          balanceAfter: e.balanceAfter.toJSON(),
          walletVersion: e.walletVersion,
        },
      }),
      e.walletId,
      em,
    );
  }
  private async enqueue<T>(event: IntegrationEvent<T>, walletId: string, em: EntityManager) {
    const message = OutboxMessage.enqueue(event.toJSON());
    await this.db.query(
      'INSERT INTO outbox_messages(id,aggregate_id,wallet_id,event_type,payload,occurred_at) VALUES (?,?,?,?,?::jsonb,?)',
      [
        message.id,
        message.aggregateId,
        walletId,
        message.eventType,
        JSON.stringify(message.payload),
        message.occurredAt,
      ],
      em,
    );
  }
}
