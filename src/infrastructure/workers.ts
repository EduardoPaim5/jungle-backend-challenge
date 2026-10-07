import { randomUUID } from 'node:crypto';
import { z } from 'zod';
import type { Message } from '@aws-sdk/client-sqs';
import { Wagering, commandFromRow, transient, type TransactionRow } from '../application/wagering.js';
import { ServiceError, hash, parseCommand, type ProcessingContext } from '../application/contracts.js';
import { Database, sqlState } from './database.js';
import { Queues } from './sqs.js';
import { Observability, logger } from './observability.js';
import { fault } from './faults.js';

function code(error: unknown): string {
  return error instanceof ServiceError
    ? error.code
    : (sqlState(error) ?? (error instanceof Error ? error.name : 'UNKNOWN_ERROR'));
}
async function concurrent<T>(items: T[], limit: number, fn: (item: T) => Promise<void>) {
  const pending = [...items];
  await Promise.all(
    Array.from({ length: Math.min(limit, items.length) }, async () => {
      while (pending.length) {
        const item = pending.shift()!;
        await fn(item);
      }
    }),
  );
}
export abstract class LoopWorker {
  protected stopping = false;
  private task: Promise<void> | undefined;
  start(): void {
    this.task = this.loop();
  }
  protected abstract tick(): Promise<void>;
  protected abstract interval: number;
  private async loop() {
    while (!this.stopping) {
      try {
        await this.tick();
      } catch (e) {
        if (!this.stopping)
          logger.error({ component: this.constructor.name, errorCode: code(e) }, 'worker_cycle_failed');
      }
      if (!this.stopping) await Bun.sleep(this.interval);
    }
  }
  async stop(): Promise<void> {
    this.stopping = true;
    await Promise.race([this.task, Bun.sleep(Number(process.env.SHUTDOWN_GRACE_MS ?? 25000))]);
  }
}
interface OutboxRow {
  id: string;
  wallet_id: string;
  payload: Record<string, unknown>;
  attempts: number;
  lease_token: string;
}
export class Publisher extends LoopWorker {
  private hadWork = false;
  protected get interval(): number {
    return this.hadWork ? 0 : Number(process.env.OUTBOX_POLL_MS ?? 500);
  }
  constructor(
    private db: Database,
    private queues: Queues,
    private metrics: Observability,
  ) {
    super();
  }
  async tick(): Promise<void> {
    const token = randomUUID();
    const rows = await this.db.query<OutboxRow>(
      `WITH due AS (SELECT id FROM outbox_messages WHERE published_at IS NULL
      AND next_attempt_at<=clock_timestamp() AND (lease_until IS NULL OR lease_until<=clock_timestamp())
      ORDER BY next_attempt_at,occurred_at,id FOR UPDATE SKIP LOCKED LIMIT 20)
      UPDATE outbox_messages o SET lease_token=?,lease_until=clock_timestamp()+(?*interval '1 millisecond'),attempts=attempts+1
      FROM due WHERE o.id=due.id RETURNING o.*`,
      [token, Number(process.env.WORK_LEASE_MS ?? 60000)],
    );
    this.hadWork = rows.length > 0;
    await concurrent(rows, 5, async (row) => {
      try {
        await fault('publisher-before-send');
        const owned = await this.db.query(
          'SELECT id FROM outbox_messages WHERE id=? AND lease_token=? AND lease_until>clock_timestamp()',
          [row.id, token],
        );
        if (!owned.length) return;
        await this.queues.send(this.queues.names.events, JSON.stringify(row.payload), row.wallet_id, row.id);
        await fault('after-publish');
        await this.db.query(
          `UPDATE outbox_messages SET published_at=clock_timestamp(),lease_until=NULL,lease_token=NULL
          WHERE id=? AND lease_token=? AND lease_until>clock_timestamp()`,
          [row.id, token],
        );
      } catch (e) {
        this.metrics.retries.inc({ component: 'outbox' });
        const delay = Math.max(
          1000,
          Math.random() * Math.min(300000, 1000 * 2 ** Math.min(row.attempts - 1, 18)),
        );
        await this.db.query(
          `UPDATE outbox_messages SET next_attempt_at=clock_timestamp()+(?*interval '1 millisecond'),lease_token=NULL,lease_until=NULL
          WHERE id=? AND lease_token=?`,
          [delay, row.id, token],
        );
        logger.warn({ eventId: row.id, errorCode: code(e) }, 'outbox_retry');
      }
    });
    const [stats] = await this.db.query<{
      lag: string;
      pending: string;
    }>(`SELECT COALESCE(extract(epoch FROM clock_timestamp()-min(occurred_at)),0)::text as lag,
      count(*)::text as pending FROM outbox_messages WHERE published_at IS NULL`);
    this.metrics.lag.set(Number(stats!.lag));
    this.metrics.pending.set(Number(stats!.pending));
  }
}
export class ReferenceWorker extends LoopWorker {
  protected interval = Number(process.env.REFERENCE_POLL_MS ?? 1000);
  constructor(private wagering: Wagering) {
    super();
  }
  async tick(): Promise<void> {
    const token = randomUUID();
    const rows = await this.wagering.db.query<TransactionRow>(
      `WITH due AS (SELECT id FROM wager_transactions WHERE status='PENDING_REFERENCE'
      AND next_attempt_at<=clock_timestamp() AND (lease_until IS NULL OR lease_until<=clock_timestamp())
      ORDER BY next_attempt_at,id FOR UPDATE SKIP LOCKED LIMIT 20)
      UPDATE wager_transactions t SET lease_token=?,lease_until=clock_timestamp()+(?*interval '1 millisecond'),reference_attempts=reference_attempts+1
      FROM due WHERE t.id=due.id RETURNING t.*`,
      [token, Number(process.env.WORK_LEASE_MS ?? 60000)],
    );
    await concurrent(rows, 5, async (row) => {
      this.wagering.metrics.retries.inc({ component: 'reference' });
      try {
        await this.wagering.process(commandFromRow(row), {
          source: 'reference',
          correlationId: row.correlation_id,
          referenceLease: { transactionId: row.id, token },
          ...(row.causation_id ? { causationId: row.causation_id } : {}),
        });
      } catch (e) {
        if (code(e) !== 'LEASE_LOST')
          logger.warn({ transactionId: row.id, errorCode: code(e) }, 'reference_retry_failed');
      }
    });
  }
}
const envelopeSchema = z.object({
  messageId: z.string().min(1).max(128),
  type: z.literal('WagerTransactionRequested'),
  occurredAt: z.iso.datetime(),
  data: z.record(z.string(), z.unknown()),
});
export class QueueConsumer extends LoopWorker {
  protected interval = 50;
  private abort = new AbortController();
  private renewalAbort = new AbortController();
  private inflight = new Map<string, Message>();
  private heartbeats = new Map<string, ReturnType<typeof setInterval>>();
  private renewals = new Set<Promise<void>>();
  constructor(
    private db: Database,
    private queues: Queues,
    private metrics: Observability,
    private mode: 'input' | 'dlq' | 'events',
    private wagering?: Wagering,
  ) {
    super();
  }
  private get name(): string {
    return this.queues.names[this.mode];
  }
  async tick(): Promise<void> {
    const messages = await this.queues.receive(this.name, this.abort.signal);
    await concurrent(messages, 5, async (message) => {
      const receipt = message.ReceiptHandle!;
      this.inflight.set(receipt, message);
      const heartbeat = setInterval(
        () => {
          const renewal = this.queues
            .visibility(
              this.name,
              receipt,
              Number(process.env.SQS_VISIBILITY_SECONDS ?? 30),
              this.renewalAbort.signal,
            )
            .catch((e) =>
              logger.warn({ messageId: message.MessageId, errorCode: code(e) }, 'visibility_renew_failed'),
            );
          this.renewals.add(renewal);
          void renewal.finally(() => this.renewals.delete(renewal));
        },
        Number(process.env.SQS_HEARTBEAT_MS ?? 10000),
      );
      this.heartbeats.set(receipt, heartbeat);
      try {
        if (this.mode === 'input') await this.processInput(message);
        else if (this.mode === 'dlq') await this.auditDlq(message);
        else await this.recordEvent(message);
        let envelopeId: string | undefined;
        try {
          const body: unknown = JSON.parse(message.Body ?? '');
          if (body && typeof body === 'object' && 'messageId' in body && typeof body.messageId === 'string')
            envelopeId = body.messageId;
        } catch {}
        await fault('before-ack', { messageId: envelopeId });
        await this.queues.ack(this.name, receipt);
      } catch (e) {
        if (this.stopping) return;
        this.metrics.retries.inc({ component: this.mode });
        logger.warn({ messageId: message.MessageId, errorCode: code(e) }, 'message_retry');
        await this.queues
          .visibility(
            this.name,
            receipt,
            Math.min(60, 2 ** Math.min(Number(message.Attributes?.ApproximateReceiveCount ?? 1) - 1, 6)),
          )
          .catch(() => {});
      } finally {
        clearInterval(heartbeat);
        this.heartbeats.delete(receipt);
        this.inflight.delete(receipt);
      }
    });
  }
  private async processInput(message: Message): Promise<void> {
    let context: ProcessingContext | undefined;
    let command: ReturnType<typeof parseCommand> | undefined;
    let parsed: ReturnType<typeof envelopeSchema.parse> | undefined;
    try {
      parsed = envelopeSchema.parse(JSON.parse(message.Body ?? ''));
      command = parseCommand(parsed.data, parsed.data.idempotencyKey);
      context = {
        source: 'sqs',
        correlationId: parsed.messageId,
        causationId: parsed.messageId,
        inbox: { messageId: parsed.messageId, payloadHash: hash(parsed) },
      };
      const [prior] = await this.db.query<{
        disposition: string;
        dead_letter_sent_at: Date | null;
        payload_hash: string;
      }>(
        `SELECT disposition,dead_letter_sent_at,payload_hash FROM inbox_messages WHERE consumer_name='wager-requests-v1' AND message_id=?`,
        [parsed.messageId],
      );
      if (prior?.disposition === 'DLQ' && prior.payload_hash === context.inbox!.payloadHash) {
        if (!prior.dead_letter_sent_at)
          await this.deadLetter(message, 'PERMANENT_MESSAGE_ERROR', context.inbox);
        return;
      }
      const result = await this.wagering!.process(command, context);
      if (result.status === 'FAILED')
        await this.deadLetter(message, result.failureCode ?? 'PERMANENT_INFRASTRUCTURE_ERROR', context.inbox);
    } catch (e) {
      if (transient(e) || (e instanceof ServiceError && e.retryable)) throw e;
      if (
        e instanceof ServiceError &&
        ['IDEMPOTENCY_CONFLICT', 'EXTERNAL_TRANSACTION_CONFLICT'].includes(e.code) &&
        context?.inbox
      ) {
        await this.db.query(
          `INSERT INTO inbox_messages(consumer_name,message_id,payload_hash,processed_at)
          VALUES ('wager-requests-v1',?,?,clock_timestamp()) ON CONFLICT DO NOTHING`,
          [context.inbox.messageId, context.inbox.payloadHash],
        );
        return;
      }
      if (sqlState(e) && command && context && ['42501', '0A000'].includes(sqlState(e)!)) {
        await this.wagering!.process(command, {
          ...context,
          permanentFailureCode: 'PERMANENT_INFRASTRUCTURE_ERROR',
        });
      }
      await this.deadLetter(
        message,
        e instanceof z.ZodError || e instanceof SyntaxError ? 'INVALID_ENVELOPE' : code(e),
        context?.inbox,
      );
    }
  }
  private async deadLetter(
    message: Message,
    reason: string,
    inbox?: { messageId: string; payloadHash: string },
  ): Promise<void> {
    if (inbox)
      await this.db.query(
        `INSERT INTO inbox_messages(consumer_name,message_id,payload_hash,processed_at,disposition)
      VALUES ('wager-requests-v1',?,?,clock_timestamp(),'DLQ') ON CONFLICT(consumer_name,message_id)
      DO UPDATE SET disposition='DLQ' WHERE inbox_messages.payload_hash=excluded.payload_hash`,
        [inbox.messageId, inbox.payloadHash],
      );
    await this.queues.send(
      this.queues.names.dlq,
      message.Body ?? '',
      `dead-${hash(message.Body ?? '').slice(0, 32)}`,
      hash({ messageId: inbox?.messageId ?? message.MessageId, body: message.Body }),
      reason,
      inbox?.messageId ?? message.MessageId,
    );
    await fault('after-dlq-send', { messageId: inbox?.messageId });
    if (inbox)
      await this.db.query(
        `UPDATE inbox_messages SET dead_letter_sent_at=clock_timestamp() WHERE consumer_name='wager-requests-v1'
      AND message_id=? AND payload_hash=?`,
        [inbox.messageId, inbox.payloadHash],
      );
  }
  private async auditDlq(message: Message): Promise<void> {
    let id = message.MessageAttributes?.SourceMessageId?.StringValue ?? message.MessageId!;
    try {
      const json = JSON.parse(message.Body ?? '');
      if (typeof json.messageId === 'string') id = json.messageId;
    } catch {}
    const inserted = await this.db.query(
      `INSERT INTO dead_letter_records(id,message_id,payload_hash,body,reason) VALUES (?,?,?,?,?)
      ON CONFLICT DO NOTHING RETURNING id`,
      [
        randomUUID(),
        id,
        hash(message.Body ?? ''),
        message.Body ?? '',
        message.MessageAttributes?.FailureCode?.StringValue ?? 'DELIVERY_RETRIES_EXHAUSTED',
      ],
    );
    if (inserted.length) this.metrics.dlq.inc();
  }
  private async recordEvent(message: Message): Promise<void> {
    const body = JSON.parse(message.Body ?? '');
    const id = z.uuid().parse(body.eventId);
    await this.db.transaction(async (em) => {
      await this.db.query(
        `INSERT INTO integration_event_receipts(consumer_name,event_id,payload_hash) VALUES ('integration-audit-v1',?,?) ON CONFLICT DO NOTHING`,
        [id, hash(body)],
        em,
      );
      const [r] = await this.db.query<{ payload_hash: string }>(
        `SELECT payload_hash FROM integration_event_receipts WHERE consumer_name='integration-audit-v1' AND event_id=?`,
        [id],
        em,
      );
      if (r?.payload_hash !== hash(body)) throw new ServiceError('EVENT_PAYLOAD_CONFLICT', 409);
    });
  }
  override async stop(): Promise<void> {
    this.stopping = true;
    this.abort.abort();
    await super.stop();
    for (const heartbeat of this.heartbeats.values()) clearInterval(heartbeat);
    this.renewalAbort.abort();
    await Promise.allSettled(this.renewals);
    await Promise.allSettled(
      [...this.inflight.keys()].map((receipt) =>
        this.queues.visibility(this.name, receipt, 0, AbortSignal.timeout(1000)),
      ),
    );
  }
}
