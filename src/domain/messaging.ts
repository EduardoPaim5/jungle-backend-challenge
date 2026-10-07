import { DomainError } from './errors.js';
function immutable<T>(value: T): T {
  if (value && typeof value === 'object') {
    Object.values(value).forEach(immutable);
    Object.freeze(value);
  }
  return value;
}

export class InboxMessage {
  private constructor(
    public readonly messageId: string,
    public readonly consumerName: string,
    public readonly payloadHash: string,
    public readonly receivedAt: Date,
    private processed: Date | undefined,
  ) {}
  static receive(p: { messageId: string; consumerName: string; payloadHash: string }, at = new Date()) {
    return new this(p.messageId, p.consumerName, p.payloadHash, new Date(at), undefined);
  }
  static rehydrate(p: {
    messageId: string;
    consumerName: string;
    payloadHash: string;
    receivedAt: Date;
    processedAt: Date | undefined;
  }) {
    return new this(p.messageId, p.consumerName, p.payloadHash, p.receivedAt, p.processedAt);
  }
  get processedAt(): Date | undefined {
    return this.processed && new Date(this.processed);
  }
  isProcessed(): boolean {
    return this.processed !== undefined;
  }
  markProcessed(at: Date): void {
    if (this.isProcessed()) throw new DomainError('INBOX_ALREADY_PROCESSED');
    this.processed = new Date(at);
  }
}
export class OutboxMessage {
  private readonly occurred: string;
  private constructor(
    public readonly id: string,
    public readonly aggregateId: string,
    public readonly eventType: string,
    public readonly payload: Readonly<Record<string, unknown>>,
    occurredAt: Date,
    private tries: number,
    private next: Date | undefined,
    private published: Date | undefined,
  ) {
    this.occurred = occurredAt.toISOString();
    this.payload = immutable(structuredClone(payload));
  }
  get occurredAt(): Date {
    return new Date(this.occurred);
  }
  static enqueue(
    p: { eventId: string; aggregateId: string; eventType: string; occurredAt: string } & Record<
      string,
      unknown
    >,
  ) {
    return new this(
      p.eventId,
      p.aggregateId,
      p.eventType,
      Object.freeze(structuredClone(p)),
      new Date(p.occurredAt),
      0,
      undefined,
      undefined,
    );
  }
  static rehydrate(p: {
    id: string;
    aggregateId: string;
    eventType: string;
    payload: Record<string, unknown>;
    occurredAt: Date;
    attempts: number;
    nextAttemptAt: Date | undefined;
    publishedAt: Date | undefined;
  }) {
    return new this(
      p.id,
      p.aggregateId,
      p.eventType,
      p.payload,
      new Date(p.occurredAt),
      p.attempts,
      p.nextAttemptAt && new Date(p.nextAttemptAt),
      p.publishedAt && new Date(p.publishedAt),
    );
  }
  get attempts(): number {
    return this.tries;
  }
  get nextAttemptAt(): Date | undefined {
    return this.next && new Date(this.next);
  }
  get publishedAt(): Date | undefined {
    return this.published && new Date(this.published);
  }
  isPending(): boolean {
    return !this.published;
  }
  isDue(now: Date): boolean {
    return this.isPending() && (!this.next || this.next <= now);
  }
  markPublished(at: Date): void {
    if (!this.isPending()) throw new DomainError('OUTBOX_ALREADY_PUBLISHED');
    this.published = new Date(at);
  }
  scheduleRetry(now: Date, random = Math.random): void {
    if (!this.isPending()) throw new DomainError('OUTBOX_ALREADY_PUBLISHED');
    this.tries += 1;
    this.next = new Date(
      now.getTime() + Math.max(1000, random() * Math.min(300000, 1000 * 2 ** Math.min(this.tries - 1, 18))),
    );
  }
}
