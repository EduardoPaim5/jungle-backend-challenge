import type { MoneyProps } from './money.js';

export interface EventContext {
  correlationId: string;
  causationId?: string;
}
export interface EventProps<T> extends EventContext {
  eventId: string;
  aggregateId: string;
  occurredAt: Date;
  data: T;
}
function freezeDeep<T>(value: T): T {
  if (value && typeof value === 'object') {
    Object.values(value).forEach(freezeDeep);
    Object.freeze(value);
  }
  return value;
}
export abstract class IntegrationEvent<T> {
  abstract readonly eventType: string;
  readonly version = 1;
  readonly eventId: string;
  readonly aggregateId: string;
  readonly correlationId: string;
  readonly causationId: string | undefined;
  private readonly at: string;
  readonly data: Readonly<T>;
  protected constructor(props: EventProps<T>) {
    this.eventId = props.eventId;
    this.aggregateId = props.aggregateId;
    this.correlationId = props.correlationId;
    this.causationId = props.causationId;
    this.at = props.occurredAt.toISOString();
    this.data = freezeDeep(structuredClone(props.data));
  }
  toJSON() {
    return {
      eventId: this.eventId,
      eventType: this.eventType,
      aggregateId: this.aggregateId,
      correlationId: this.correlationId,
      ...(this.causationId ? { causationId: this.causationId } : {}),
      occurredAt: this.at,
      version: this.version,
      data: this.data,
    };
  }
}
export interface TransactionEventData {
  transactionId: string;
  walletId: string;
  providerId: string;
  externalTransactionId: string;
  kind: string;
  status: string;
  money: MoneyProps;
  balance?: MoneyProps;
  walletVersion?: number;
  failureCode?: string;
}
export class WagerTransactionProcessed extends IntegrationEvent<TransactionEventData> {
  readonly eventType = 'WagerTransactionProcessed';
  static create(p: EventProps<TransactionEventData>) {
    return new this(p);
  }
}
export class WagerTransactionRejected extends IntegrationEvent<TransactionEventData> {
  readonly eventType = 'WagerTransactionRejected';
  static create(p: EventProps<TransactionEventData>) {
    return new this(p);
  }
}
export class WagerTransactionPendingReference extends IntegrationEvent<TransactionEventData> {
  readonly eventType = 'WagerTransactionPendingReference';
  static create(p: EventProps<TransactionEventData>) {
    return new this(p);
  }
}
export class WagerTransactionFailed extends IntegrationEvent<TransactionEventData> {
  readonly eventType = 'WagerTransactionFailed';
  static create(p: EventProps<TransactionEventData>) {
    return new this(p);
  }
}
export interface BalanceChangedData {
  walletId: string;
  transactionId: string;
  direction: string;
  money: MoneyProps;
  balanceBefore: MoneyProps;
  balanceAfter: MoneyProps;
  walletVersion: number;
}
export class WalletBalanceChanged extends IntegrationEvent<BalanceChangedData> {
  readonly eventType = 'WalletBalanceChanged';
  static create(p: EventProps<BalanceChangedData>) {
    return new this(p);
  }
}
