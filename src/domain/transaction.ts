import { DomainError, InvalidTransactionStateError } from './errors.js';
import { Money, type MoneyProps } from './money.js';
import type { LedgerDirection } from './ledger.js';

export type WagerKind = 'OPENING' | 'BET' | 'WIN' | 'LOSS' | 'REFUND' | 'ROLLBACK';
export type TransactionStatus = 'PENDING' | 'PENDING_REFERENCE' | 'PROCESSED' | 'REJECTED' | 'FAILED';
export interface WagerCommand {
  providerId: string;
  externalTransactionId: string;
  idempotencyKey: string;
  playerId: string;
  walletId: string;
  roundId: string;
  gameId: string;
  kind: Exclude<WagerKind, 'OPENING'>;
  money: MoneyProps;
  referenceExternalTransactionId?: string | undefined;
}
export interface TransactionState {
  id: string;
  providerId: string;
  externalTransactionId: string;
  idempotencyKey: string;
  payloadHash: string;
  playerId: string;
  walletId: string;
  roundId: string;
  gameId: string;
  kind: WagerKind;
  money: Money;
  referenceExternalTransactionId: string | undefined;
  createdAt: Date;
  status: TransactionStatus;
  referenceTransactionId: string | undefined;
  failureCode: string | undefined;
  processedAt: Date | undefined;
}
export class WagerTransaction {
  private constructor(private state: TransactionState) {}
  static create(
    props: Omit<TransactionState, 'status' | 'referenceTransactionId' | 'failureCode' | 'processedAt'>,
  ): WagerTransaction {
    if (['REFUND', 'ROLLBACK'].includes(props.kind) && !props.referenceExternalTransactionId)
      throw new DomainError('REFERENCE_REQUIRED');
    if (['BET', 'LOSS', 'OPENING'].includes(props.kind) && props.referenceExternalTransactionId)
      throw new DomainError('REFERENCE_NOT_ALLOWED');
    return new WagerTransaction({
      ...props,
      status: 'PENDING',
      referenceTransactionId: undefined,
      failureCode: undefined,
      processedAt: undefined,
    });
  }
  static rehydrate(state: TransactionState): WagerTransaction {
    return new WagerTransaction({
      ...state,
      createdAt: new Date(state.createdAt),
      processedAt: state.processedAt && new Date(state.processedAt),
    });
  }
  get id(): string {
    return this.state.id;
  }
  get providerId(): string {
    return this.state.providerId;
  }
  get playerId(): string {
    return this.state.playerId;
  }
  get walletId(): string {
    return this.state.walletId;
  }
  get roundId(): string {
    return this.state.roundId;
  }
  get kind(): WagerKind {
    return this.state.kind;
  }
  get money(): Money {
    return this.state.money;
  }
  get status(): TransactionStatus {
    return this.state.status;
  }
  get failureCode(): string | undefined {
    return this.state.failureCode;
  }
  get referenceTransactionId(): string | undefined {
    return this.state.referenceTransactionId;
  }
  get processedAt(): Date | undefined {
    return this.state.processedAt && new Date(this.state.processedAt);
  }
  markProcessed(referenceTransactionId: string | undefined, at: Date): void {
    this.assertMutable();
    this.state = { ...this.state, status: 'PROCESSED', referenceTransactionId, processedAt: new Date(at) };
  }
  markPendingReference(): void {
    this.assertMutable();
    this.state = { ...this.state, status: 'PENDING_REFERENCE' };
  }
  reject(code: string): void {
    this.assertMutable();
    this.state = { ...this.state, status: 'REJECTED', failureCode: code };
  }
  fail(code: string): void {
    this.assertMutable();
    this.state = { ...this.state, status: 'FAILED', failureCode: code };
  }
  isTerminal(): boolean {
    return ['PROCESSED', 'REJECTED', 'FAILED'].includes(this.status);
  }
  affectsBalance(): boolean {
    return this.kind !== 'LOSS';
  }
  requiresReference(): boolean {
    return this.kind === 'REFUND' || this.kind === 'ROLLBACK';
  }
  matchesPayload(hash: string): boolean {
    return this.state.payloadHash === hash;
  }
  ledgerDirectionFor(reference?: WagerTransaction): LedgerDirection {
    if (this.kind === 'LOSS') throw new DomainError('NO_LEDGER_FOR_LOSS');
    if (this.kind === 'BET') return 'DEBIT';
    if (this.kind !== 'ROLLBACK') return 'CREDIT';
    if (!reference) throw new DomainError('REFERENCE_REQUIRED');
    return reference.kind === 'BET' ? 'CREDIT' : 'DEBIT';
  }
  validateReference(reference: WagerTransaction): void {
    if (
      this.providerId !== reference.providerId ||
      this.playerId !== reference.playerId ||
      this.walletId !== reference.walletId ||
      this.money.currency !== reference.money.currency ||
      this.roundId !== reference.roundId
    )
      throw new DomainError('REFERENCE_CONTEXT_MISMATCH');
    const allowed = this.kind === 'ROLLBACK' ? ['BET', 'WIN', 'REFUND'] : ['BET'];
    if (reference.id === this.id || !allowed.includes(reference.kind))
      throw new DomainError('INVALID_REFERENCE_KIND');
    if (this.requiresReference() && !this.money.equals(reference.money))
      throw new DomainError('REFERENCE_AMOUNT_MISMATCH');
    if (reference.isTerminal() && reference.status !== 'PROCESSED')
      throw new DomainError('REFERENCE_NOT_PROCESSED');
  }
  private assertMutable(): void {
    if (this.isTerminal()) throw new InvalidTransactionStateError();
  }
}
