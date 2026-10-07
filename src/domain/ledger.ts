import { DomainError } from './errors.js';
import { Money } from './money.js';

export type LedgerDirection = 'DEBIT' | 'CREDIT';
export interface LedgerState {
  id: string;
  walletId: string;
  transactionId: string;
  direction: LedgerDirection;
  money: Money;
  balanceBefore: Money;
  balanceAfter: Money;
  walletVersion: number;
  createdAt: Date;
}
export class WalletLedgerEntry {
  private constructor(private readonly state: LedgerState) {
    Object.freeze(state);
    Object.freeze(this);
  }
  static create(state: LedgerState): WalletLedgerEntry {
    const entry = new WalletLedgerEntry({ ...state, createdAt: new Date(state.createdAt) });
    if (
      !state.money.isPositive() ||
      state.balanceBefore.isNegative() ||
      state.balanceAfter.isNegative() ||
      !entry.isBalanced()
    )
      throw new DomainError('UNBALANCED_LEDGER');
    return entry;
  }
  static rehydrate(state: LedgerState): WalletLedgerEntry {
    return new WalletLedgerEntry({ ...state, createdAt: new Date(state.createdAt) });
  }
  get id(): string {
    return this.state.id;
  }
  get walletId(): string {
    return this.state.walletId;
  }
  get transactionId(): string {
    return this.state.transactionId;
  }
  get direction(): LedgerDirection {
    return this.state.direction;
  }
  get money(): Money {
    return this.state.money;
  }
  get balanceBefore(): Money {
    return this.state.balanceBefore;
  }
  get balanceAfter(): Money {
    return this.state.balanceAfter;
  }
  get walletVersion(): number {
    return this.state.walletVersion;
  }
  get createdAt(): Date {
    return new Date(this.state.createdAt);
  }
  isBalanced(): boolean {
    const expected =
      this.direction === 'CREDIT'
        ? this.balanceBefore.add(this.money)
        : this.balanceBefore.subtract(this.money);
    return expected.equals(this.balanceAfter);
  }
}
