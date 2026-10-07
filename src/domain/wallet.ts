import { DomainError } from './errors.js';
import { Money } from './money.js';

export interface WalletState {
  id: string;
  playerId: string;
  currency: string;
  balance: Money;
  version: number;
  createdAt: Date;
  updatedAt: Date;
}
export class Wallet {
  private constructor(private state: WalletState) {}
  static open(props: { id: string; playerId: string; initialBalance: Money }, at = new Date()): Wallet {
    props.initialBalance.assertPersistable();
    return new Wallet({
      id: props.id,
      playerId: props.playerId,
      currency: props.initialBalance.currency,
      balance: props.initialBalance,
      version: 1,
      createdAt: new Date(at),
      updatedAt: new Date(at),
    });
  }
  static rehydrate(state: WalletState): Wallet {
    return new Wallet({
      ...state,
      createdAt: new Date(state.createdAt),
      updatedAt: new Date(state.updatedAt),
    });
  }
  get id(): string {
    return this.state.id;
  }
  get playerId(): string {
    return this.state.playerId;
  }
  get currency(): string {
    return this.state.currency;
  }
  get balance(): Money {
    return this.state.balance;
  }
  get version(): number {
    return this.state.version;
  }
  get createdAt(): Date {
    return new Date(this.state.createdAt);
  }
  get updatedAt(): Date {
    return new Date(this.state.updatedAt);
  }
  debit(money: Money, code = 'INSUFFICIENT_FUNDS', at = new Date()): void {
    this.assertPositive(money);
    if (this.balance.isLessThan(money)) throw new DomainError(code);
    this.apply(this.balance.subtract(money), at);
  }
  credit(money: Money, at = new Date()): void {
    this.assertPositive(money);
    this.apply(this.balance.add(money), at);
  }
  private assertPositive(money: Money): void {
    if (money.currency !== this.currency) throw new DomainError('CURRENCY_MISMATCH');
    if (!money.isPositive()) throw new DomainError('AMOUNT_MUST_BE_POSITIVE');
  }
  private apply(balance: Money, at: Date): void {
    balance.assertPersistable();
    this.state = { ...this.state, balance, version: this.version + 1, updatedAt: new Date(at) };
  }
}
