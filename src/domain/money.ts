import { DomainError } from './errors.js';

export interface MoneyProps {
  amount: string;
  currency: string;
}
const MAX_CENTS = 10n ** 38n - 1n;

export class Money {
  private constructor(
    private readonly cents: bigint,
    public readonly currency: string,
  ) {
    Object.freeze(this);
  }

  static from(props: MoneyProps): Money {
    if (typeof props.amount !== 'string' || !/^\d+\.\d{2}$/.test(props.amount) || props.amount.length > 128)
      throw new DomainError('INVALID_MONEY');
    const money = Money.rehydrate(props);
    money.assertPersistable();
    return money;
  }
  static rehydrate(props: MoneyProps): Money {
    if (!/^[A-Z]{3}$/.test(props.currency) || !/^-?\d+\.\d{2}$/.test(props.amount))
      throw new DomainError('INVALID_MONEY');
    return new Money(BigInt(props.amount.replace('.', '')), props.currency);
  }
  static zero(currency: string): Money {
    return Money.from({ amount: '0.00', currency });
  }
  add(other: Money): Money {
    this.assertSameCurrency(other);
    return new Money(this.cents + other.cents, this.currency);
  }
  subtract(other: Money): Money {
    this.assertSameCurrency(other);
    return new Money(this.cents - other.cents, this.currency);
  }
  negate(): Money {
    return new Money(-this.cents, this.currency);
  }
  isZero(): boolean {
    return this.cents === 0n;
  }
  isPositive(): boolean {
    return this.cents > 0n;
  }
  isNegative(): boolean {
    return this.cents < 0n;
  }
  isLessThan(other: Money): boolean {
    this.assertSameCurrency(other);
    return this.cents < other.cents;
  }
  equals(other: Money): boolean {
    this.assertSameCurrency(other);
    return this.cents === other.cents;
  }
  assertPersistable(): void {
    if (this.cents < 0n || this.cents > MAX_CENTS) throw new DomainError('MONEY_LIMIT_EXCEEDED');
  }
  toJSON(): MoneyProps {
    const absolute = this.cents < 0n ? -this.cents : this.cents;
    return {
      amount: `${this.cents < 0n ? '-' : ''}${absolute / 100n}.${(absolute % 100n).toString().padStart(2, '0')}`,
      currency: this.currency,
    };
  }
  toString(): string {
    return `${this.toJSON().amount} ${this.currency}`;
  }
  private assertSameCurrency(other: Money): void {
    if (this.currency !== other.currency) throw new DomainError('CURRENCY_MISMATCH');
  }
}
