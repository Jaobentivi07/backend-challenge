import Decimal from 'decimal.js';

// NUMERIC(19,2) values and their intermediate sums can exceed Decimal's
// default 20 significant digits. Keep headroom so cent arithmetic never rounds.
Decimal.set({ precision: 40 });

export interface MoneyProps { amount: string; currency: string }

/** Valor monetário imutável e exato; aceita somente strings decimais válidas. */
export class Money {
  private constructor(private readonly value: Decimal, public readonly currency: string) {}

  static from({ amount, currency }: MoneyProps): Money {
    if (!/^(0|[1-9]\d*)\.\d{2}$/.test(amount) || !/^[A-Z]{3}$/.test(currency)) {
      throw new Error('INVALID_MONEY');
    }
    return new Money(new Decimal(amount), currency);
  }

  static zero(currency: string): Money { return Money.from({ amount: '0.00', currency }); }
  add(other: Money): Money { this.assertCurrency(other); return Money.from({ amount: this.value.plus(other.value).toFixed(2), currency: this.currency }); }
  subtract(other: Money): Money { this.assertCurrency(other); const result = this.value.minus(other.value); if (result.isNegative()) throw new Error('INSUFFICIENT_FUNDS'); return Money.from({ amount: result.toFixed(2), currency: this.currency }); }
  negate(): Money { return new Money(this.value.negated(), this.currency); }
  isZero(): boolean { return this.value.isZero(); }
  isPositive(): boolean { return this.value.isPositive(); }
  isNegative(): boolean { return this.value.isNegative(); }
  isLessThan(other: Money): boolean { this.assertCurrency(other); return this.value.lessThan(other.value); }
  equals(other: Money): boolean { return this.currency === other.currency && this.value.equals(other.value); }
  toJSON(): MoneyProps { return { amount: this.value.toFixed(2), currency: this.currency }; }
  toString(): string { return `${this.currency} ${this.value.toFixed(2)}`; }
  private assertCurrency(other: Money): void { if (this.currency !== other.currency) throw new Error('CURRENCY_MISMATCH'); }
}
