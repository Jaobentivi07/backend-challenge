import { describe, expect, test } from 'bun:test';
import { Money } from '../src/domain/money';

describe('Money', () => {
  test('preserves exact arithmetic and immutable values', () => {
    const opening = Money.from({ amount: '9007199254740993.12', currency: 'BRL' });
    const bet = Money.from({ amount: '0.12', currency: 'BRL' });
    expect(opening.subtract(bet).toJSON()).toEqual({ amount: '9007199254740993.00', currency: 'BRL' });
    expect(opening.toJSON()).toEqual({ amount: '9007199254740993.12', currency: 'BRL' });
  });

  test('keeps cents near the NUMERIC(19,2) limit', () => {
    const nearLimit = Money.from({ amount: '99999999999999999.99', currency: 'BRL' });
    expect(nearLimit.add(Money.from({ amount: '0.01', currency: 'BRL' })).toJSON())
      .toEqual({ amount: '100000000000000000.00', currency: 'BRL' });
  });

  test('rejects malformed values, mixed currencies, and negative balances', () => {
    for (const amount of ['', '1', '1.2', '1.234', '-1.00', '1e2', 'NaN', 'Infinity']) {
      expect(() => Money.from({ amount, currency: 'BRL' })).toThrow('INVALID_MONEY');
    }
    const brl = Money.from({ amount: '1.00', currency: 'BRL' });
    expect(() => brl.add(Money.from({ amount: '1.00', currency: 'USD' }))).toThrow('CURRENCY_MISMATCH');
    expect(() => Money.zero('BRL').subtract(brl)).toThrow('INSUFFICIENT_FUNDS');
  });
});
