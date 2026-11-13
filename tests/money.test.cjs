const test = require('node:test');
const assert = require('node:assert/strict');
const { Money } = require('../dist/src/domain/money.js');

test('Money mantém aritmética decimal exata e valores imutáveis', () => {
  const opening = Money.from({ amount: '9007199254740993.12', currency: 'BRL' });
  const bet = Money.from({ amount: '0.12', currency: 'BRL' });
  assert.deepEqual(opening.subtract(bet).toJSON(), { amount: '9007199254740993.00', currency: 'BRL' });
  assert.deepEqual(opening.toJSON(), { amount: '9007199254740993.12', currency: 'BRL' });
});

test('Money preserva centavos no limite de NUMERIC(19,2)', () => {
  const nearLimit = Money.from({ amount: '99999999999999999.99', currency: 'BRL' });
  const cent = Money.from({ amount: '0.01', currency: 'BRL' });
  assert.deepEqual(nearLimit.add(cent).toJSON(), { amount: '100000000000000000.00', currency: 'BRL' });
});

test('Money rejeita valores malformados e códigos de moeda inválidos', () => {
  for (const amount of ['', '1', '1.2', '1.234', '-1.00', '1e2', 'NaN', 'Infinity']) {
    assert.throws(() => Money.from({ amount, currency: 'BRL' }), { message: 'INVALID_MONEY' });
  }
  assert.throws(() => Money.from({ amount: '1.00', currency: 'brl' }), { message: 'INVALID_MONEY' });
});

test('Money rejeita moedas diferentes e saldos negativos', () => {
  const brl = Money.from({ amount: '1.00', currency: 'BRL' });
  const usd = Money.from({ amount: '1.00', currency: 'USD' });
  assert.throws(() => brl.add(usd), { message: 'CURRENCY_MISMATCH' });
  assert.throws(() => Money.zero('BRL').subtract(brl), { message: 'INSUFFICIENT_FUNDS' });
});
