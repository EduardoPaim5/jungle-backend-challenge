import { describe, expect, test } from 'bun:test';
import { Money } from '../../src/domain/money.js';
import { Wallet } from '../../src/domain/wallet.js';
import { WalletLedgerEntry } from '../../src/domain/ledger.js';
import { WagerTransaction, type TransactionState } from '../../src/domain/transaction.js';
import { InboxMessage, OutboxMessage } from '../../src/domain/messaging.js';
import { WagerTransactionProcessed } from '../../src/domain/events.js';
import { businessHash, parseCommand } from '../../src/application/contracts.js';
const money = (amount: string, currency = 'BRL') => Money.from({ amount, currency });
function transaction(kind: TransactionState['kind'] = 'BET', reference?: string) {
  return WagerTransaction.create({
    id: 'tx',
    providerId: 'provider',
    externalTransactionId: 'external',
    idempotencyKey: 'key',
    payloadHash: 'hash',
    playerId: 'player',
    walletId: 'wallet',
    roundId: 'round',
    gameId: 'game',
    kind,
    money: money('10.00'),
    referenceExternalTransactionId: reference,
    createdAt: new Date(),
  });
}
describe('Money: centavos exatos, imutabilidade e limites', () => {
  test('opera acima da precisão segura de number, sem arredondamento', () => {
    const original = money('9007199254740993.01');
    expect(original.add(money('0.02')).toJSON().amount).toBe('9007199254740993.03');
    expect(original.toJSON().amount).toBe('9007199254740993.01');
    expect(money('0.10').add(money('0.20')).toJSON().amount).toBe('0.30');
    expect(money('00010.00').toJSON().amount).toBe('10.00');
    expect(Object.isFrozen(original)).toBe(true);
  });
  for (const amount of [
    '-1.00',
    '1',
    '1.0',
    '1.001',
    '1e2',
    'NaN',
    'Infinity',
    ' 1.00',
    '1,00',
    '+1.00',
    '',
  ]) {
    test(`rejeita ${JSON.stringify(amount)}`, () => expect(() => money(amount)).toThrow());
  }
  test('limite NUMERIC(38,2) e diferença assinada', () => {
    const maximum = money(`${'9'.repeat(36)}.99`);
    expect(() => maximum.add(money('0.01')).assertPersistable()).toThrow('MONEY_LIMIT_EXCEEDED');
    expect(() => money(`${'1'.repeat(37)}.00`)).toThrow('MONEY_LIMIT_EXCEEDED');
    expect(money('1.00').subtract(money('2.01')).toJSON().amount).toBe('-1.01');
  });
  test('moedas distintas nunca são combinadas', () => {
    expect(() => money('1.00').add(money('1.00', 'USD'))).toThrow('CURRENCY_MISMATCH');
    expect(() => money('1.00', 'brl')).toThrow();
  });
});
describe('Wallet e ledger', () => {
  test('saldo insuficiente e overflow preservam estado; movimento incrementa versão', () => {
    const w = Wallet.open({ id: 'w', playerId: 'p', initialBalance: money('100.00') });
    expect(() => w.debit(money('101.00'))).toThrow('INSUFFICIENT_FUNDS');
    expect(w.version).toBe(1);
    w.debit(money('80.00'));
    expect(w.balance.toJSON().amount).toBe('20.00');
    expect(w.version).toBe(2);
    expect(() => w.credit(money('0.00'))).toThrow();
    const max = Wallet.open({ id: 'm', playerId: 'p', initialBalance: money(`${'9'.repeat(36)}.99`) });
    expect(() => max.credit(money('0.01'))).toThrow();
    expect(max.version).toBe(1);
  });
  test('ledger valida sua aritmética e protege estado e datas', () => {
    const at = new Date();
    const props = {
      id: 'l',
      transactionId: 'tx',
      walletId: 'w',
      direction: 'DEBIT' as const,
      money: money('80.00'),
      balanceBefore: money('100.00'),
      balanceAfter: money('20.00'),
      walletVersion: 2,
      createdAt: at,
    };
    const entry = WalletLedgerEntry.create(props);
    expect(entry.isBalanced()).toBe(true);
    expect(Object.isFrozen(entry)).toBe(true);
    entry.createdAt.setFullYear(2000);
    expect(entry.createdAt.getFullYear()).toBe(at.getFullYear());
    const restored = WalletLedgerEntry.rehydrate(props);
    const expectedTime = at.getTime();
    at.setFullYear(2000);
    expect(restored.createdAt.getTime()).toBe(expectedTime);
    expect(() => WalletLedgerEntry.create({ ...props, balanceAfter: money('21.00') })).toThrow(
      'UNBALANCED_LEDGER',
    );
  });
});
describe('transições e referências', () => {
  for (const terminal of ['processed', 'rejected', 'failed'] as const)
    test(`estado ${terminal} não reabre`, () => {
      const tx = transaction();
      tx.markPendingReference();
      if (terminal === 'processed') tx.markProcessed(undefined, new Date());
      else if (terminal === 'rejected') tx.reject('reason');
      else tx.fail('reason');
      expect(() => tx.markPendingReference()).toThrow();
      expect(() => tx.markProcessed(undefined, new Date())).toThrow();
      expect(() => tx.reject('x')).toThrow();
    });
  test('WIN pode diferir da aposta; reversão exige valor e contexto iguais', () => {
    const reference = transaction();
    reference.markProcessed(undefined, new Date());
    const win = transaction('WIN', 'external');
    expect(() => win.validateReference(reference)).toThrow('INVALID_REFERENCE_KIND'); // same internal id
    const state: TransactionState = {
      id: 'other',
      providerId: 'provider',
      externalTransactionId: 'win',
      idempotencyKey: 'other',
      payloadHash: 'h',
      playerId: 'player',
      walletId: 'wallet',
      roundId: 'round',
      gameId: 'game',
      kind: 'WIN',
      money: money('50.00'),
      referenceExternalTransactionId: 'external',
      createdAt: new Date(),
      status: 'PENDING',
      referenceTransactionId: undefined,
      failureCode: undefined,
      processedAt: undefined,
    };
    expect(() => WagerTransaction.rehydrate(state).validateReference(reference)).not.toThrow();
    expect(() =>
      WagerTransaction.rehydrate({ ...state, kind: 'REFUND' }).validateReference(reference),
    ).toThrow('REFERENCE_AMOUNT_MISMATCH');
    expect(() =>
      WagerTransaction.rehydrate({ ...state, roundId: 'different' }).validateReference(reference),
    ).toThrow('REFERENCE_CONTEXT_MISMATCH');
    expect(transaction('ROLLBACK', 'x').ledgerDirectionFor(reference)).toBe('CREDIT');
    expect(transaction('ROLLBACK', 'x').ledgerDirectionFor(transaction('WIN'))).toBe('DEBIT');
  });
});
test('eventos são classes concretas com envelope estável e dados imutáveis', () => {
  const source = {
    transactionId: 't',
    walletId: 'w',
    providerId: 'p',
    externalTransactionId: 'e',
    kind: 'BET',
    status: 'PROCESSED',
    money: { amount: '10.00', currency: 'BRL' },
  };
  const event = WagerTransactionProcessed.create({
    eventId: 'event',
    aggregateId: 't',
    correlationId: 'c',
    occurredAt: new Date(),
    data: source,
  });
  source.money.amount = '999.00';
  expect(event.toJSON().data.money.amount).toBe('10.00');
  expect(Object.isFrozen(event.data.money)).toBe(true);
  expect(event.toJSON().version).toBe(1);
  expect(event.eventType).toBe('WagerTransactionProcessed');
});
test('inbox/outbox têm transições explícitas e backoff limitado', () => {
  const inbox = InboxMessage.receive({ messageId: 'm', consumerName: 'c', payloadHash: 'h' });
  inbox.markProcessed(new Date());
  expect(() => inbox.markProcessed(new Date())).toThrow();
  const at = new Date();
  const outbox = OutboxMessage.enqueue({
    eventId: 'e',
    aggregateId: 'a',
    eventType: 'T',
    occurredAt: at.toISOString(),
  });
  expect(outbox.isDue(at)).toBe(true);
  for (let i = 0; i < 25; i++) outbox.scheduleRetry(at, () => 1);
  expect(outbox.nextAttemptAt!.getTime() - at.getTime()).toBe(300000);
  outbox.markPublished(at);
  expect(outbox.isDue(at)).toBe(false);
  expect(() => outbox.scheduleRetry(at)).toThrow();
});
test('hash de negócio ignora transporte/chave, normaliza dinheiro e ausência da referência', () => {
  const body = {
    providerId: 'p',
    externalTransactionId: 'e',
    playerId: crypto.randomUUID(),
    walletId: crypto.randomUUID(),
    roundId: 'r',
    gameId: 'g',
    kind: 'BET',
    money: { amount: '001.00', currency: 'BRL' },
  };
  expect(businessHash(parseCommand(body, 'key'))).toBe(
    businessHash(parseCommand({ ...body, money: { amount: '1.00', currency: 'BRL' } }, 'other-key')),
  );
  expect(() => parseCommand({ ...body, kind: 'OPENING' }, 'key')).toThrow();
  expect(() => parseCommand(body, undefined)).toThrow();
});
