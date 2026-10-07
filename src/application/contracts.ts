import { z } from 'zod';
import { createHash } from 'node:crypto';
import { Money } from '../domain/money.js';
import { DomainError } from '../domain/errors.js';
import type { WagerCommand, TransactionStatus } from '../domain/transaction.js';
import type { MoneyProps } from '../domain/money.js';

const identifier = z
  .string()
  .min(1)
  .max(128)
  .refine((x) => x.trim().length > 0 && !/[\u0000-\u001f\u007f]/.test(x));
const moneySchema = z.object({ amount: z.string().max(128), currency: z.string().regex(/^[A-Z]{3}$/) });
export const walletInput = z.object({ playerId: z.uuid(), initialBalance: moneySchema });
const wageringInput = z
  .object({
    providerId: identifier.refine((x) => x !== '__system__'),
    externalTransactionId: identifier,
    playerId: z.uuid(),
    walletId: z.uuid(),
    roundId: identifier,
    gameId: identifier,
    kind: z.enum(['BET', 'WIN', 'LOSS', 'REFUND', 'ROLLBACK']),
    money: moneySchema,
    referenceExternalTransactionId: identifier.optional(),
  })
  .superRefine((p, ctx) => {
    if (['REFUND', 'ROLLBACK'].includes(p.kind) && !p.referenceExternalTransactionId)
      ctx.addIssue({ code: 'custom', message: 'REFERENCE_REQUIRED' });
    if (['BET', 'LOSS'].includes(p.kind) && p.referenceExternalTransactionId)
      ctx.addIssue({ code: 'custom', message: 'REFERENCE_NOT_ALLOWED' });
  });
export interface ProcessingResult {
  transactionId: string;
  status: TransactionStatus;
  balance?: MoneyProps;
  walletVersion?: number;
  failureCode?: string;
  idempotentReplay: boolean;
}
export interface ProcessingContext {
  source: 'http' | 'sqs' | 'reference';
  correlationId: string;
  causationId?: string;
  permanentFailureCode?: string;
  inbox?: { messageId: string; payloadHash: string };
  referenceLease?: { transactionId: string; token: string };
}
export class ServiceError extends Error {
  constructor(
    public readonly code: string,
    public readonly status: number,
    public readonly retryable = false,
  ) {
    super(code);
  }
}
export function parseCommand(body: unknown, key: unknown): WagerCommand {
  const p = wageringInput.safeParse(body);
  if (
    !p.success ||
    typeof key !== 'string' ||
    !key.trim() ||
    key.length > 256 ||
    /[\u0000-\u001f\u007f]/.test(key)
  )
    throw new ServiceError('INVALID_REQUEST', 400);
  const money = normalizeMoney(p.data.money);
  return {
    ...p.data,
    playerId: p.data.playerId.toLowerCase(),
    walletId: p.data.walletId.toLowerCase(),
    money,
    idempotencyKey: key,
  };
}
export function normalizeMoney(p: MoneyProps): MoneyProps {
  try {
    const money = Money.from(p);
    if (money.currency !== 'BRL') throw new DomainError('UNSUPPORTED_CURRENCY');
    return money.toJSON();
  } catch (e) {
    if (e instanceof DomainError) throw new ServiceError(e.code, 400);
    throw e;
  }
}
export function canonical(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(canonical).join(',')}]`;
  if (value && typeof value === 'object')
    return `{${Object.entries(value)
      .filter(([, v]) => v !== undefined)
      .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
      .map(([k, v]) => `${JSON.stringify(k)}:${canonical(v)}`)
      .join(',')}}`;
  return JSON.stringify(value) ?? 'null';
}
export function hash(value: unknown): string {
  return createHash('sha256').update(canonical(value)).digest('hex');
}
export function businessHash(command: WagerCommand): string {
  const { idempotencyKey: _key, ...business } = command;
  return hash({
    ...business,
    referenceExternalTransactionId: command.referenceExternalTransactionId ?? null,
  });
}
export function resultHttpStatus(result: ProcessingResult): number {
  return result.status === 'PROCESSED'
    ? 200
    : result.status === 'REJECTED'
      ? 422
      : result.status === 'FAILED'
        ? 500
        : 202;
}
