export class DomainError extends Error {
  constructor(
    public readonly code: string,
    message = code,
  ) {
    super(message);
    this.name = 'DomainError';
  }
}
export class InvalidTransactionStateError extends DomainError {
  constructor() {
    super('INVALID_TRANSACTION_STATE');
  }
}
