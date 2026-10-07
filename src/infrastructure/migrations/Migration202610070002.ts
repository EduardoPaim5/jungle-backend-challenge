import { Migration } from '@mikro-orm/migrations';

/** Defense in depth for persisted references, snapshots and message identity. */
export class Migration202610070002 extends Migration {
  override async up(): Promise<void> {
    this.addSql(`
      ALTER TABLE wager_transactions ADD CONSTRAINT transaction_currency CHECK(currency ~ '^[A-Z]{3}$');
      CREATE FUNCTION check_transaction_reference_and_snapshot() RETURNS trigger LANGUAGE plpgsql AS $$
      DECLARE tx wager_transactions; ref wager_transactions; movement wallet_ledger;
      BEGIN
        SELECT * INTO tx FROM wager_transactions WHERE id=NEW.id;
        IF (tx.result_snapshot->>'transactionId') IS DISTINCT FROM tx.id::text
          OR (tx.result_snapshot->>'status') IS DISTINCT FROM tx.status
          OR (tx.result_snapshot->>'failureCode') IS DISTINCT FROM tx.failure_code
          OR (tx.result_snapshot->>'idempotentReplay') IS DISTINCT FROM 'false' THEN
          RAISE EXCEPTION 'Transaction snapshot disagrees with decision' USING ERRCODE='23514';
        END IF;
        IF tx.status='PROCESSED' AND tx.reference_external_transaction_id IS NOT NULL THEN
          SELECT * INTO ref FROM wager_transactions WHERE id=tx.reference_transaction_id;
          IF ref.id IS NULL OR ref.status<>'PROCESSED'
            OR ROW(tx.provider_id,tx.player_id,tx.wallet_id,tx.currency,tx.round_id,tx.reference_external_transaction_id)
              IS DISTINCT FROM ROW(ref.provider_id,ref.player_id,ref.wallet_id,ref.currency,ref.round_id,ref.external_transaction_id)
            OR ref.id=tx.id
            OR (tx.kind IN ('WIN','REFUND') AND ref.kind<>'BET')
            OR (tx.kind='ROLLBACK' AND ref.kind NOT IN ('BET','WIN','REFUND'))
            OR (tx.kind IN ('REFUND','ROLLBACK') AND tx.amount<>ref.amount) THEN
            RAISE EXCEPTION 'Invalid processed reference' USING ERRCODE='23514';
          END IF;
        END IF;
        IF tx.status='PROCESSED' AND tx.kind<>'LOSS' THEN
          SELECT * INTO movement FROM wallet_ledger WHERE transaction_id=tx.id;
          IF (tx.result_snapshot->'balance'->>'amount') IS DISTINCT FROM movement.balance_after::text
            OR (tx.result_snapshot->'balance'->>'currency') IS DISTINCT FROM tx.currency
            OR (tx.result_snapshot->>'walletVersion') IS DISTINCT FROM movement.wallet_version::text THEN
            RAISE EXCEPTION 'Financial snapshot disagrees with ledger' USING ERRCODE='23514';
          END IF;
        END IF;
        RETURN NULL;
      END $$;
      CREATE CONSTRAINT TRIGGER transaction_reference_snapshot AFTER INSERT OR UPDATE ON wager_transactions
        DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION check_transaction_reference_and_snapshot();
      CREATE FUNCTION protect_message_identity() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN
        IF TG_TABLE_NAME='outbox_messages' THEN
          IF ROW(NEW.id,NEW.aggregate_id,NEW.wallet_id,NEW.event_type,NEW.payload,NEW.occurred_at)
            IS DISTINCT FROM ROW(OLD.id,OLD.aggregate_id,OLD.wallet_id,OLD.event_type,OLD.payload,OLD.occurred_at)
            OR (OLD.published_at IS NOT NULL AND NEW IS DISTINCT FROM OLD) THEN
            RAISE EXCEPTION 'Outbox identity or published outcome is immutable' USING ERRCODE='23514';
          END IF;
        ELSE
          IF ROW(NEW.consumer_name,NEW.message_id,NEW.payload_hash,NEW.received_at)
            IS DISTINCT FROM ROW(OLD.consumer_name,OLD.message_id,OLD.payload_hash,OLD.received_at)
            OR (OLD.transaction_id IS NOT NULL AND NEW.transaction_id IS DISTINCT FROM OLD.transaction_id)
            OR (OLD.processed_at IS NOT NULL AND NEW.processed_at IS DISTINCT FROM OLD.processed_at)
            OR (OLD.disposition='DLQ' AND NEW.disposition<>'DLQ') THEN
            RAISE EXCEPTION 'Inbox identity or committed decision is immutable' USING ERRCODE='23514';
          END IF;
        END IF;
        RETURN NEW;
      END $$;
      CREATE TRIGGER outbox_identity BEFORE UPDATE ON outbox_messages FOR EACH ROW EXECUTE FUNCTION protect_message_identity();
      CREATE TRIGGER inbox_identity BEFORE UPDATE ON inbox_messages FOR EACH ROW EXECUTE FUNCTION protect_message_identity();
    `);
  }
  override async down(): Promise<void> {
    this
      .addSql(`DROP TRIGGER inbox_identity ON inbox_messages; DROP TRIGGER outbox_identity ON outbox_messages;
      DROP FUNCTION protect_message_identity(); DROP TRIGGER transaction_reference_snapshot ON wager_transactions;
      DROP FUNCTION check_transaction_reference_and_snapshot(); ALTER TABLE wager_transactions DROP CONSTRAINT transaction_currency;`);
  }
}
