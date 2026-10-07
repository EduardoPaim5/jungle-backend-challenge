import { Migration } from '@mikro-orm/migrations';

export class Migration202610070001 extends Migration {
  override async up(): Promise<void> {
    this.addSql(`
      CREATE TABLE wallets (
        id uuid PRIMARY KEY, player_id uuid NOT NULL, currency varchar(3) NOT NULL CHECK(currency ~ '^[A-Z]{3}$'),
        balance numeric(38,2) NOT NULL CHECK(balance >= 0 AND balance <> 'NaN'::numeric),
        version integer NOT NULL CHECK(version >= 1), created_at timestamptz NOT NULL, updated_at timestamptz NOT NULL,
        UNIQUE(player_id,currency), UNIQUE(id,currency)
      );
      CREATE TABLE wager_transactions (
        id uuid PRIMARY KEY, provider_id varchar(128) NOT NULL, external_transaction_id varchar(128) NOT NULL,
        idempotency_key varchar(256) NOT NULL UNIQUE, payload_hash char(64) NOT NULL,
        wallet_id uuid NOT NULL, player_id uuid NOT NULL, round_id varchar(128) NOT NULL, game_id varchar(128) NOT NULL,
        kind text NOT NULL CHECK(kind IN ('OPENING','BET','WIN','LOSS','REFUND','ROLLBACK')),
        amount numeric(38,2) NOT NULL CHECK(amount >= 0 AND amount <> 'NaN'::numeric), currency varchar(3) NOT NULL,
        reference_external_transaction_id varchar(128), reference_transaction_id uuid REFERENCES wager_transactions(id),
        status text NOT NULL CHECK(status IN ('PENDING','PENDING_REFERENCE','PROCESSED','REJECTED','FAILED')),
        failure_code text, processed_at timestamptz, created_at timestamptz NOT NULL,
        result_snapshot jsonb, correlation_id varchar(128) NOT NULL, causation_id varchar(128),
        reference_attempts integer NOT NULL DEFAULT 0 CHECK(reference_attempts >= 0), next_attempt_at timestamptz,
        lease_token uuid, lease_until timestamptz,
        UNIQUE(provider_id,external_transaction_id), UNIQUE(id,wallet_id,currency),
        CHECK((status IN ('REJECTED','FAILED')) = (failure_code IS NOT NULL)),
        CHECK((status = 'PROCESSED') = (processed_at IS NOT NULL)),
        CHECK(kind NOT IN ('REFUND','ROLLBACK') OR reference_external_transaction_id IS NOT NULL),
        CHECK(kind NOT IN ('BET','LOSS','OPENING') OR reference_external_transaction_id IS NULL)
      );
      CREATE UNIQUE INDEX one_reversal_per_type ON wager_transactions(reference_transaction_id,kind)
        WHERE status='PROCESSED' AND kind IN ('REFUND','ROLLBACK');
      CREATE INDEX pending_references_due ON wager_transactions(next_attempt_at,id) WHERE status='PENDING_REFERENCE';
      CREATE TABLE wallet_ledger (
        id uuid PRIMARY KEY, wallet_id uuid NOT NULL, transaction_id uuid NOT NULL,
        direction text NOT NULL CHECK(direction IN ('DEBIT','CREDIT')),
        amount numeric(38,2) NOT NULL CHECK(amount > 0 AND amount <> 'NaN'::numeric), currency varchar(3) NOT NULL,
        balance_before numeric(38,2) NOT NULL CHECK(balance_before >= 0 AND balance_before <> 'NaN'::numeric),
        balance_after numeric(38,2) NOT NULL CHECK(balance_after >= 0 AND balance_after <> 'NaN'::numeric),
        wallet_version integer NOT NULL CHECK(wallet_version >= 1), created_at timestamptz NOT NULL,
        FOREIGN KEY(wallet_id,currency) REFERENCES wallets(id,currency),
        FOREIGN KEY(transaction_id,wallet_id,currency) REFERENCES wager_transactions(id,wallet_id,currency),
        UNIQUE(transaction_id,wallet_id), UNIQUE(wallet_id,wallet_version),
        CHECK(balance_after = CASE direction WHEN 'CREDIT' THEN balance_before+amount ELSE balance_before-amount END)
      );
      CREATE INDEX ledger_cursor ON wallet_ledger(wallet_id,wallet_version DESC);
      CREATE TABLE inbox_messages (
        consumer_name text NOT NULL, message_id varchar(128) NOT NULL, payload_hash char(64) NOT NULL,
        received_at timestamptz NOT NULL DEFAULT clock_timestamp(), processed_at timestamptz,
        transaction_id uuid REFERENCES wager_transactions(id), disposition text NOT NULL DEFAULT 'ACK'
          CHECK(disposition IN ('ACK','DLQ')), dead_letter_sent_at timestamptz,
        PRIMARY KEY(consumer_name,message_id)
      );
      CREATE TABLE outbox_messages (
        id uuid PRIMARY KEY, aggregate_id uuid NOT NULL, wallet_id uuid NOT NULL, event_type text NOT NULL,
        payload jsonb NOT NULL, occurred_at timestamptz NOT NULL, attempts integer NOT NULL DEFAULT 0 CHECK(attempts>=0),
        next_attempt_at timestamptz NOT NULL DEFAULT clock_timestamp(), published_at timestamptz,
        lease_token uuid, lease_until timestamptz,
        CHECK(payload->>'eventId'=id::text), CHECK(payload->>'eventType'=event_type)
      );
      CREATE INDEX outbox_due ON outbox_messages(next_attempt_at,occurred_at,id) WHERE published_at IS NULL;
      CREATE TABLE dead_letter_records (
        id uuid PRIMARY KEY, message_id text NOT NULL, payload_hash char(64) NOT NULL, body text NOT NULL,
        reason text NOT NULL, recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(), UNIQUE(message_id,payload_hash)
      );
      CREATE TABLE integration_event_receipts (
        consumer_name text NOT NULL, event_id uuid NOT NULL, payload_hash char(64) NOT NULL,
        received_at timestamptz NOT NULL DEFAULT clock_timestamp(), PRIMARY KEY(consumer_name,event_id)
      );
    `);
    this.addSql(`
      CREATE FUNCTION immutable_ledger() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN RAISE EXCEPTION 'Ledger is append-only' USING ERRCODE='23514'; END $$;
      CREATE TRIGGER ledger_immutable BEFORE UPDATE OR DELETE ON wallet_ledger FOR EACH ROW EXECUTE FUNCTION immutable_ledger();
      CREATE FUNCTION protect_transaction() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN
        IF TG_OP='DELETE' THEN RAISE EXCEPTION 'Transaction is audit-only' USING ERRCODE='23514'; END IF;
        IF ROW(NEW.id,NEW.provider_id,NEW.external_transaction_id,NEW.idempotency_key,NEW.payload_hash,NEW.wallet_id,
               NEW.player_id,NEW.round_id,NEW.game_id,NEW.kind,NEW.amount,NEW.currency,NEW.reference_external_transaction_id,NEW.created_at,
               NEW.correlation_id,NEW.causation_id)
          IS DISTINCT FROM ROW(OLD.id,OLD.provider_id,OLD.external_transaction_id,OLD.idempotency_key,OLD.payload_hash,OLD.wallet_id,
               OLD.player_id,OLD.round_id,OLD.game_id,OLD.kind,OLD.amount,OLD.currency,OLD.reference_external_transaction_id,OLD.created_at,
               OLD.correlation_id,OLD.causation_id) THEN
          RAISE EXCEPTION 'Transaction input is immutable' USING ERRCODE='23514';
        END IF;
        IF OLD.status IN ('PROCESSED','REJECTED','FAILED') AND NEW IS DISTINCT FROM OLD THEN
          RAISE EXCEPTION 'Terminal transaction is immutable' USING ERRCODE='23514';
        END IF;
        IF OLD.status='PENDING_REFERENCE' AND NEW.status='PENDING' THEN
          RAISE EXCEPTION 'Invalid transaction transition' USING ERRCODE='23514';
        END IF;
        RETURN NEW;
      END $$;
      CREATE TRIGGER transaction_immutable BEFORE UPDATE OR DELETE ON wager_transactions FOR EACH ROW EXECUTE FUNCTION protect_transaction();
      CREATE FUNCTION check_wallet_movement() RETURNS trigger LANGUAGE plpgsql AS $$
      DECLARE latest wallet_ledger; movement wallet_ledger;
      BEGIN
        IF TG_OP='INSERT' THEN
          IF NEW.version<>1 THEN RAISE EXCEPTION 'Wallet must open at version 1' USING ERRCODE='23514'; END IF;
          IF NEW.balance>0 AND NOT EXISTS(SELECT 1 FROM wallet_ledger l JOIN wager_transactions t ON t.id=l.transaction_id
             WHERE l.wallet_id=NEW.id AND l.wallet_version=1 AND l.balance_before=0 AND l.balance_after=NEW.balance
               AND l.direction='CREDIT' AND t.kind='OPENING') THEN
            RAISE EXCEPTION 'Opening credit missing' USING ERRCODE='23514';
          END IF;
        ELSE
          IF ROW(NEW.id,NEW.player_id,NEW.currency,NEW.created_at) IS DISTINCT FROM ROW(OLD.id,OLD.player_id,OLD.currency,OLD.created_at) THEN
            RAISE EXCEPTION 'Wallet identity is immutable' USING ERRCODE='23514';
          END IF;
          IF NEW.balance=OLD.balance AND NEW.version<>OLD.version THEN
            RAISE EXCEPTION 'Version without balance movement' USING ERRCODE='23514';
          END IF;
          IF NEW.balance<>OLD.balance THEN
            SELECT * INTO movement FROM wallet_ledger WHERE wallet_id=NEW.id AND wallet_version=NEW.version;
            IF NEW.version<>OLD.version+1 OR movement.id IS NULL OR movement.balance_before<>OLD.balance OR movement.balance_after<>NEW.balance THEN
              RAISE EXCEPTION 'Wallet movement missing matching ledger' USING ERRCODE='23514';
            END IF;
          END IF;
        END IF;
        SELECT * INTO latest FROM wallet_ledger WHERE wallet_id=NEW.id ORDER BY wallet_version DESC LIMIT 1;
        IF latest.id IS NULL THEN
          IF NEW.balance<>0 OR NEW.version<>1 THEN RAISE EXCEPTION 'Wallet ledger missing' USING ERRCODE='23514'; END IF;
        ELSIF latest.balance_after<>NEW.balance OR latest.wallet_version<>NEW.version THEN
          RAISE EXCEPTION 'Wallet and ledger disagree' USING ERRCODE='23514';
        END IF;
        RETURN NULL;
      END $$;
      CREATE CONSTRAINT TRIGGER wallet_movement AFTER INSERT OR UPDATE ON wallets DEFERRABLE INITIALLY DEFERRED
        FOR EACH ROW EXECUTE FUNCTION check_wallet_movement();
      CREATE FUNCTION check_ledger_movement() RETURNS trigger LANGUAGE plpgsql AS $$
      DECLARE tx wager_transactions; previous wallet_ledger; current_wallet wallets; latest wallet_ledger;
      BEGIN
        SELECT * INTO tx FROM wager_transactions WHERE id=NEW.transaction_id;
        SELECT * INTO current_wallet FROM wallets WHERE id=NEW.wallet_id;
        IF tx.status<>'PROCESSED' OR tx.kind='LOSS' OR tx.amount<>NEW.amount OR tx.player_id<>current_wallet.player_id THEN
          RAISE EXCEPTION 'Ledger must match processed financial operation' USING ERRCODE='23514';
        END IF;
        IF NEW.direction<>(CASE WHEN tx.kind='BET' THEN 'DEBIT' WHEN tx.kind='ROLLBACK' THEN
          CASE WHEN (SELECT kind FROM wager_transactions WHERE id=tx.reference_transaction_id)='BET' THEN 'CREDIT' ELSE 'DEBIT' END ELSE 'CREDIT' END) THEN
          RAISE EXCEPTION 'Incorrect ledger direction' USING ERRCODE='23514';
        END IF;
        IF NEW.wallet_version=1 THEN
          IF tx.kind<>'OPENING' OR NEW.balance_before<>0 THEN RAISE EXCEPTION 'Invalid opening ledger' USING ERRCODE='23514'; END IF;
        ELSE
          IF tx.kind='OPENING' THEN RAISE EXCEPTION 'Late opening not allowed' USING ERRCODE='23514'; END IF;
          SELECT * INTO previous FROM wallet_ledger WHERE wallet_id=NEW.wallet_id AND wallet_version=NEW.wallet_version-1;
          IF previous.id IS NULL THEN
            IF NEW.wallet_version<>2 OR NEW.balance_before<>0 THEN RAISE EXCEPTION 'Ledger version gap' USING ERRCODE='23514'; END IF;
          ELSIF previous.balance_after<>NEW.balance_before THEN RAISE EXCEPTION 'Ledger chain broken' USING ERRCODE='23514'; END IF;
        END IF;
        SELECT * INTO latest FROM wallet_ledger WHERE wallet_id=NEW.wallet_id ORDER BY wallet_version DESC LIMIT 1;
        IF current_wallet.balance<>latest.balance_after OR current_wallet.version<>latest.wallet_version THEN
          RAISE EXCEPTION 'Orphan financial ledger' USING ERRCODE='23514';
        END IF;
        RETURN NULL;
      END $$;
      CREATE CONSTRAINT TRIGGER ledger_movement AFTER INSERT ON wallet_ledger DEFERRABLE INITIALLY DEFERRED
        FOR EACH ROW EXECUTE FUNCTION check_ledger_movement();
      CREATE FUNCTION check_transaction_ledger() RETURNS trigger LANGUAGE plpgsql AS $$
      DECLARE tx wager_transactions; entries integer;
      BEGIN
        SELECT * INTO tx FROM wager_transactions WHERE id=NEW.id;
        SELECT count(*) INTO entries FROM wallet_ledger WHERE transaction_id=NEW.id;
        IF tx.status='PENDING' OR (tx.status IN ('PROCESSED','REJECTED','FAILED','PENDING_REFERENCE') AND tx.result_snapshot IS NULL) THEN
          RAISE EXCEPTION 'Unfinished transaction cannot commit' USING ERRCODE='23514';
        END IF;
        IF tx.status='PROCESSED' AND tx.kind<>'LOSS' AND entries<>1 THEN
          RAISE EXCEPTION 'Processed transaction requires ledger' USING ERRCODE='23514';
        END IF;
        IF (tx.status<>'PROCESSED' OR tx.kind='LOSS') AND entries<>0 THEN
          RAISE EXCEPTION 'Non-financial transaction cannot have ledger' USING ERRCODE='23514';
        END IF;
        RETURN NULL;
      END $$;
      CREATE CONSTRAINT TRIGGER transaction_ledger AFTER INSERT OR UPDATE ON wager_transactions DEFERRABLE INITIALLY DEFERRED
        FOR EACH ROW EXECUTE FUNCTION check_transaction_ledger();
      GRANT USAGE ON SCHEMA public TO jungle_app;
      GRANT SELECT,INSERT,UPDATE ON wallets,wager_transactions,inbox_messages,outbox_messages TO jungle_app;
      GRANT SELECT,INSERT ON wallet_ledger,dead_letter_records,integration_event_receipts TO jungle_app;
    `);
  }
  override async down(): Promise<void> {
    this
      .addSql(`DROP TABLE integration_event_receipts,dead_letter_records,outbox_messages,inbox_messages,wallet_ledger,wager_transactions,wallets CASCADE;
      DROP FUNCTION immutable_ledger(),protect_transaction(),check_wallet_movement(),check_ledger_movement(),check_transaction_ledger();`);
  }
}
