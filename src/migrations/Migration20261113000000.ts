import { Migration } from '@mikro-orm/migrations';

export class Migration20261113000000 extends Migration {
  async up(): Promise<void> {
    this.addSql(`alter table wager_transaction
      add column reference_attempts integer not null default 0 check (reference_attempts >= 0),
      add column reference_next_attempt_at timestamptz,
      add column reference_expires_at timestamptz;
      alter table outbox_message add column correlation_id varchar(200), add column causation_id varchar(200);
      create index wager_pending_reference_due_idx on wager_transaction(reference_next_attempt_at, created_at)
        where status='PENDING_REFERENCE';`);

    this.addSql(`create function validate_wager_reference() returns trigger language plpgsql as $$
      declare referenced wager_transaction%rowtype;
      begin
        if new.reference_external_transaction_id is null then
          if new.reference_transaction_id is not null then raise exception 'reference id without external reference' using errcode='23514'; end if;
          return new;
        end if;
        if new.reference_transaction_id = new.id then raise exception 'self reference is not allowed' using errcode='23514'; end if;
        if new.status not in ('PROCESSED','PENDING_REFERENCE') then return new; end if;
        select * into referenced from wager_transaction
          where provider_id=new.provider_id and external_transaction_id=new.reference_external_transaction_id;
        if not found then
          if new.status <> 'PENDING_REFERENCE' then raise exception 'processed reference is missing' using errcode='23514'; end if;
          return new;
        end if;
        if referenced.id = new.id or referenced.player_id <> new.player_id or referenced.wallet_id <> new.wallet_id
          or referenced.currency <> new.currency or referenced.round_id <> new.round_id then
          raise exception 'reference context mismatch' using errcode='23514';
        end if;
        if new.reference_transaction_id is not null and new.reference_transaction_id <> referenced.id then
          raise exception 'reference identity mismatch' using errcode='23514';
        end if;
        if new.status = 'PROCESSED' and (referenced.status <> 'PROCESSED'
          or (new.kind='REFUND' and (referenced.kind <> 'BET' or new.amount <> referenced.amount))
          or (new.kind='ROLLBACK' and (referenced.kind not in ('BET','WIN','REFUND') or new.amount <> referenced.amount))
          or (new.kind='WIN' and referenced.kind <> 'BET')) then
          raise exception 'invalid processed reference' using errcode='23514';
        end if;
        return new;
      end $$;
      create trigger wager_reference_guard before insert or update of provider_id,external_transaction_id,player_id,wallet_id,round_id,kind,amount,currency,reference_external_transaction_id,reference_transaction_id,status
        on wager_transaction for each row execute function validate_wager_reference();`);

    this.addSql(`create function validate_transaction_ledger() returns trigger language plpgsql as $$
      declare target_id uuid; tx wager_transaction%rowtype; ledger_count integer; entry wallet_ledger_entry%rowtype;
      begin
        if tg_table_name='wager_transaction' then target_id := coalesce(new.id,old.id);
        else target_id := coalesce(new.transaction_id,old.transaction_id); end if;
        select * into tx from wager_transaction where id=target_id;
        if not found then return null; end if;
        select count(*) into ledger_count from wallet_ledger_entry where transaction_id=target_id;
        if (tx.status='PROCESSED' and tx.kind<>'LOSS' and ledger_count<>1)
          or ((tx.status<>'PROCESSED' or tx.kind='LOSS') and ledger_count<>0) then
          raise exception 'transaction and ledger entries are inconsistent' using errcode='23514';
        end if;
        if ledger_count=1 then
          select * into entry from wallet_ledger_entry where transaction_id=target_id;
          if entry.wallet_id<>tx.wallet_id or entry.currency<>tx.currency or entry.amount<>tx.amount
            or (tx.kind='BET' and entry.direction<>'DEBIT')
            or (tx.kind in ('WIN','REFUND') and entry.direction<>'CREDIT')
            or (tx.kind='ROLLBACK' and (entry.direction not in ('CREDIT','DEBIT') or entry.amount<>tx.amount)) then
            raise exception 'transaction and ledger entry fields are inconsistent' using errcode='23514';
          end if;
        end if;
        return null;
      end $$;
      create constraint trigger wager_transaction_ledger_guard after insert or update on wager_transaction
        deferrable initially deferred for each row execute function validate_transaction_ledger();
      create constraint trigger ledger_transaction_guard after insert on wallet_ledger_entry
        deferrable initially deferred for each row execute function validate_transaction_ledger();`);

    this.addSql(`create function validate_wallet_ledger_balance() returns trigger language plpgsql as $$
      declare target_wallet uuid; stored numeric(19,2); calculated numeric(19,2);
      begin
        if tg_table_name='wallet' then target_wallet := coalesce(new.id,old.id);
        else target_wallet := coalesce(new.wallet_id,old.wallet_id); end if;
        select balance into stored from wallet where id=target_wallet;
        if not found then return null; end if;
        select coalesce(sum(case when direction='CREDIT' then amount else -amount end),0)::numeric(19,2)
          into calculated from wallet_ledger_entry where wallet_id=target_wallet;
        if stored<>calculated then raise exception 'wallet balance differs from immutable ledger' using errcode='23514'; end if;
        return null;
      end $$;
      create constraint trigger wallet_ledger_balance_guard after insert or update on wallet
        deferrable initially deferred for each row execute function validate_wallet_ledger_balance();
      create constraint trigger ledger_wallet_balance_guard after insert on wallet_ledger_entry
        deferrable initially deferred for each row execute function validate_wallet_ledger_balance();`);

    this.addSql(`create function guard_wager_status_transition() returns trigger language plpgsql as $$
      begin
        if old.status in ('PROCESSED','REJECTED','FAILED') and new.status<>old.status then
          raise exception 'terminal wager status cannot transition' using errcode='23514';
        end if;
        if old.status='PENDING_REFERENCE' and new.status not in ('PENDING_REFERENCE','PROCESSED','REJECTED') then
          raise exception 'invalid pending reference transition' using errcode='23514';
        end if;
        return new;
      end $$;
      create trigger wager_status_transition_guard before update of status on wager_transaction
        for each row execute function guard_wager_status_transition();`);
  }

  async down(): Promise<void> {
    this.addSql(`drop trigger if exists wager_status_transition_guard on wager_transaction;
      drop function if exists guard_wager_status_transition();
      drop trigger if exists ledger_wallet_balance_guard on wallet_ledger_entry;
      drop trigger if exists wallet_ledger_balance_guard on wallet;
      drop function if exists validate_wallet_ledger_balance();
      drop trigger if exists ledger_transaction_guard on wallet_ledger_entry;
      drop trigger if exists wager_transaction_ledger_guard on wager_transaction;
      drop function if exists validate_transaction_ledger();
      drop trigger if exists wager_reference_guard on wager_transaction;
      drop function if exists validate_wager_reference();
      drop index if exists wager_pending_reference_due_idx;
      alter table outbox_message drop column if exists causation_id, drop column if exists correlation_id;
      alter table wager_transaction drop column if exists reference_expires_at, drop column if exists reference_next_attempt_at, drop column if exists reference_attempts;`);
  }
}
