import { Migration } from '@mikro-orm/migrations';

export class Migration20261114000000 extends Migration {
  async up(): Promise<void> {
    this.addSql(`alter table wager_transaction add constraint wager_no_self_reference
      check (reference_transaction_id is null or reference_transaction_id<>id);
      create or replace function validate_wager_reference() returns trigger language plpgsql as $$
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
      create or replace function guard_wager_status_transition() returns trigger language plpgsql as $$
      begin
        if old.status in ('PROCESSED','REJECTED','FAILED') then
          raise exception 'terminal wager rows are immutable' using errcode='23514';
        end if;
        if old.status='PENDING_REFERENCE' and new.status not in ('PENDING_REFERENCE','PROCESSED','REJECTED') then
          raise exception 'invalid pending reference transition' using errcode='23514';
        end if;
        return new;
      end $$;
      drop trigger if exists wager_status_transition_guard on wager_transaction;
      create trigger wager_status_transition_guard before update on wager_transaction
        for each row execute function guard_wager_status_transition();`);
  }

  async down(): Promise<void> {
    this.addSql(`alter table wager_transaction drop constraint if exists wager_no_self_reference;
      create or replace function validate_wager_reference() returns trigger language plpgsql as $$
      declare referenced wager_transaction%rowtype;
      begin
        if new.reference_external_transaction_id is null then
          if new.reference_transaction_id is not null then raise exception 'reference id without external reference' using errcode='23514'; end if;
          return new;
        end if;
        if new.reference_transaction_id = new.id then raise exception 'self reference is not allowed' using errcode='23514'; end if;
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
      end $$;`);
    this.addSql(`drop trigger if exists wager_status_transition_guard on wager_transaction;
      create or replace function guard_wager_status_transition() returns trigger language plpgsql as $$
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
}
