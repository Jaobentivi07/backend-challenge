import { Migration } from '@mikro-orm/migrations';

export class Migration20261115000000 extends Migration {
  async up(): Promise<void> {
    this.addSql(`create or replace function validate_transaction_ledger() returns trigger language plpgsql as $$
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
            or (tx.kind='OPENING' and entry.direction<>'CREDIT') then
            raise exception 'transaction and ledger entry fields are inconsistent' using errcode='23514';
          end if;
        end if;
        return null;
      end $$;
      create or replace function validate_wallet_ledger_balance() returns trigger language plpgsql as $$
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
      end $$;`);
  }

  async down(): Promise<void> {
    // This migration only replaces trigger functions with equivalent branch-safe implementations.
  }
}
