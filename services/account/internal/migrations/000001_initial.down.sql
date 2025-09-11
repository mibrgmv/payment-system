drop trigger if exists accounts_updated_at_trigger on accounts;
drop function if exists update_updated_at;
drop table if exists balances;
drop table if exists accounts;
drop type if exists currency_code;
drop table if exists processed_events;
drop table if exists outbox_events;