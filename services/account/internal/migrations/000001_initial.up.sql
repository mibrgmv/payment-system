create type currency_code as enum
(
    'RUB',
    'USD',
    'EUR'
);

create table accounts
(
    account_id uuid primary key       default gen_random_uuid(),
    user_id    uuid          not null,
    currency   currency_code not null,
    created_at timestamptz   not null default now(),
    updated_at timestamptz   not null default now()
);

create index idx_accounts_user_id on accounts (user_id);

create table balances
(
    account_id   uuid primary key references accounts (account_id),
    amount       decimal(19, 4) not null default 0 check (amount >= 0),
    last_updated timestamptz    not null default now()
);

create index idx_balances_amount on balances (amount);

create or replace function update_updated_at()
returns trigger as $$
begin
    new.updated_at = now();
    return new;
end;
$$ language plpgsql;

create trigger accounts_updated_at_trigger
    before update on accounts
    for each row
    execute function update_updated_at();

create table processed_events (
    event_id varchar(255) primary key ,
    account_id uuid not null,
    processed_at timestamptz not null default now()
);

create index idx_processed_events_account_id on processed_events (account_id);