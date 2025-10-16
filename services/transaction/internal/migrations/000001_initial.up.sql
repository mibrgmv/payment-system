create type transaction_type as enum (
    'transfer',
    'deposit',
    'withdrawal'
);

create type transaction_status as enum (
    'pending',
    'processing',
    'completed',
    'failed',
    'cancelled'
);

create type currency_code as enum (
    'RUB',
    'USD',
    'EUR'
);

create table if not exists transactions
(
    transaction_id  uuid primary key            default gen_random_uuid(),
    type            transaction_type   not null,
    from_account_id uuid null,
    to_account_id   uuid null,
    amount          decimal(19, 4)     not null check (amount > 0),
    currency        currency_code      not null,
    status          transaction_status not null default 'pending',
    idempotency_key varchar(255)       not null unique,
    error_message   text null,
    created_at      timestamptz        not null default now(),
    updated_at      timestamptz        not null default now(),
    completed_at    timestamptz null,

    constraint valid_from_account check (
        (type in ('transfer', 'withdrawal') and from_account_id is not null) or
        (type = 'deposit' and from_account_id is null)
    ),
    constraint valid_to_account check (
        (type in ('transfer', 'deposit') and to_account_id is not null) or
        (type = 'withdrawal' and to_account_id is null)
    ),
    constraint valid_account_combination check (
        from_account_id is distinct from to_account_id or
        (from_account_id is null and to_account_id is null)
    )
);

create index idx_transactions_status on transactions (status);
create index idx_transactions_idempotency on transactions (idempotency_key);
create index idx_transactions_from_account on transactions (from_account_id) where from_account_id is not null;
create index idx_transactions_to_account on transactions (to_account_id) where to_account_id is not null;
create index idx_transactions_created_at on transactions (created_at);
create index idx_transactions_user_activity on transactions (from_account_id, to_account_id, created_at);

create or replace function update_updated_at()
returns trigger as $$
begin
    new.updated_at = now();
    return new;
end;
$$ language plpgsql;

create trigger transactions_updated_at_trigger
    before update on transactions
    for each row
    execute function update_updated_at();