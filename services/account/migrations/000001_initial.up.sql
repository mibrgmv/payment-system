create type currency_code as enum ('RUB', 'USD', 'EUR');

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
    amount       numeric(19, 4) not null default 0,
    last_updated timestamptz    not null default now(),

    constraint positive_balance check (amount >= 0)
);

create index idx_balances_amount on balances (amount);