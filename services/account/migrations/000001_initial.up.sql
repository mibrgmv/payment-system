create type currency_code as enum ('RUB', 'USD', 'EUR');

create table accounts
(
    account_id uuid primary key        default gen_random_uuid(),
    user_id    uuid           not null,
    balance    numeric(15, 2) not null default 0,
    currency   currency_code  not null,
    created_at timestamptz    not null default now(),
    updated_at timestamptz    not null default now(),

    constraint positive_balance check (balance >= 0),
);

create index idx_accounts_user_id on accounts (user_id);
create index idx_accounts_balance on accounts (balance);