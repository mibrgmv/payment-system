create type currency_code as enum (
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

create
or replace function update_updated_at()
returns trigger as $$
begin
    new.updated_at
= now();
return new;
end;
$$
language plpgsql;

create trigger accounts_updated_at_trigger
    before update
    on accounts
    for each row
    execute function update_updated_at();

create table processed_events
(
    event_id       varchar(255) primary key,
    event_type     varchar(100) not null,
    source_service varchar(100) not null,
    processed_at   timestamptz  not null default now(),
    created_at     timestamptz  not null default now()
);

create index idx_processed_events_event_id on processed_events (event_id);
create index idx_processed_events_created_at on processed_events (created_at);
create index idx_processed_events_source_service on processed_events (source_service);

create table outbox_events
(
    event_id      varchar(255) primary key ,
    event_type    varchar(100) not null,
    topic         varchar(255) not null,
    payload       jsonb        not null,
    status        varchar(50)  not null default 'pending',
    retry_count   integer      not null default 0,
    error_message text,
    created_at    timestamptz  not null default now(),
    published_at  timestamptz,
    updated_at    timestamptz  not null default now()
);

create index idx_outbox_events_status on outbox_events (status);
create index idx_outbox_events_created_at on outbox_events (created_at);