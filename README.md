# ТЗ

## функциональные требования

### deposit service

- возможность пополнить счёт
- возможность снять со счёта
- возможность просмотра баланса счёта

### transfer service

- возможность перевода между счетами

## нефункциональные требования

- событие об изменении баланса должно быть обработано exactly-once
- изменение баланса в БД должно использовать транзакции
- сервис переводов должнен быть представлен в 2+ инстансах
- нагрузка на сервис пополнения должны быть сбалансирована между инстансами сервиса переводов
- к таблицам должны быть написаны индексы
- пополнение/списание должно происходить поэтапно с изменением статуса платежа

## схема данных

### deposit service

```postgresql
create type currency_type as enum ('RUB', 'USD', 'EUR');
create type operation_status as enum ('pending', 'completed', 'failed');

create table deposits
(
    deposit_id      uuid primary key,
    account_id      uuid references accounts (account_id),
    amount          numeric(15, 2)   not null,
    fee             numeric(15, 2)   not null,
    currency        currency_type    not null,
    status          operation_status not null,
    idempotency_key uuid             not null,

    constraint positive_amount check (amount > 0),
    constraint positive_fee check (fee >= 0)
);

create table accounts
(
    account_id      uuid primary key,
    user_id         uuid                     not null,
    balance         numeric(15, 2)           not null,
    blocked_balance numeric(15, 2)           not null,
    currency        currency_type            not null,
    created_at      timestamp with time zone not null,
    updated_at      timestamp with time zone not null,

    constraint positive_balance check (balance >= 0),
    constraint positive_blocked_balance check (blocked_balance >= 0)
);
```

### transfer service

```postgresql
create type currency_type as enum ('RUB', 'USD', 'EUR');
create type operation_status as enum ('pending', 'completed', 'failed');

create table transfers
(
    transfer_id     uuid primary key,
    from_account_id uuid references accounts (account_id),
    to_account_id   uuid references accounts (account_id),
    amount          numeric(15, 2)   not null,
    fee             numeric(15, 2)   not null,
    currency        currency_type    not null,
    status          operation_status not null,
    idempotency_key uuid             not null,

    constraint positive_amount check (amount > 0),
    constraint different_accounts check (from_account_id != to_account_id)
);

create table accounts
(
    account_id      uuid primary key,
    user_id         uuid                     not null,
    balance         numeric(15, 2)           not null,
    blocked_balance numeric(15, 2)           not null,
    currency        currency_type            not null,
    created_at      timestamp with time zone not null,
    updated_at      timestamp with time zone not null,

    constraint positive_balance check (balance >= 0),
    constraint positive_blocked_balance check (blocked_balance >= 0)
);
```