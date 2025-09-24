create table if not exists processed_events
(
    event_id       varchar(255) primary key,
    event_type     varchar(100) not null,
    source_service varchar(100) not null,
    processed_at   timestamptz  not null default now()
    );

create index idx_processed_events_event_id on processed_events (event_id);
create index idx_processed_events_processed_at on processed_events (processed_at);
create index idx_processed_events_source_service on processed_events (source_service);

create table if not exists outbox_events
(
    event_id      varchar(255) primary key,
    event_type    varchar(100) not null,
    topic         varchar(255) not null,
    payload       jsonb        not null,
    status        varchar(50)  not null default 'pending',
    retry_count   integer      not null default 0,
    max_retries   integer      not null default 5,
    error_message text,
    created_at    timestamptz  not null default now(),
    published_at  timestamptz,
    updated_at    timestamptz  not null default now(),
    next_retry_at timestamptz
);

create index idx_outbox_events_status on outbox_events (status);

create index idx_outbox_events_created_at on outbox_events (created_at);

create index idx_outbox_events_pending on outbox_events (created_at)
    where status = 'pending';

create index idx_outbox_events_pending_retry on outbox_events (status, next_retry_at)
    where status = 'failed'
    and next_retry_at is not null;