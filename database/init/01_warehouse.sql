create user warehouse with password 'warehouse';
create database warehouse owner warehouse;

\connect warehouse

set role warehouse;

create schema raw;
create schema staging;
create schema consume;

create table raw.products_snapshot (
    snapshot_date date not null,
    id integer not null,
    payload jsonb not null,
    run_id text not null,
    loaded_at timestamptz not null default now(),
    primary key (snapshot_date, id)
);

create table raw.carts_snapshot (
    snapshot_date date not null,
    id integer not null,
    payload jsonb not null,
    run_id text not null,
    loaded_at timestamptz not null default now(),
    primary key (snapshot_date, id)
);

create table raw.load_audit (
    snapshot_date date not null,
    resource text not null,
    row_count integer not null,
    run_id text not null,
    loaded_at timestamptz not null default now()
);

create table raw.dq_results (
    snapshot_date date not null,
    resource text not null,
    rule text not null,
    field text not null,
    checked integer not null,
    failed integer not null,
    expected_accuracy numeric(5, 4),
    measured_accuracy numeric(5, 4) not null,
    passed boolean not null,
    sample_failed_ids jsonb not null,
    run_id text not null,
    evaluated_at timestamptz not null default now()
);
