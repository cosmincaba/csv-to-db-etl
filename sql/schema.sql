create table if not exists  customers (
    customer_id integer primary key
    ,full_name text not null
    ,email text not null
    ,created_at timestamp not null
);

create table if not exists transactions (
    transaction_id text primary key
    ,customer_id integer not null references customers(customer_id)
    ,amount numeric(12,2) not null
    ,currency text not null
    ,transaction_date date not null
);

