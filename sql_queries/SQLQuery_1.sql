-- create tables
create table senders (
    sender_id       smallserial PRIMARY KEY,
    first_name      character varying (50) NOT NULL,
    last_name       character varying (50) NOT NULL,
    phone_number    character (8),
    hire_date       date,
    annual_salary   numeric(10, 2)
);

create table receivers (
    receiver_id int2 generated always as identity (start with 1 increment by 1) PRIMARY KEY,
    first_name       character varying (50) not null,
    last_name        character varying (50) not null,
    phone_number     character (8),
    hire_date        date,
    annual_salary    numeric(10, 2)
);

create table transactions (
    transaction_id int2 generated always as identity (start with 1 increment by 1) PRIMARY KEY,
    sender_id int2      not null REFERENCES senders (sender_id),
    receiver_id int2    not null REFERENCES receivers (receiver_id),
    transaction_date    date,
    amount              numeric(10, 2) CHECK (amount > 0)
);

-- create indices
create index transactions_sender_id_idx
on transactions (sender_id);

-- alter indices
alter index test_table_pkey
rename to senders_pkey;

drop index transactions_pkey;

-- alter sequences
alter SEQUENCE test_table_account_id_seq
RENAME to senders_sender_id_seq;

-- alter tables
alter table receivers
add CONSTRAINT receivers_annual_salary_chk CHECK (annual_salary > 0);


-- insert values
INSERT INTO receivers(first_name, last_name, phone_number, hire_date, annual_salary) 
    VALUES ('Barack', 'Obama', '83838383', '20220223', 180000.00),
           ('Donald', 'Trump', '13344455', '20220220', 200000.00);

INSERT INTO transactions(sender_id, receiver_id, transaction_date, amount)
    VALUES (1, 1, '20220228', 10000.00),
           (1, 2, '20220323', 23000.00),
           (2, 3, '20220312', 100.00);

select * from test_table;


-- query
select * from transactions;
select * from senders;
select * from receivers;

select * from pg_indexes where schemaname='public';
select * from pg_stat_user_indexes;

select * from pg_sequences where schemaname='public';

select * from information_schema.columns
where table_name='transactions';

explain (analyze)
select
    t.transaction_id as "Transaction ID",
    t.transaction_date as "Transaction Date",
    concat(s.first_name, ' ', s.last_name) as "Sender Full Name"
from transactions as t
    join senders as s on t.sender_id=s.sender_id
where t.sender_id=2;

select *
from senders
where sender_id = 1;

