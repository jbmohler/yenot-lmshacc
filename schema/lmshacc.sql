create extension if not exists "uuid-ossp";

create schema hacc;

create table hacc.accounttypes (
  id uuid primary key default uuid_generate_v1mc(),
  atype_name varchar(20) check(char_length(atype_name)>2),
  description text,
  balance_sheet boolean,
  debit boolean,
  retained_earnings boolean,
  sort integer
);

create table hacc.journals (
  id uuid primary key default uuid_generate_v1mc(),
  jrn_name varchar(20) check(char_length(jrn_name)>2),
  description text
);


create table hacc.accounts (
  id uuid primary key default uuid_generate_v1mc(),
  type_id uuid not null references hacc.accounttypes(id),
  journal_id uuid not null references hacc.journals(id),
  acc_name varchar(20) check(char_length(acc_name)>2),
  description varchar(60) check(char_length(description)>2),
  acc_note text,
  rec_note text,
  contact_keywords text,
  retearn_id uuid,
  instname text,
  instaddr1 text,
  instaddr2 text,
  instcity text,
  inststate text,
  instzip text
);

create table hacc.transactions (
  tid uuid primary key default uuid_generate_v1mc(),
  trandate date,
  tranref varchar(15),
  payee text,
  memo text,
  receipt text
);


create table hacc.tags (
  id uuid primary key default uuid_generate_v1mc(),
  tag_name varchar(20) check(char_length(tag_name)>2),
  description text
);

create table hacc.splits (
  sid uuid primary key default uuid_generate_v1mc(),
  stid uuid not null references hacc.transactions(tid),
  account_id uuid not null references hacc.accounts(id),
  sum numeric(10,2)
);

create index stid_idx on hacc.splits(stid);

create table hacc.tagsplits (
  tag_id uuid not null references hacc.tags(id),
  split_id uuid not null references hacc.splits(sid),
  primary key(tag_id, split_id)
);
