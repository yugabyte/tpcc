-- Use PG for this to save time.
-- The results are the same on YB and on PG.

\i real-data/cr-tables-pg.sql
\i real-data/insert-data.dump
\i real-data/cr-indexes-and-constraints-pg.sql

\i real-data-expected-results/cr-tables-pg.sql
\i real-data-expected-results/cr-insert-data-dump/do-insert-to-result-tables.sql

--------------------------------------------------
drop table if exists item_expected_result cascade;
create table item_expected_result(
  i_id             int not null,
  i_name           varchar(24) not null,
  i_price          numeric(5,2) not null,
  i_data           varchar(50) not null,
  i_im_id          int not null,

  primary key(i_id)
);

insert into item_expected_result(
  i_id, i_price, i_im_id, i_name, i_data)
select
  i_id, i_price, i_im_id, i_name, i_data
from item;
--------------------------------------------------

\i mini-data/cr-tables-pg.sql
\i new-order-implementations/cr-new-order-parameters.sql

\i mini-data/cr-insert-data.sql
