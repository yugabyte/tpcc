set client_min_messages = error; -- Issue #738.

-- Starting data. Choose between PG and YB.
    \i mini-data/pg.sql
--  \i mini-data/yb.sql

\i new-order-implementations/cr-the-order-lines.sql

--------------------------------------------------------------------------------
-- Choose the implementation of new_order() of interest.
-- Simply put it last in the list!

\i new-order-implementations/cr-new-order-2-for-loop.sql
\i new-order-implementations/cr-new-order-3-pure-sql.sql

\i new-order-implementations/prepare-insert-order-line.sql
\i new-order-implementations/prepare-update-stock.sql

--------------------------------------------------------------------------------
/*
\i mini-data/insert-data.sql

-- THE BIND "VARIABES"
\set w_id_in         1
\set d_id_in         4
\set c_id_in        21
\set o_all_local_in  1
\set d_next_o_id_v   4000

select order_lines() as ols
\gset x_
\set ols_in '\'':x_ols'\'::order_line_t[]'

\t on
select ':w_id_in        =  '||:w_id_in::text;
select ':d_id_in        =  '||:d_id_in::text;
select ':d_next_o_id_v  =  '||:d_next_o_id_v::text;

select ':ols_in         =  '||:ols_in::text;
select
  j,
  (order_lines())[j].i_id,
  (order_lines())[j].w_id,
  (order_lines())[j].qty
from generate_subscripts(:ols_in, 1) as g(j);
\t off

call new_order(
  w_id_in        => :w_id_in,
  d_id_in        => :d_id_in,
  c_id_in        => :c_id_in,
  o_all_local_in => :o_all_local_in,
  ols_in         => :ols_in);

\i new-order-implementations/qa-query.sql
*/;
--------------------------------------------------------------------------------
