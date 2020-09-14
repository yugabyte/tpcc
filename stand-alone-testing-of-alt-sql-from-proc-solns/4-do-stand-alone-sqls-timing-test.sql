/*
________________________________________________________________________________

  ENSURE THAT THIS SET UP HAS BEEN DONE:

  \i utils/cr-is-yb.sql
  \i real-data/insert-data.sql
  \i new-order-implementations/cr-new-order-parameters.sql
  \i new-order-implementations/cr-stand-alone-insert-order-line.sql
  \i new-order-implementations/cr-stand-alone-update-stock.sql
  \i new-order-implementations/cr-timing_experiments-and-times-tables.sql
  \i new-order-implementations/cr-time-stand-alone-sqls.sql
________________________________________________________________________________
*/;

-- THE BIND "VARIABES"
\set no_of_warm_ups 100
\set no_of_repeats  5000

\i new-order-implementations/do-set_bind-variables-from-new-order-parameters.sql
select (d_next_o_id - 1) as o_id_v
from district
where d_w_id = :w_id_in and d_id = :d_id_in
\gset d_next_

\i real-data/restore-new_order-affected-tables-to-starting-state.sql

-- Issue #738. Up to you if you want to set it back to, say, "warning"
set client_min_messages = error;

call start_clock();

alter table order_line drop constraint if exists ol_fkey_o; -- << Look! 

call time_stand_alone_sqls(
  'insert-order-line',
  :no_of_warm_ups,
  :no_of_repeats,
  :d_next_o_id_v);

call time_stand_alone_sqls(
  'update-stock',
  :no_of_warm_ups,
  :no_of_repeats);

\i real-data/restore-new_order-affected-tables-to-starting-state.sql
alter table only order_line
add constraint ol_fkey_o foreign key(ol_w_id, ol_d_id, ol_o_id)
references oorder(o_w_id, o_d_id, o_id)
-- Save time.
not valid;

select 'Elapsed time for timing experiment: '||elapsed_time();
\t off

\i new-order-implementations/do-report-timing-results.sql
