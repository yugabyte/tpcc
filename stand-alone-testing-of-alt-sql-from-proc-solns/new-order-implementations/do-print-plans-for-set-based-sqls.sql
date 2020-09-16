deallocate all;
\ir prepare-insert-order-line.sql
\ir prepare-update-stock.sql


select is_yb() as is_yb
\gset env_

\if :env_is_yb
  \t on
  \o new-order-implementations/execution-plans/plan-for-insert-order-line-YB.txt
  select 'Plan for insert_order_line';
  explain execute insert_order_line(:w_id_in, :d_id_in, :ols_in, :d_next_o_id_v);
  \o

  \o new-order-implementations/execution-plans/plan-for-update-stock-YB.txt
  select 'Plan for update_stock';
  explain execute update_stock(:w_id_in, :ols_in);
  \o
  \t off

\else
  \t on
  \o new-order-implementations/execution-plans/plan-for-insert-order-line-PG.txt
  select 'Plan for insert_order_line';
  explain execute insert_order_line(:w_id_in, :d_id_in, :ols_in, :d_next_o_id_v);
  \o

  \o new-order-implementations/execution-plans/plan-for-update-stock-PG.txt
  select 'Plan for update_stock';
  explain execute update_stock(:w_id_in, :ols_in);
  \o
  \t off

\endif
