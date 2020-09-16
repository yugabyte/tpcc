\i new-order-implementations/do-set_bind-variables-from-new-order-parameters.sql
-- Get the first item ID and warehoue ID from the input order lines.
select u.i_id, u.w_id from unnest(:ols_in) as u(i_id, w_id, qty)
limit 1
\gset x_

\o new-order-implementations/execution-plans/plan-for-select-from-item-stock-inner-join.txt
--------------------------------------------------------------------------------

drop index if exists idx_s_i_id;
select 'idx_s_i_id not present';

\i new-order-implementations/prepare-select-from-item-stock-inner-join.sql
execute select_from_item_stock_join(:d_id_in, :x_i_id, :x_w_id);
explain execute select_from_item_stock_join(:d_id_in, :x_i_id, :x_w_id);

--------------------------------------------------------------------------------

create unique index idx_s_i_id on stock using lsm((s_i_id) hash);
select 'unique index idx_s_i_id';

\i new-order-implementations/prepare-select-from-item-stock-inner-join.sql
execute select_from_item_stock_join(:d_id_in, :x_i_id, :x_w_id);
explain execute select_from_item_stock_join(:d_id_in, :x_i_id, :x_w_id);

--------------------------------------------------------------------------------

drop index idx_s_i_id;
create index idx_s_i_id on stock using lsm((s_i_id) hash);
\i new-order-implementations/prepare-select-from-item-stock-inner-join.sql

select 'non-unique index idx_s_i_id';
execute select_from_item_stock_join(:d_id_in, :x_i_id, :x_w_id);
explain execute select_from_item_stock_join(:d_id_in, :x_i_id, :x_w_id);

--------------------------------------------------------------------------------
\o
