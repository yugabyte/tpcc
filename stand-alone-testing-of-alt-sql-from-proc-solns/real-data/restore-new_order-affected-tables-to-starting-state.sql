-- Restore the starting state in the fifve tables that "new_order()" changes.

\i new-order-implementations/do-set_bind-variables-from-new-order-parameters.sql

delete from order_line where ol_o_id > 3000;

delete from new_order
where no_o_id > 3000;

delete from oorder
where o_id > 3000;

update district
set d_next_o_id = 3001
where d_w_id = :w_id_in and d_id = :d_id_in;

do $body$
begin
  deallocate reset_stock;
exception
  when invalid_sql_statement_name then null;
end;
$body$;

prepare reset_stock(int, int) as
update stock set
  s_quantity = $1, s_ytd = 0.0, s_order_cnt  = 0, s_remote_cnt = 0
where stock.s_i_id = $2 and stock.s_w_id = 1;

start transaction;
  execute reset_stock(11,  854);
  execute reset_stock(94, 10244);
  execute reset_stock(27, 17012);
  execute reset_stock(30, 24685);
  execute reset_stock(89, 37330);
  execute reset_stock(54, 54379);
  execute reset_stock(80, 57757);
  execute reset_stock(82, 70079);
  execute reset_stock(50, 79076);
  execute reset_stock(43, 91121);
commit;

/* 
PRESENT STATE OF "stock" FOR THE ROWS THAT ARE CHANGED BY
THIS PARAMETRIZATION: new_order_parameters()

with
  ols_arr as (
    select (new_order_parameters()).ols as ols),
  order_lines as (
    select
      u.i_id,
      u.w_id
    from unnest((select ols from ols_arr)) as u(i_id, w_id, qty))

select
  s.s_i_id,
  s.s_w_id,
  s.s_quantity,
  s.s_ytd,
  s.s_order_cnt,
  s.s_remote_cnt
from
  order_lines o
  inner join stock s on(
    o.i_id = s.s_i_id and
    o.w_id = s.s_w_id)
order by s_i_id, s_w_id;

ORIG:
-----
 s_i_id | s_w_id | s_quantity | s_ytd | s_order_cnt | s_remote_cnt 
--------+--------+------------+-------+-------------+--------------
    854 |      1 |         11 |  0.00 |           0 |            0
  10244 |      1 |         94 |  0.00 |           0 |            0
  17012 |      1 |         27 |  0.00 |           0 |            0
  24685 |      1 |         30 |  0.00 |           0 |            0
  37330 |      1 |         89 |  0.00 |           0 |            0
  54379 |      1 |         54 |  0.00 |           0 |            0
  57757 |      1 |         80 |  0.00 |           0 |            0
  70079 |      1 |         82 |  0.00 |           0 |            0
  79076 |      1 |         50 |  0.00 |           0 |            0
  91121 |      1 |         43 |  0.00 |           0 |            0

FINAL:
------
    854 |      1 |         10 |  1.00 |           1 |            0
  10244 |      1 |         93 |  1.00 |           1 |            0
  17012 |      1 |         22 |  5.00 |           1 |            0
  24685 |      1 |         26 |  4.00 |           1 |            0
  37330 |      1 |         83 |  6.00 |           1 |            0
  54379 |      1 |         52 |  2.00 |           1 |            0
  57757 |      1 |         73 |  7.00 |           1 |            0
  70079 |      1 |         74 |  8.00 |           1 |            0
  79076 |      1 |         48 |  2.00 |           1 |            0
  91121 |      1 |         40 |  3.00 |           1 |            0

*/;
