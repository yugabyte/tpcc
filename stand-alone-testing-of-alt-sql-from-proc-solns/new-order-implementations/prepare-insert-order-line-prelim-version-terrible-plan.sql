-- Issue #738 and "ysql_suppress_unsupported_error=true" flag.
-- Done here for self-doc. But done at the start of each top-directory
-- "master script for maximum visibility.
set client_min_messages = error;

prepare insert_order_line(int, int, order_line_t[], int) as
insert into order_line(
  ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, 
  ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info)

with
  u as (
    select u.i_id, u.w_id, u.qty
    from unnest($3) as u(i_id, w_id, qty)),

  -- The order is arbitrary
  order_lines as (
    select row_number() over() as j, i_id, w_id, qty
    from u),

  item_stock as (
    select
      $4, $2, $1,
      case $2
        when 1 then s.s_dist_01
        when 2 then s.s_dist_02
        when 2 then s.s_dist_02
        when 3 then s.s_dist_03
        when 4 then s.s_dist_04
        when 5 then s.s_dist_05
        when 6 then s.s_dist_06
        when 7 then s.s_dist_07
        when 8 then s.s_dist_08
        when 9 then s.s_dist_09
                else s.s_dist_10
      end as ol_dist_info

    from item i inner join stock s
    on i.i_id = s.s_i_id)

select
  d_next_o_id_v, d_id_in, w_id_in,
  b.j, b.i_id, b.w_id, b.qty, b.qty*a.i_price, a.ol_dist_info
from order_lines as b inner join item_stock as a
on   b.i_id = a.i_id
and  b.w_id = a.s_w_id;
