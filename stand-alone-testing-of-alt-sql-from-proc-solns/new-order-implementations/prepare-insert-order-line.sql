-- Issue #738 and "ysql_suppress_unsupported_error=true" flag.
-- Done here for self-doc. But done at the start of each top-directory
-- "master script for maximum visibility.
set client_min_messages = error;

prepare insert_order_line(int, int, order_line_t[], int) as
insert into order_line(
  ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, 
  ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info)

with
  order_lines as (
    -- The order is important to get the proper correlation
    -- between the order-line number and what item it specified.
    --
    -- It's a property of "unnest()" that it produces the rows
    -- in subscript order, so the empty OVER() is OK here.
    -- If we do away with the WITH view "order_lines" and put
    -- "unnest()" directly in the FROM list, then the result order,
    -- and therefore the "row_number()" values, seem to depend
    -- on the plan selection. And by this point, we can't access
    -- the original array element ordering.
    select
      row_number() over() as j, u.i_id, u.w_id, u.qty
    from unnest($3) as u(i_id, w_id, qty))
select
  $4, $2, $1,

  u.j, u.i_id, u.w_id, u.qty, u.qty*i.i_price,

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

from
  order_lines as u
  inner join item  as i on u.i_id = i.i_id
  inner join stock as s on i.i_id = s.s_i_id;
