-- Issue #738. Up to you if you want to set it back to, say, "warning"
set client_min_messages = error;

prepare insert_order_line(int, int, order_line_t[], int) as
insert into order_line(
  ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, 
  ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info)

select
  $4, $2, $1,

  -- This was a semntic error. The order must map to the ordering
  -- of the elements (by array index value) in the customer order.
  --
  -- The order is arbitrary, hence the empty OVER().
  row_number() over(), u.i_id, u.w_id, u.qty, u.qty*i.i_price,

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
  unnest($3) as u(i_id, w_id, qty)
  inner join item  as i on u.i_id = i.i_id
  inner join stock as s on i.i_id = s.s_i_id;
