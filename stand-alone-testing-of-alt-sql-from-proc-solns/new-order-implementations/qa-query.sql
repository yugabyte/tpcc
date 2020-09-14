select d_next_o_id
from district
where d_w_id = :w_id_in and d_id = :d_id_in;

select no_w_id, no_d_id, no_o_id
from new_order
order by no_w_id, no_d_id, no_o_id;

select
  o_w_id, o_d_id, o_id, o_c_id, o_carrier_id, o_ol_cnt, o_all_local, o_entry_d
from oorder order by o_w_id, o_d_id, o_id;

select
ol_o_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info
from order_line
order by ol_w_id, ol_d_id, ol_o_id, ol_number;

select s_quantity, s_ytd, s_order_cnt, s_remote_cnt
from stock
where s_w_id = :w_id_in
and s_i_id in (select u.i_id from unnest(:ols_in) as u(i_id, w_id, qty))
order by s_i_id, s_w_id;



