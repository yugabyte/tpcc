\o t.txt

\t on
select '--------------------------------------------------------------------------------------------------------------------------------------------';
select 'oorder inserts';
\t off
select
  o_w_id, o_d_id, o_id, o_c_id, o_carrier_id, o_ol_cnt, o_all_local
from oorder_expected_result
except
select
  o_w_id, o_d_id, o_id, o_c_id, o_carrier_id, o_ol_cnt, o_all_local
from oorder
order by o_w_id, o_d_id, o_id;

\t on
select '--------------------------------------------------------------------------------------------------------------------------------------------';
select 'order_line inserts';
\t off
select
  ol_w_id, ol_d_id, ol_o_id, ol_number, ol_i_id, ol_delivery_d, ol_amount, ol_supply_w_id, ol_quantity, ol_dist_info
from order_line_expected_result
except
select
  ol_w_id, ol_d_id, ol_o_id, ol_number, ol_i_id, ol_delivery_d, ol_amount, ol_supply_w_id, ol_quantity, ol_dist_info
from order_line
order by ol_w_id, ol_d_id, ol_o_id, ol_number;

\t on
select '--------------------------------------------------------------------------------------------------------------------------------------------';
select 'new_order inserts';
\t off
select
  no_w_id, no_d_id, no_o_id
from new_order_expected_result
except
select
  no_w_id, no_d_id, no_o_id
from new_order
order by no_w_id, no_d_id, no_o_id;

\t on
select '--------------------------------------------------------------------------------------------------------------------------------------------';
select 'district updates';
select 'district rowcount unchanged: '||
  ((select count(*) from district_expected_result) = (select count(*) from district))::text;

\t off
select
  d_w_id, d_id, d_ytd, d_tax, d_next_o_id, d_name, d_street_1, d_street_2, d_city, d_state, d_zip
from district_expected_result
except
select
  d_w_id, d_id, d_ytd, d_tax, d_next_o_id, d_name, d_street_1, d_street_2, d_city, d_state, d_zip
from district
order by d_w_id, d_id;

\t on
select '--------------------------------------------------------------------------------------------------------------------------------------------';
select 'stock updates';
select 'stock rowcount unchanged: '||
  ((select count(*) from stock_expected_result) = (select count(*) from stock))::text;

\t off
select
  s_w_id, s_i_id, s_quantity, s_ytd, s_order_cnt, s_remote_cnt, s_data, s_dist_01, s_dist_02,
  s_dist_03, s_dist_04, s_dist_05, s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10
from stock_expected_result
except
select
  s_w_id, s_i_id, s_quantity, s_ytd, s_order_cnt, s_remote_cnt, s_data, s_dist_01, s_dist_02,
  s_dist_03, s_dist_04, s_dist_05, s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10
from stock
order by s_w_id, s_i_id;

\t on
select '--------------------------------------------------------------------------------------------------------------------------------------------';
\t off
\o
