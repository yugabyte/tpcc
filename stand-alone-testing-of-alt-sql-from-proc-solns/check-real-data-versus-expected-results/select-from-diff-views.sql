/*
\i check-real-data-versus-expected-results/select-from-diff-views.sql
*/;
----------------------------------------------------------------------------------------------------------------------------------

select 'order_line diff count: '||(select count(*) from order_line_diffs)::text;
select 'district diff count:   '||(select count(*) from district_diffs)::text;
select 'new_order diff count:  '||(select count(*) from new_order_diffs)::text;
select 'oorder diff count:     '||(select count(*) from oorder_diffs)::text;
select 'stock diff count:      '||(select count(*) from stock_diffs)::text;
----------------------------------------------------------------------------------------------------------------------------------

select 'order_line diffs';

select
  label, ol_w_id, ol_d_id, ol_o_id, ol_number, ol_i_id, ol_delivery_d, ol_amount, ol_supply_w_id, ol_quantity, ol_dist_info
from order_line_diffs
order by sort_col, ol_w_id, ol_d_id, ol_o_id, ol_number;
----------------------------------------------------------------------------------------------------------------------------------

select 'district diffs';

select
  label, d_w_id, d_id, d_ytd, d_tax, d_next_o_id, d_name, d_street_1, d_street_2, d_city, d_state, d_zip
from district_diffs
order by sort_col, d_w_id, d_id;
----------------------------------------------------------------------------------------------------------------------------------

select 'new_order diffs';

select
  label, no_w_id, no_d_id, no_o_id 
from new_order_diffs
order by sort_col, no_w_id, no_d_id;
----------------------------------------------------------------------------------------------------------------------------------

select 'oorde diffs';

select
  label, o_w_id, o_d_id, o_id, o_c_id, o_carrier_id, o_ol_cnt, o_all_local 
from oorder_diffs
order by sort_col, o_w_id, o_d_id;
----------------------------------------------------------------------------------------------------------------------------------

select 'stock diffs';

select
   label, s_w_id, s_i_id, s_quantity, s_ytd, s_order_cnt, s_remote_cnt, s_data,
  s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10 
from stock_diffs
order by sort_col, s_w_id, s_i_id;
----------------------------------------------------------------------------------------------------------------------------------
