\t on
\o real-data-expected-results/insert-data.dump

select 'delete from new_order_expected_result;';
select 'copy new_order_expected_result(no_w_id, no_d_id, no_o_id) from stdin;';

copy (select no_w_id, no_d_id, no_o_id from new_order) to stdout;

select '\.';
select '------------------------------------------------------------------------------------------------------------------------------------------------------';

select 'delete from oorder_expected_result;';
select 'copy oorder_expected_result(o_w_id, o_d_id, o_id, o_c_id, o_carrier_id, o_ol_cnt, o_all_local, o_entry_d) from stdin;';

copy (select o_w_id, o_d_id, o_id, o_c_id, o_carrier_id, o_ol_cnt, o_all_local, o_entry_d from oorder) to stdout;

select '\.';
select '------------------------------------------------------------------------------------------------------------------------------------------------------';

select 'delete from order_line_expected_result;';
select 'copy order_line_expected_result(ol_w_id, ol_d_id, ol_o_id, ol_number, ol_i_id,';
select '  ol_delivery_d, ol_amount, ol_supply_w_id, ol_quantity, ol_dist_info) from stdin;';

copy (select ol_w_id, ol_d_id, ol_o_id, ol_number, ol_i_id,
  ol_delivery_d, ol_amount, ol_supply_w_id, ol_quantity, ol_dist_info from order_line) to stdout;

select '\.';
select '------------------------------------------------------------------------------------------------------------------------------------------------------';

select 'delete from district_expected_result;';
select 'copy district_expected_result(d_w_id, d_id, d_ytd, d_tax, d_next_o_id, d_name, d_street_1, d_street_2, d_city, d_state, d_zip) from stdin;';

copy (select d_w_id, d_id, d_ytd, d_tax, d_next_o_id, d_name, d_street_1, d_street_2, d_city, d_state, d_zip from district) to stdout;

select '\.';
select '------------------------------------------------------------------------------------------------------------------------------------------------------';

select 'delete from stock_expected_result;';
select 'copy stock_expected_result(s_w_id, s_i_id, s_quantity, s_ytd, s_order_cnt, s_remote_cnt, s_data, s_dist_01, s_dist_02,';
select '  s_dist_03, s_dist_04, s_dist_05, s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10) from stdin;';

copy (select s_w_id, s_i_id, s_quantity, s_ytd, s_order_cnt, s_remote_cnt, s_data, s_dist_01, s_dist_02,
  s_dist_03, s_dist_04, s_dist_05, s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10 from stock) to stdout;

select '\.';

\o
\t off
