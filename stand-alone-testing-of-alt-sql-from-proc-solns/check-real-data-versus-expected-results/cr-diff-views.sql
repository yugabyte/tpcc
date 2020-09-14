\pset null ''
drop view if exists order_line_diffs cascade;
create view order_line_diffs as
with
  expected_result_except_actual as (
    select
      1                               as sort_col,
      'expected result EXCEPT actual' as label,
      ol_w_id, ol_d_id, ol_o_id, ol_number, ol_i_id, ol_delivery_d, ol_amount, ol_supply_w_id, ol_quantity, ol_dist_info
    from order_line_expected_result
    except
    select
      1                               as sort_col,
      'expected result EXCEPT actual' as label,
      ol_w_id, ol_d_id, ol_o_id, ol_number, ol_i_id, ol_delivery_d, ol_amount, ol_supply_w_id, ol_quantity, ol_dist_info
    from order_line),
  blank as (
    select
      2                               as sort_col,
      null                            as label,
      null::int as ol_w_id, null::int as ol_d_id, null::int as ol_o_id, null::int as ol_number, null::int as ol_i_id, null::timestamp as ol_delivery_d, null::numeric as ol_amount, null::int as ol_supply_w_id, null::int as ol_quantity, null::char(24) as ol_dist_info
    ),
  actual_except_expected_result as (
    select
      3                               as sort_col,
      'actual EXCEPT expected result' as label,
      ol_w_id, ol_d_id, ol_o_id, ol_number, ol_i_id, ol_delivery_d, ol_amount, ol_supply_w_id, ol_quantity, ol_dist_info
    from order_line
    except
    select
      3                               as sort_col,
      'actual EXCEPT expected result' as label,
      ol_w_id, ol_d_id, ol_o_id, ol_number, ol_i_id, ol_delivery_d, ol_amount, ol_supply_w_id, ol_quantity, ol_dist_info
    from order_line_expected_result)
select
  sort_col,
  label,
  ol_w_id, ol_d_id, ol_o_id, ol_number, ol_i_id, ol_delivery_d, ol_amount, ol_supply_w_id, ol_quantity, ol_dist_info
from expected_result_except_actual
union all
select
  sort_col,
  label,
  ol_w_id, ol_d_id, ol_o_id, ol_number, ol_i_id, ol_delivery_d, ol_amount, ol_supply_w_id, ol_quantity, ol_dist_info
from blank
union all
select
  sort_col,
  label,
  ol_w_id, ol_d_id, ol_o_id, ol_number, ol_i_id, ol_delivery_d, ol_amount, ol_supply_w_id, ol_quantity, ol_dist_info
from actual_except_expected_result;

drop view if exists district_diffs cascade;
create view district_diffs as
with
  expected_result_except_actual as (
    select
      1                               as sort_col,
      'expected result EXCEPT actual' as label,
      d_w_id, d_id, d_ytd, d_tax, d_next_o_id, d_name, d_street_1, d_street_2, d_city, d_state, d_zip
    from district_expected_result
    except
    select
      1                               as sort_col,
      'expected result EXCEPT actual' as label,
      d_w_id, d_id, d_ytd, d_tax, d_next_o_id, d_name, d_street_1, d_street_2, d_city, d_state, d_zip
    from district),
  blank as (
    select
      2                               as sort_col,
      '' as label,
      null::int as d_w_id, null::int as d_id, null::numeric as d_ytd, null::numeric as d_tax, null::int as d_next_o_id, null::varchar(10) as d_name, null::varchar(20) as d_street_1, null::varchar(20) as d_street_2, null::varchar(20) as d_city, null::varchar(2) as d_state, null::varchar(9) as d_zip),
  actual_except_expected_result as (
    select
      3                               as sort_col,
      'actual EXCEPT expected result' as label,
      d_w_id, d_id, d_ytd, d_tax, d_next_o_id, d_name, d_street_1, d_street_2, d_city, d_state, d_zip
    from district
    except
    select
      3                               as sort_col,
      'actual EXCEPT expected result' as label,
      d_w_id, d_id, d_ytd, d_tax, d_next_o_id, d_name, d_street_1, d_street_2, d_city, d_state, d_zip
    from district_expected_result)
select
  sort_col,
  label,
  d_w_id, d_id, d_ytd, d_tax, d_next_o_id, d_name, d_street_1, d_street_2, d_city, d_state, d_zip
from expected_result_except_actual
union all
select
  sort_col,
  label,
  d_w_id, d_id, d_ytd, d_tax, d_next_o_id, d_name, d_street_1, d_street_2, d_city, d_state, d_zip
from blank
union all
select
  sort_col,
  label,
  d_w_id, d_id, d_ytd, d_tax, d_next_o_id, d_name, d_street_1, d_street_2, d_city, d_state, d_zip
from actual_except_expected_result;

drop view if exists new_order_diffs cascade;
create view new_order_diffs as
with
  expected_result_except_actual as (
    select
      1                               as sort_col,
      'expected result EXCEPT actual' as label,
      no_w_id, no_d_id, no_o_id
    from new_order_expected_result
    except
    select
      1                               as sort_col,
      'expected result EXCEPT actual' as label,
      no_w_id, no_d_id, no_o_id
    from new_order),
  blank as (
    select
      2                               as sort_col,
      ''                              as label,
      null::int as no_w_id, null::int as no_d_id, null::int as no_o_id),
  actual_except_expected_result as (
    select
      3                               as sort_col,
      'actual EXCEPT expected result' as label,
      no_w_id, no_d_id, no_o_id
    from new_order
    except
    select
      3                               as sort_col,
      'actual EXCEPT expected result' as label,
      no_w_id, no_d_id, no_o_id
    from new_order_expected_result)
select
  sort_col,
  label,
  no_w_id, no_d_id, no_o_id
from expected_result_except_actual
union all
select
  sort_col,
  label,
  no_w_id, no_d_id, no_o_id
from actual_except_expected_result;

drop view if exists oorder_diffs cascade;
create view oorder_diffs as
with
  expected_result_except_actual as (
    select
      1                               as sort_col,
      'expected result EXCEPT actual' as label,
      o_w_id, o_d_id, o_id, o_c_id, o_carrier_id, o_ol_cnt, o_all_local
    from oorder_expected_result
    except
    select
      1                               as sort_col,
      'expected result EXCEPT actual' as label,
      o_w_id, o_d_id, o_id, o_c_id, o_carrier_id, o_ol_cnt, o_all_local
    from oorder),
  blank as (
    select
      2                               as sort_col,
      ''                              as label,
      null::int as o_w_id, null::int as o_d_id, null::int as o_id, null::int as o_c_id, null::int as o_carrier_id, null::numeric as o_ol_cnt, null::numeric as o_all_local),
  actual_except_expected_result as (
    select
      3                               as sort_col,
      'actual EXCEPT expected result' as label,
      o_w_id, o_d_id, o_id, o_c_id, o_carrier_id, o_ol_cnt, o_all_local
    from oorder
    except
    select
      3                               as sort_col,
      'actual EXCEPT expected result' as label,
      o_w_id, o_d_id, o_id, o_c_id, o_carrier_id, o_ol_cnt, o_all_local
    from oorder_expected_result)
select
  sort_col,
  label,
  o_w_id, o_d_id, o_id, o_c_id, o_carrier_id, o_ol_cnt, o_all_local
from expected_result_except_actual
union all
select
  sort_col,
  label,
  o_w_id, o_d_id, o_id, o_c_id, o_carrier_id, o_ol_cnt, o_all_local
from blank
union all
select
  sort_col,
  label,
  o_w_id, o_d_id, o_id, o_c_id, o_carrier_id, o_ol_cnt, o_all_local
from actual_except_expected_result;

drop view if exists stock_diffs cascade;
create view stock_diffs as
with
  expected_result_except_actual as (
    select
      1::int                             as sort_col,
      'expected result EXCEPT actual' as label,
      s_w_id, s_i_id, s_quantity, s_ytd, s_order_cnt, s_remote_cnt, s_data, s_dist_01, s_dist_02,
      s_dist_03, s_dist_04, s_dist_05, s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10
    from stock_expected_result
    except
    select
      1::int                             as sort_col,
      'expected result EXCEPT actual' as label,
      s_w_id, s_i_id, s_quantity, s_ytd, s_order_cnt, s_remote_cnt, s_data, s_dist_01, s_dist_02,
      s_dist_03, s_dist_04, s_dist_05, s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10
    from stock),
  blank as (
    select
      2::int                          as sort_col,
      ''                              as label,
      null::int as s_w_id, null::int as s_i_id, null::int as s_quantity, null::numeric as s_ytd, null::int as s_order_cnt, null::int as s_remote_cnt, null::varchar(50) as s_data, null::varchar(24), null::varchar(24) as s_dist_01, null::varchar(24) as s_dist_02,
      null::varchar(24) as s_dist_03, null::varchar(24) as s_dist_04, null::varchar(24) as s_dist_05, null::varchar(24) as s_dist_06, null::varchar(24) as s_dist_07, null::varchar(24) as s_dist_08, null::varchar(24) as s_dist_09, null::varchar(24) as s_dist_10),
  actual_except_expected_result as (
    select
      3                               as sort_col,
      'actual EXCEPT expected result' as label,
      s_w_id, s_i_id, s_quantity, s_ytd, s_order_cnt, s_remote_cnt, s_data, s_dist_01, s_dist_02,
      s_dist_03, s_dist_04, s_dist_05, s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10
    from stock
    except
    select
      3                               as sort_col,
      'actual EXCEPT expected result' as label,
      s_w_id, s_i_id, s_quantity, s_ytd, s_order_cnt, s_remote_cnt, s_data, s_dist_01, s_dist_02,
      s_dist_03, s_dist_04, s_dist_05, s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10
    from stock_expected_result)
select
  sort_col,
  label,
  s_w_id, s_i_id, s_quantity, s_ytd, s_order_cnt, s_remote_cnt, s_data, s_dist_01, s_dist_02,
  s_dist_03, s_dist_04, s_dist_05, s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10
from expected_result_except_actual
union all
select
  sort_col,
  label,
  s_w_id, s_i_id, s_quantity, s_ytd, s_order_cnt, s_remote_cnt, s_data, s_dist_01, s_dist_02,
  s_dist_03, s_dist_04, s_dist_05, s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10
from blank
union all
select
  sort_col,
  label,
  s_w_id, s_i_id, s_quantity, s_ytd, s_order_cnt, s_remote_cnt, s_data, s_dist_01, s_dist_02,
  s_dist_03, s_dist_04, s_dist_05, s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10
from actual_except_expected_result;
