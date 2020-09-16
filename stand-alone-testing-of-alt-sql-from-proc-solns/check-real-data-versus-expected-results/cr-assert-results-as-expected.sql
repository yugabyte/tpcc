/*
select count(*) from order_line_diffs;
select count(*) from district_diffs;
select count(*) from new_order_diffs;
select count(*) from oorder_diffs;
select count(*) from stock_diffs;
*/;

\ir cr-diff-views.sql

create or replace procedure assert_results_as_expected()
  language plpgsql
as $body$
declare
  n_order_line_diffs  constant int := (select count(*) from order_line_diffs);
  n_district_diffs    constant int := (select count(*) from district_diffs);
  n_new_order_diffs   constant int := (select count(*) from new_order_diffs);
  n_oorder_diffs      constant int := (select count(*) from oorder_diffs);
  n_stock_diffs       constant int := (select count(*) from stock_diffs);
begin
  assert n_order_line_diffs = 0,
        'n_order_line_diffs = '  ||n_order_line_diffs::text;

  assert n_district_diffs = 0,
        'n_district_diffs = '    ||n_district_diffs::text;

  assert n_new_order_diffs = 0,
        'n_new_order_diffs = '   ||n_new_order_diffs::text;

  assert n_oorder_diffs = 0,
        'n_oorder_diffs = '      ||n_oorder_diffs::text;

  assert n_stock_diffs = 0,
        'n_stock_diffs = '       ||n_stock_diffs::text;
end;
$body$;
