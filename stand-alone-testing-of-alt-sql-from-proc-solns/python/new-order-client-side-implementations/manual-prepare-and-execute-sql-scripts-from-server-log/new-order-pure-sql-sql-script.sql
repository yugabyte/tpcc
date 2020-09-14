/*
\i python/new-order-client-side-implementations/new-order-common-prepare-stmts-sql-script.sql
\i real-data/restore-new_order-affected-tables-to-starting-state.sql
\i python/new-order-client-side-implementations/new-order-pure-sql-sql-script.sql
*/;

set client_min_messages = error;
rollback;

call start_clock();
start transaction isolation level repeatable read;

-- Prelude.
execute select_customer(1::int, 4::int, 21::int);
execute select_warehouse(1::int);
execute select_district(1::int, 4::int);
execute update_district(1::int, 4::int);
execute insert_oorder(3001::int, 4::int, 1::int, 21::int, 10::int, 1::int);
execute insert_new_order(3001::int, 4::int, 1::int);

-- The two set-base SQLs.
execute insert_order_line_set_based(
  1::int,
  4::int,
  '{
    "(17012,1,5)",
    "(24685,1,4)",
    "(854,1,1)",
    "(57757,1,7)",
    "(70079,1,8)",
    "(79076,1,2)",
    "(54379,1,2)",
    "(91121,1,3)",
    "(10244,1,1)",
    "(37330,1,6)"
  }'::order_lines_t,
  3001::int);

execute update_stock_set_based(
  1::int,
  '{
    "(17012,1,5)",
    "(24685,1,4)",
    "(854,1,1)",
    "(57757,1,7)",
    "(70079,1,8)",
    "(79076,1,2)",
    "(54379,1,2)",
    "(91121,1,3)",
    "(10244,1,1)",
    "(37330,1,6)"
  }'::order_lines_t);

commit;

select elapsed_time();

-- call assert_results_as_expected();
