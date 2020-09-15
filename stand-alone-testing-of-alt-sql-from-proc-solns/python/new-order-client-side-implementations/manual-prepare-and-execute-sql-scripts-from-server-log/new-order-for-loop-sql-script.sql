/*
\i python/new-order-client-side-implementations/new-order-common-prepare-stmts-sql-script.sql
\i real-data/restore-new_order-affected-tables-to-starting-state.sql
\i python/new-order-client-side-implementations/new-order-for-loop-sql-script.sql
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

    -- The for-loop itself (10 iterations).
    ----------------------------------------------------------------------------------------------------------------------------------------------------------------
    -- #1
    execute select_item_stock(4::int, 17012::int, 1::int);

    -- Notice the trailing space. This looks like a Python bug. Look for "ltrim(rtrim($9))" in the "prepare" for this statement.
    execute insert_order_line_single_row(3001::int, 4::int, 1::int, 1::int, 17012::int, 1::int, 5::int, '154.70'::numeric(6,2), 'atvgnfqghleskscmcynmcwq '::text);
    execute update_stock_single_row(27::int, 5::int, 1::int, 1::int, 17012::int);

    ----------------------------------------------------------------------------------------------------------------------------------------------------------------
    -- #2
    execute select_item_stock(4::int, 24685::int, 1::int);
    execute insert_order_line_single_row(3001::int, 4::int, 1::int, 2::int, 24685::int, 1::int, 4::int, 319.60::numeric(6,2), 'lrftrclydajxkfqehocvhfj '::text);
    execute update_stock_single_row(30::int, 4::int, 1::int, 1::int, 24685::int);

    ----------------------------------------------------------------------------------------------------------------------------------------------------------------
    -- #3
    execute select_item_stock(4::int, 854::int, 1::int);
    execute insert_order_line_single_row(3001::int, 4::int, 1::int, 3::int, 854::int, 1::int, 1::int, 16.37::numeric(6,2), 'yyhobsnwshxiachwxmqbvwx '::text);
    execute update_stock_single_row(11::int, 1::int, 1::int, 1::int, 854::int);

    ----------------------------------------------------------------------------------------------------------------------------------------------------------------
    -- #4
    execute select_item_stock(4::int, 57757::int, 1::int);
    execute insert_order_line_single_row(3001::int, 4::int, 1::int, 4::int, 57757::int, 1::int, 7::int, 23.94::numeric(6,2), 'fuhfutquamvuureviacwymh '::text);
    execute update_stock_single_row(80::int, 7::int, 1::int, 1::int, 57757::int);

    ----------------------------------------------------------------------------------------------------------------------------------------------------------------
    -- #5
    execute select_item_stock(4::int, 70079::int, 1::int);
    execute insert_order_line_single_row(3001::int, 4::int, 1::int, 5::int, 70079::int, 1::int, 8::int, 420.72::numeric(6,2), 'rtfkmykfjpzpulnafmqizwl '::text);
    execute update_stock_single_row(82::int, 8::int, 1::int, 1::int, 70079::int);

    ----------------------------------------------------------------------------------------------------------------------------------------------------------------
    -- #6
    execute select_item_stock(4::int, 79076::int, 1::int);
    execute insert_order_line_single_row(3001::int, 4::int, 1::int, 6::int, 79076::int, 1::int, 2::int, 52.54::numeric(6,2), 'ntyzrppyfyectbelfnzwkiy '::text);
    execute update_stock_single_row(50::int, 2::int, 1::int, 1::int, 79076::int);

    ----------------------------------------------------------------------------------------------------------------------------------------------------------------
    -- #7
    execute select_item_stock(4::int, 54379::int, 1::int);
    execute insert_order_line_single_row(3001::int, 4::int, 1::int, 7::int, 54379::int, 1::int, 2::int, 72.82::numeric(6,2), 'debubhniqntqgdfbmkxtmfw '::text);
    execute update_stock_single_row(54::int, 2::int, 1::int, 1::int, 54379::int);

    ----------------------------------------------------------------------------------------------------------------------------------------------------------------
    -- #8
    execute select_item_stock(4::int, 91121::int, 1::int);
    execute insert_order_line_single_row(3001::int, 4::int, 1::int, 8::int, 91121::int, 1::int, 3::int, 113.22::numeric(6,2), 'ynakviijdcxbqqmkodxhqcp '::text);
    execute update_stock_single_row(43::int, 3::int, 1::int, 1::int, 91121::int);

    ----------------------------------------------------------------------------------------------------------------------------------------------------------------
    -- #9
    execute select_item_stock(4::int, 10244::int, 1::int);
    execute insert_order_line_single_row(3001::int, 4::int, 1::int, 9::int, 10244::int, 1::int, 1::int, 90.29::numeric(6,2), 'uduhxprcyxixatlnncinhat '::text);
    execute update_stock_single_row(94::int, 1::int, 1::int, 1::int, 10244::int);

    ----------------------------------------------------------------------------------------------------------------------------------------------------------------
    -- #10
    execute select_item_stock(4::int, 37330::int, 1::int);
    execute insert_order_line_single_row(3001::int, 4::int, 1::int, 10::int, 37330::int, 1::int, 6::int, 335.58::numeric(6,2), 'jftiphcrxklvtlbrwqhnhov '::text);
    execute update_stock_single_row(89::int, 6::int, 1::int, 1::int, 37330::int);

    ----------------------------------------------------------------------------------------------------------------------------------------------------------------

commit;

select elapsed_time();

-- call assert_results_as_expected();
