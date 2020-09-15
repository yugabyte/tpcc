\timing off
-- Issue #738. Up to you if you want to set it back to, say, "warning"
set client_min_messages = error;

\i utils/cr-clock.sql

--------------------------------------------------------------------------------
-- Bllewell utility (uses PG "session variable") thus
-- "set clock.start_time to <clock_timestamp()>" to start the clcok and then
-- "current_setting('clock.start_time')" to get it back to get the elapsed time.
call start_clock();

-- The script senses the env with "function is_yb()" and uses the appropriate
-- "cr-tables-*.sql" and "cr-indexes-and-constraints-*.sql"
-- before and after the common "insert-data.dump".
\i utils/cr-is-yb.sql
\i real-data/insert-data.sql

-- Expected results.
\i real-data-expected-results/pg.sql
\i check-real-data-versus-expected-results/cr-assert-results-as-expected.sql
\i new-order-implementations/cr-new-order-parameters.sql
\i new-order-implementations/cr-time-new-order.sql

--------------------------------------------------------------------------------
-- Choose the implementation of new_order() of interest.
-- Simply place it last in the list!

\i new-order-implementations/cr-new-order-1-sudheer.sql
\i new-order-implementations/cr-new-order-2-for-loop.sql
\i new-order-implementations/cr-new-order-3-pure-sql.sql

\t on
select 'using "pure-sql"';
\t off

-- This is a correctness test. It's pointless to time just
-- a single, and first, execution.
call new_order(new_order_parameters());

--------------------------------------------------------------------------------

call assert_results_as_expected();

\t on
select 'test-on-real-data elapsed time: '||elapsed_time();
\t off

--------------------------------------------------------------------------------
-- If "call assert_results_as_expected()" fails...

/*

\i check-real-data-versus-expected-results/select-from-diff-views.sql

*/;
