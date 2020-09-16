/*
______________________________________________________________________________

ENSURE THAT THIS SET UP HAS BEEN DONE:

set client_min_messages = error;
\i utils/cr-is-yb.sql
\i real-data/insert-data.sql
\i new-order-implementations/cr-new-order-parameters.sql
\i new-order-implementations/cr-time-new-order.sql
______________________________________________________________________________
*/;

\set no_of_warm_ups 100
\set no_of_repeats  10000

-- Issue #738. Up to you if you want to set it back to, say, "warning"
set client_min_messages = error;

call start_clock();

\i new-order-implementations/cr-new-order-1-sudheer.sql
call time_new_order('sudheer',   :no_of_warm_ups,  :no_of_repeats);

\i new-order-implementations/cr-new-order-2-for-loop.sql
call time_new_order('for-loop',  :no_of_warm_ups,  :no_of_repeats);

\i new-order-implementations/cr-new-order-3-pure-sql.sql
call time_new_order('pure-sql',  :no_of_warm_ups,  :no_of_repeats);

select 'Elapsed time for timing experiment: '||elapsed_time();
\t off

\i new-order-implementations/do-report-timing-results.sql
