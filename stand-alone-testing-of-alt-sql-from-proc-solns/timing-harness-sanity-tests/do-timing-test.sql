/*
ONE-TIME MANUAL SET-UP
:u1
:c
set client_min_messages = error;
\i cr-is-yb.sql
\i cr-new-order-parameters.sql
\i cr-to-be-timed.sql
\i cr-time-to-be-timed.sql
________________________________________________________________________________
*/;

\set no_of_warm_ups   10
\set sleep_seconds     0.05

\i cr-time-to-be-timed.sql

call start_clock();
\i cr-to-be-timed.sql

call time_to_be_timed('to-be-timed', :no_of_warm_ups,  100, :sleep_seconds);
call time_to_be_timed('to-be-timed', :no_of_warm_ups,  200, :sleep_seconds);
call time_to_be_timed('to-be-timed', :no_of_warm_ups,  800, :sleep_seconds);
call time_to_be_timed('to-be-timed', :no_of_warm_ups, 1600, :sleep_seconds);

\o t.txt
\t on
select
  (case is_yb() when true then 'YB' else 'PG' end)||
  ' Elapsed time for timing experiment: '||elapsed_time() as t;
\t off
\i report-to-be-timed-timing-results.sql
\o

/*

BASIC VERSION.

   method    | no_of_repeats | env | elapsed_time | min | avg | max | stddev 
-------------+---------------+-----+--------------+-----+-----+-----+--------
 to-be-timed |           100 | PG  |            6 |  50 |  51 |  51 |   0.48
 to-be-timed |           200 | PG  |           11 |  50 |  50 |  51 |   0.50
 to-be-timed |           800 | PG  |           41 |  50 |  50 |  52 |   0.50
 to-be-timed |          1600 | PG  |           22 |  50 |  51 |  52 |   0.48

 to-be-timed |           100 | YB  |            6 |  50 |  51 |  51 |   0.46
 to-be-timed |           200 | YB  |           11 |  50 |  51 |  51 |   0.43
 to-be-timed |           800 | YB  |           43 |  50 |  51 |  52 |   0.42
 to-be-timed |          1600 | YB  |           25 |  50 |  51 |  51 |   0.40

--------------------------------------------------------------------------------

AFTER ADDING "no in new_order_parameters_t" WITH CHECKSUM CALCULATION.

 PG Elapsed time for timing experiment: 2:20 min

 to-be-timed |           100 | PG  |            6 |  50 |  51 |  52 |   0.58
 to-be-timed |           200 | PG  |           11 |  50 |  51 |  52 |   0.48
 to-be-timed |           800 | PG  |           41 |  50 |  51 |  52 |   0.49
 to-be-timed |          1600 | PG  |           22 |  50 |  51 |  52 |   0.48

 YB Elapsed time for timing experiment: 2:33 min.

 to-be-timed |           100 | YB  |            6 |  51 |  52 |  54 |   0.63
 to-be-timed |           200 | YB  |           12 |  51 |  53 |  54 |   0.58
 to-be-timed |           800 | YB  |           45 |  51 |  52 |  54 |   0.60
 to-be-timed |          1600 | YB  |           30 |  51 |  53 |  54 |   0.55

*/;
