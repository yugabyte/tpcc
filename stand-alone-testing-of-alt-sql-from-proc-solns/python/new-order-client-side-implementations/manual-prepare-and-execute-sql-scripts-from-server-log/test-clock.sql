/*
\i python/new-order-client-side-implementations/test-clock.sql
*/;

call start_clock();
select pg_sleep(0.05);
select elapsed_time();
