-- \i python/proc-vs-top-level-sql-for-multi-stmt-txns/do-timings-report.sql 

\t on
select t from timings_report();
\t off
