-- Convert this ito a table function?
\pset null '<null>'

with
  stats as (
    select
                                             timing_experiments_k,
      min(measured_ms)                       as min,
      avg(measured_ms)                       as avg,
      max(measured_ms)                       as max,
      stddev(measured_ms)                    as stddev
    from times
    group by timing_experiments_k)

select
  e.method                     as "Method",
  e.no_of_repeats              as "No. of repeats",
  e.env                        as "Env.",
  e.elapsed_time               as "Elapsed time",
  to_char(s.min,    '9990.99') as "Min",
  to_char(s.avg,    '9990.99') as "Avg",
  to_char(s.max,    '9990.99') as "Max",
  to_char(s.stddev, '9990.99') as "StdDev"
from
  timing_experiments as e
  inner join stats as s on e.k = s.timing_experiments_k
order by
  e.no_of_repeats,
  e.method;

/*
--------------------------------------------------------------------------------

  method  | no_of_repeats | env | elapsed_time | min | avg | max  | stddev 
----------+---------------+-----+--------------+-----+-----+------+--------
 for-loop |           100 | YB  |            4 |  25 |  33 |   45 |   4.51
 pure-sql |           100 | YB  |            5 |  29 |  41 |   55 |   7.21
 sudheer  |           100 | YB  |            3 |  23 |  30 |   43 |   4.50
 for-loop |           500 | YB  |           37 |  26 |  72 |  141 |  27.37
 pure-sql |           500 | YB  |           46 |  28 |  89 | 1263 |  67.27
 sudheer  |           500 | YB  |           34 |  23 |  66 |  125 |  26.08
 for-loop |          1000 | YB  |            3 |  31 | 122 |  940 |  63.43
 pure-sql |          1000 | YB  |           31 |  46 | 150 |  259 |  59.43
 sudheer  |          1000 | YB  |           53 |  27 | 112 |  213 |  49.40

 for-loop |          1000 | PG  |            2 |   1 |   2 |   4 |   0.84
 pure-sql |          1000 | PG  |            2 |   1 |   2 |  21 |   1.07
 sudheer  |          1000 | PG  |            2 |   1 |   2 |   3 |   0.75
 for-loop |          5000 | PG  |           44 |   1 |   9 |  50 |   5.35
 pure-sql |          5000 | PG  |           42 |   1 |   8 |  34 |   5.35
 sudheer  |          5000 | PG  |           42 |   1 |   8 |  25 |   5.16

--------------------------------------------------------------------------------
*/;
