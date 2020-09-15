\pset null '<null>'

with
  stats as (
    select
                                             timing_experiments_k,
      min(measured_ms)                       as min,
      round(avg(measured_ms::numeric), 0)    as avg,
      max(measured_ms)                       as max,
      round(stddev(measured_ms::numeric), 2) as stddev
    from times
    group by timing_experiments_k)

select
  e.method,
  e.no_of_repeats,
  e.env,
  e.elapsed_time,
  s.min,
  s.avg,
  s.max,
  s.stddev
from
  timing_experiments as e
  inner join stats as s on e.k = s.timing_experiments_k
order by
  e.no_of_repeats,
  e.method;
