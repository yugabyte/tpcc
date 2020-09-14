:u3
:c

create table t(k serial primary key, ms int, measured_ms int);

create or replace procedure p(ms1 in int, ms2 in int)
language plpgsql
as $body$
declare
  thousand  constant double precision           := 1000.0;
  sec1      constant double precision           := (ms1::double precision)/thousand;
  sec2      constant double precision           := (ms2::double precision)/thousand;
  r1                 text             not null  := '';
  r2                 text             not null  := '';
begin
  for j in 1..50 loop
    declare
      t0        timestamptz               not null := clock_timestamp();
      t1        timestamptz               not null := clock_timestamp();
      d         int                       not null := -1;
    begin
      perform pg_sleep(sec1);
      t1 := clock_timestamp();
      d  := round(1000.0*(extract(seconds from (t1 - t0)::interval)::numeric), 0);

      insert into t(ms, measured_ms) values(ms1, d);
    end;

    declare
      t0        timestamptz               not null := clock_timestamp();
      t1        timestamptz               not null := clock_timestamp();
      d         int                       not null := -1;
    begin
      perform pg_sleep(sec2);
      t1 := clock_timestamp();
      d  := round(1000.0*(extract(seconds from (t1 - t0)::interval)::numeric), 0);

      insert into t(ms, measured_ms) values(ms2, d);
    end;
  end loop;
end;
$body$;

call p(1000, 2000);

select 'for 1000';
select
  min(measured_ms)                       as min,
  round(avg(measured_ms::numeric), 0)    as avg,
  max(measured_ms)                       as max,
  round(stddev(measured_ms::numeric), 2) as stddev
from t
where ms = 1000;

select 'for 2000';
select
  min(measured_ms)                       as min,
  round(avg(measured_ms::numeric), 0)    as avg,
  max(measured_ms)                       as max,
  round(stddev(measured_ms::numeric), 2) as stddev
from t
where ms = 2000;


/*

 min  | avg  | max  | stddev 
------+------+------+--------
 1000 | 1001 | 1003 |   0.56

 ?column? 
----------
 for 2000

 min  | avg  | max  | stddev 
------+------+------+--------
 2000 | 2001 | 2002 |   0.50


*/;
