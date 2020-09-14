/*
-- This shows the critical semantic difference between "now()" (transactional)
-- and "clock_timestamp()" (non-transactional).
do $body$
declare
  t0 timestamptz;
  t1 timestamptz;
  diff interval;
  d numeric;
begin
  perform pg_sleep(1.0);
  t0 := now();
  t1 := clock_timestamp();
  diff := (t1 - t0);
  d :=  extract(seconds from diff)::numeric;

  assert
    (d between 0.99 and 1.01),
  'assert failed';
end;
$body$;
--------------------------------------------------------------------------------
*/;

create or replace procedure start_clock()
language plpgsql
as $body$
declare
  -- Produce a bare timestamp value representing what "clock_timestamp()"
  -- (which returns a timestamptz value in the session's current timezone)
  -- would be on a wall clock in in UTC.
  --
  -- These two expressions have identical meaning:
  --
  --    timezone('UTC', clock_timestamp())
  --
  -- and:
  --
  --   clock_timestamp() at time zone 'UTC' 
  --
  -- The second is SQL-Standard-compilant but more verbose.
  --
  -- If "start_clock()" and "elapsed_time()" were called in
  -- the same txn and the session timeszone were changed between
  -- the two calls, then we'd get wrong results without doing the
  -- conversion to UTC.

  start_time constant text not null :=
    timezone('UTC', clock_timestamp())::text;
begin
  execute 'set clock.start_time to '''||start_time||'''';
end;
$body$;

create or replace function elapsed_time()
  returns text
  volatile
  --
  -- This must be marked "volatile" if it's to be used to
  -- start and stop the clock _within_ a stored proc because
  -- of the transactional behavior of the "set" and "get
  -- for a sessipon variable:
  --
  --    SET: "set clock.start_time to..."
  --    GET: "current_setting('clock.start_time')"
  --
  -- I don't have a mental model for this, but anyway
  -- marking it "immuable" has the effect the "elapsed_time()"
  -- always reports zero (see as "less than ~20 ms." here).
  --
  language plpgsql
as $body$
declare
  start_time constant timestamp not null :=
    current_setting('clock.start_time');

  curr_time constant timestamp not null :=
    timezone('UTC', clock_timestamp());

  diff constant interval := curr_time - start_time;

  hours    constant numeric := extract(hours   from diff);
  minutes  constant numeric := extract(minutes from diff);
  seconds  constant numeric := extract(seconds from diff);
begin
  return
    case
      when seconds < 0.020
        and minutes < 1
        and hours < 1                               then 'less than ~20 ms.'

      when seconds between 0.020 and 2.0
        and minutes < 1
        and hours < 1                               then ltrim(to_char(round(seconds*1000), '999999'))||' ms.'

      when seconds between 2.0 and  59.999
        and minutes < 1
        and hours < 1                               then ltrim(to_char(seconds, '99.9'))||' sec.'

      when minutes between 1 and 59
        and hours < 1                               then ltrim(to_char(minutes, '99'))||':'||
                                                         ltrim(to_char(seconds, '09'))||' min.'

      else                                               ltrim(to_char(hours,   '09'))||':'||
                                                         ltrim(to_char(minutes, '09'))||':'||
                                                         ltrim(to_char(seconds, '09'))||' hours'
    end;
end;
$body$;

-- "warming up" the use seems to improve the accuracy of the reported time.
do $body$
declare
  t text not null := '';
begin
  for j in 1..5 loop
    call start_clock();
    t := (select elapsed_time());
  end loop;
end;
$body$;

/*
------------------------------------------------------------
-- TEST IT.

do $body$
declare
  -- pg_sleep is take as SECONDS.
  --    seconds
  --   --------  
  --       3822 sec -- 01:03:42

  sleep_seconds constant double precision[] := array[
        '0.016',
        '0.023',
        '103.0'  --  01:43
    ];

  t text not null := '';
begin
  for j in 1..array_upper(sleep_seconds, 1) loop

    call start_clock();
    perform pg_sleep(sleep_seconds[j]);
    t := elapsed_time();

    raise info '% :: %',
       to_char(sleep_seconds[j], '99999990.999'), t;
  end loop;
end;
$body$;

------------------------------------------------------------
*/;
