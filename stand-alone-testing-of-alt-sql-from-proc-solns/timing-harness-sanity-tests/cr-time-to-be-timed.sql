drop table if exists times;
drop table if exists timing_experiments;

create table timing_experiments(
  k              serial  primary key,
  method         text    not null,
  no_of_repeats  int     not null,
  env            text    not null,
  elapsed_time   int             );

create table times(
  k                     serial   primary key,
  timing_experiments_k  int      not null,
  measured_ms           numeric  not null,

  constraint times_fk foreign key(timing_experiments_k) references timing_experiments(k));

--------------------------------------------------------------------------------

create or replace procedure time_to_be_timed(
  method          in text,
  no_of_warm_ups  in int,
  no_of_repeats   in int,
  sleep_seconds   in numeric)
language plpgsql
as $body$
declare
  env                    constant text        not null :=
    case is_yb()
      when true then 'YB'
                else 'PG'
    end;
  timing_experiments_k  int                   not null := 0;

  experiment_secs                 int                    not null := -1;
  experiment_t1                   timestamptz not null := clock_timestamp();
  experiment_t0                   timestamptz not null := clock_timestamp();

  expected_checksum      constant int         not null := 442613;
begin
  experiment_t0 := clock_timestamp();

  -- Insert the master row.
  insert into timing_experiments(method, no_of_repeats, env)
  values                        (method, no_of_repeats, env)
  returning k into timing_experiments_k;

  -- Warm-up.
  for j in 1..no_of_warm_ups loop
    call to_be_timed(new_order_parameters(), expected_checksum, sleep_seconds);
  end loop;

  -- Timing test.
  for j in 1..no_of_repeats loop
    declare
      ms  int          not null := -1;
      t1  timestamptz  not null := clock_timestamp();
      t0  timestamptz  not null := clock_timestamp();
    begin
      call to_be_timed(new_order_parameters(), expected_checksum, sleep_seconds);
      t1 := clock_timestamp();
      ms  := round(1000.0*(extract(seconds from (t1 - t0)::interval)::numeric), 0);

      insert into times(timing_experiments_k, measured_ms) values(timing_experiments_k, ms);
    end;
  end loop;

  experiment_t1   := clock_timestamp();
  experiment_secs :=
    round((extract(seconds from (experiment_t1 - experiment_t0)::interval)::numeric), 0);

  update timing_experiments
  set elapsed_time = experiment_secs
  where k = timing_experiments_k;
end;
$body$;
