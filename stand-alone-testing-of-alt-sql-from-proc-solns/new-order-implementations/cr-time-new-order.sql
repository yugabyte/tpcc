create or replace procedure time_new_order(
  method          in text,
  no_of_warm_ups  in int,
  no_of_repeats   in int)
language plpgsql
as $body$
declare
  env                   constant text not null :=
    case is_yb()
      when true then 'YB'
                else 'PG'
    end;
  timing_experiments_k           int                    not null := 0;
  no                    constant new_order_parameters_t not null := new_order_parameters();

  experiment_secs                int                    not null := -1;
  experiment_t1                  timestamptz            not null := clock_timestamp();
  experiment_t0                  timestamptz            not null := clock_timestamp();
begin
  experiment_t0 := clock_timestamp();

  -- Insert the master row.
  insert into timing_experiments(method, no_of_repeats, env)
  values                        (method, no_of_repeats, env)
  returning k into timing_experiments_k;

  -- Warm-up.
  for j in 1..no_of_warm_ups loop
    call new_order(no);
  end loop;

  -- Timing test.
  for j in 1..no_of_repeats loop
    declare
      ms  numeric      not null := -1;
      t1  timestamptz  not null := clock_timestamp();
      t0  timestamptz  not null := clock_timestamp();
    begin
      call new_order(no);
      t1 := clock_timestamp();
      ms  := 1000.0*(extract(seconds from (t1 - t0)::interval));

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
