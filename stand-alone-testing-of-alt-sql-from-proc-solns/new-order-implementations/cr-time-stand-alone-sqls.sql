create or replace procedure time_stand_alone_sqls(
  method          in text,
  no_of_warm_ups  in int,
  no_of_repeats   in int,
  d_next_o_id     in int default 0)
language plpgsql
as $body$
declare
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

  d_next_o_id_v                   int         not null := d_next_o_id;
begin
  experiment_t0 := clock_timestamp();

  -- Insert the master row.
  insert into timing_experiments(method, no_of_repeats, env)
  values                        (method, no_of_repeats, env)
  returning k into timing_experiments_k;

  raise info 'Method: %', method;

  -- Timing tests.
  case method
    when 'insert-order-line' then
      raise info 'Doing  %', 'insert-order-line';

      -- Warm-up.
      for j in 1..no_of_warm_ups loop
        d_next_o_id_v:= d_next_o_id_v + 1;
        call insert_order_line(new_order_parameters(), d_next_o_id_v);
      end loop;

      -- Timing test.
      for j in 1..no_of_repeats loop 
        declare
          ms            numeric      not null := -1;
          t1            timestamptz  not null := clock_timestamp();
          t0            timestamptz  not null := clock_timestamp();
        begin
          d_next_o_id_v:= d_next_o_id_v + 1;
          call insert_order_line(new_order_parameters(), d_next_o_id_v);

          t1 := clock_timestamp();
          ms  := 1000.0*(extract(seconds from (t1 - t0)::interval));

          insert into times(timing_experiments_k, measured_ms) values(timing_experiments_k, ms);
        end;
      end loop;

    when 'update-stock' then
      raise info 'Doing  %', 'update-stock';

      -- Warm-up.
      for j in 1..no_of_warm_ups loop
        d_next_o_id_v:= d_next_o_id_v + 1;
        call update_stock(new_order_parameters());
      end loop;

      -- Timing test.
      for j in 1..no_of_repeats loop 
        declare
          ms            numeric      not null := -1;
          t1            timestamptz  not null := clock_timestamp();
          t0            timestamptz  not null := clock_timestamp();
        begin
          call update_stock(new_order_parameters());

          t1 := clock_timestamp();
          ms  := 1000.0*(extract(seconds from (t1 - t0)::interval));

          insert into times(timing_experiments_k, measured_ms) values(timing_experiments_k, ms);
        end;
      end loop;
    end case;

  experiment_t1   := clock_timestamp();
  experiment_secs :=
    round((extract(seconds from (experiment_t1 - experiment_t0)::interval)::numeric), 0);

  update timing_experiments
  set elapsed_time = experiment_secs
  where k = timing_experiments_k;
end;
$body$;
