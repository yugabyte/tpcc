create or replace procedure to_be_timed(

  -- Included for realism ------------------------
  no                in new_order_parameters_t,  --
  expected_checksum in int,                     --
  ------------------------------------------------

  sleep_seconds     in numeric)
  language plpgsql
as $body$
declare

  w_id_in         constant int            not null := no.w_id;
  d_id_in         constant int            not null := no.d_id;
  c_id_in         constant int            not null := no.c_id;
  o_all_local_in  constant int            not null := no.o_all_local;
  ols_in          constant order_line_t[] not null := no.ols;
  ols_in_cnt      constant int            not null := cardinality(ols_in);

  -- Assigned immediately after "begin".
  item_ids        int[]                   not null := '{}';
  supplier_wh     int[]                   not null := '{}';
  order_qts       int[]                   not null := '{}';

  checksum int not null := -1;
begin
  -- The aim here isn't to write the most efficient procedural code.
  -- Rather, it's just to do a checkable computation that doesn't touch SQL.
  for j in 1..ols_in_cnt loop
    item_ids[j]     := ols_in[j].i_id;
    supplier_wh[j]  := ols_in[j].w_id;
    order_qts[j]    := ols_in[j].qty;
  end loop;

  checksum := w_id_in + d_id_in + c_id_in + o_all_local_in;
  for j in 1..ols_in_cnt loop
    checksum := checksum + item_ids[j] + supplier_wh[j] + order_qts[j];
  end loop;

  perform pg_sleep(sleep_seconds);

  assert
    checksum = expected_checksum,
  'to_be_timed(): assert failed';
end;
$body$;
