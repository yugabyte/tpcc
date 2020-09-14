create or replace function mini_insert_scripts(
  w_id_in         in int,
  d_id_in         in int,
  c_id_in         in int,
  o_all_local_in  in int,
  ols_in          in order_line_t[]
  )
  returns table(t text)
  immutable
  language plpgsql
as $body$
declare
  c   constant char(2) := ', ';
  q   constant char(1) := '''';
  qc  constant char(3) := ''', ';

  arr text[];
begin
  t := 'delete from district;'                            ; return next;
  t := 'delete from stock;'                               ; return next;
  t := 'delete from item;'                                ; return next;
  t := 'delete from new_order;'                           ; return next;
  t := 'delete from oorder;'                              ; return next;
  t := 'delete from order_line;'                          ; return next;
  t := 'delete from warehouse;'                           ; return next;
  t := ''                                                 ; return next;

  ------------------------------------------------------------------------------

  t := 'insert into district(d_w_id, d_id, d_next_o_id)'  ; return  next;
  t := 'values ';
  t := t||(
    select '('||d_w_id||c||d_id||c||d_next_o_id||');'
    from district_expected_result
    where
    d_w_id = w_id_in and
    d_id   = d_id_in)                                     ; return next;
  t := ''                                                 ; return next;

  ------------------------------------------------------------------------------

  t := 'insert into item('                                ; return next;
  t := '  i_id,'                                          ; return next;
  t := '  i_price,'                                       ; return next;
  t := '  i_im_id,'                                       ; return next;
  t := '  i_name,'                                        ; return next;
  t := '  i_data)'                                        ; return next;
  t := 'values'                                           ; return next;

  with
    v as (
      select
      '  ('||i_id         ||c||
             i_price      ||c||
             i_im_id      ||c||
          q||i_name       ||qc||
          q||i_data       ||q||'),' as a
      from item_expected_result
      where i_id in (select u.i_id from unnest(ols_in) as u(i_id, w_id, qty))
    )
  select array_agg(a)
  into arr
  from v;

  arr[cardinality(arr)] := rtrim(arr[cardinality(arr)], ',')||';';

  for t in (select unnest(arr)) loop
    return next;
  end loop;
  t := ''                                                 ; return next;

  ------------------------------------------------------------------------------

  t := 'insert into stock('                               ; return next;
  t := '  s_w_id,'                                        ; return next;
  t := '  s_i_id,'                                        ; return next;
  t := '  s_quantity,'                                    ; return next;
  t := '  s_ytd,'                                         ; return next;
  t := '  s_order_cnt,'                                   ; return next;
  t := '  s_remote_cnt,'                                  ; return next;
  t := '  s_dist_01,'                                     ; return next;
  t := '  s_dist_02,'                                     ; return next;
  t := '  s_dist_03,'                                     ; return next;
  t := '  s_dist_04,'                                     ; return next;
  t := '  s_dist_05,'                                     ; return next;
  t := '  s_dist_06,'                                     ; return next;
  t := '  s_dist_07,'                                     ; return next;
  t := '  s_dist_08,'                                     ; return next;
  t := '  s_dist_09,'                                     ; return next;
  t := '  s_dist_10)'                                     ; return next;
  t := 'values'                                           ; return next;

  with
    v as (
      select
      '  ('||s_w_id       ||c||
             s_i_id       ||c||
             s_quantity   ||c||
             s_ytd        ||c||
             s_order_cnt  ||c||
             s_remote_cnt ||c||
          q||s_dist_01    ||qc||
          q||s_dist_02    ||qc||
          q||s_dist_03    ||qc||
          q||s_dist_04    ||qc||
          q||s_dist_05    ||qc||
          q||s_dist_06    ||qc||
          q||s_dist_07    ||qc||
          q||s_dist_08    ||qc||
          q||s_dist_09    ||qc||
          q||s_dist_10    ||q||'),' as a
      from stock_expected_result
      where s_w_id = w_id_in
      and s_i_id in (select u.i_id from unnest(ols_in) as u(i_id, w_id, qty))
    )
  select array_agg(a)
  into arr
  from v;

  arr[cardinality(arr)] := rtrim(arr[cardinality(arr)], ',')||';';

  for t in (select unnest(arr)) loop
    return next;
  end loop;
end;
$body$;

\o mini-data/insert-data.sql
\t on
select t from
    mini_insert_scripts(
    w_id_in        => 1,
    d_id_in        => 4,
    c_id_in        => 21,
    o_all_local_in => 1,
    ols_in         => order_lines());
\t off
\o
