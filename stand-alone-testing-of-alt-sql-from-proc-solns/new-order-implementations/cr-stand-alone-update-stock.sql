-- Issue #738 and "ysql_suppress_unsupported_error=true" flag.
-- Done here for self-doc. But done at the start of each top-directory
-- "master script for maximum visibility.
set client_min_messages = error;

create or replace procedure update_stock(no in new_order_parameters_t)
  language plpgsql
as $body$
declare
  w_id_in         constant int            not null := no.w_id;
  d_id_in         constant int            not null := no.d_id;
  c_id_in         constant int            not null := no.c_id;
  o_all_local_in  constant int            not null := no.o_all_local;
  ols_in          constant order_line_t[] not null := no.ols;
begin
  with
    v as (
      select
        u.i_id                                      as o_i_id,
        u.w_id                                      as o_w_id,
        u.qty                                       as o_qty,
        case (s.s_quantity - u.qty >= 10)
          when true then      s.s_quantity - u.qty
                    else 91 + s.s_quantity - u.qty
        end                                         as s_qty,
        case (u.w_id = w_id_in)
          when true then 0
                    else 1
        end                                         as delta

      from
        unnest(ols_in) as u(i_id, w_id, qty)
        inner join stock s on  s.s_i_id = u.i_id
                           and s.s_w_id = u.w_id)

  update stock set
    s_quantity   = v.s_qty,
    s_ytd        = s_ytd + v.o_qty,
    s_order_cnt  = s_order_cnt + 1,
    s_remote_cnt = s_remote_cnt + v.delta
  from v
  where
    stock.s_i_id = v.o_i_id and
    stock.s_w_id = v.o_w_id;
end;
$body$;
