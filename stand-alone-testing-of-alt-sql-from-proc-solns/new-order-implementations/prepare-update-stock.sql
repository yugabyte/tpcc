-- Issue #738. Up to you if you want to set it back to, say, "warning"
set client_min_messages = error;

prepare update_stock(int, order_line_t[]) as
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
        case (u.w_id = $1)
          when true then 0
                    else 1
        end                                         as delta

      from
        unnest($2) as u(i_id, w_id, qty)
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
