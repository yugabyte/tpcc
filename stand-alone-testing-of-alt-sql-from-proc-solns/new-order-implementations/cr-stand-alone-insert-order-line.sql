create or replace procedure insert_order_line(
  no             in new_order_parameters_t,
  d_next_o_id_v  in int)
  language plpgsql
as $body$
declare
  w_id_in         constant int            not null := no.w_id;
  d_id_in         constant int            not null := no.d_id;
  c_id_in         constant int            not null := no.c_id;
  o_all_local_in  constant int            not null := no.o_all_local;
  ols_in          constant order_line_t[] not null := no.ols;

begin
  insert into order_line(
    ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, 
    ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info)

  with
    order_lines as (
      select
        row_number() over() as j, u.i_id, u.w_id, u.qty
      from unnest(ols_in) as u(i_id, w_id, qty))
  select
    d_next_o_id_v, d_id_in, w_id_in,

    u.j, u.i_id, u.w_id, u.qty, u.qty*i.i_price,

    case d_id_in
      when 1 then s.s_dist_01
      when 2 then s.s_dist_02
      when 2 then s.s_dist_02
      when 3 then s.s_dist_03
      when 4 then s.s_dist_04
      when 5 then s.s_dist_05
      when 6 then s.s_dist_06
      when 7 then s.s_dist_07
      when 8 then s.s_dist_08
      when 9 then s.s_dist_09
              else s.s_dist_10
    end as ol_dist_info

  from
    order_lines as u
    inner join item  as i on u.i_id = i.i_id
    inner join stock as s on i.i_id = s.s_i_id;
end;
$body$;
