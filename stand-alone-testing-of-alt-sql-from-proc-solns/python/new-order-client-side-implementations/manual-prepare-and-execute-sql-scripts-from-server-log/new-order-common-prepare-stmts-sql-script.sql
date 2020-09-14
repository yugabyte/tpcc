/*

\i python/new-order-client-side-implementations/new-order-common-prepare-stmts-sql-script.sql

*/;

deallocate all;

prepare select_customer(int, int, int) as
select
  to_char(c_discount, '90.9999')::text,
  c_last::text,
  c_credit::text
from customer
where
  c_w_id = $1  and
  c_d_id = $2  and
  c_id   = $3;

prepare select_warehouse(int) as
select to_char(w_tax, '90.9999')::text
from warehouse
where w_id = $1;

prepare select_district(int, int) as
select d_next_o_id
from district
where
  d_w_id = $1 and
  d_id   = $2
for update;

prepare update_district(int, int) as
update district
set d_next_o_id = d_next_o_id + 1
where
  d_w_id = $1 and
  d_id   = $2;

prepare insert_oorder(int, int, int, int, int, int) as
insert into oorder    (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local)
values                ($1,   $2,     $3,     $4,     now(),     $5,       $6);

prepare insert_new_order(int, int, int) as
insert into new_order (no_o_id, no_d_id, no_w_id)
values                ($1,      $2,      $3);

prepare select_item_stock(int, int, int) as
select
  i.i_price,
  s.s_quantity,
  case $1
    when $1 then s.s_dist_01
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

from item i inner join stock s on i.i_id = s.s_i_id

where
  i.i_id =   $2 and
  s.s_i_id = $2 and
  s.s_w_id = $3
for update of s;

prepare insert_order_line_single_row(int, int, int, int, int, int, int, numeric(6,2), text) as
insert into order_line (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info)
values                 ($1,      $2,      $3,      $4,        $5,      $6,             $7,          $8,        ltrim(rtrim($9)));

prepare update_stock_single_row(int, int, int, int, int) as
update stock set
  s_quantity =
  case ($1 - $2 >= 10)
    when true then      $1 - $2
    else           91 + $1 - $2
  end,
  s_ytd        = s_ytd + $2,
  s_order_cnt  = s_order_cnt + 1,
  s_remote_cnt = s_remote_cnt +
  case ($3 = $4)
    when true then 0
    else           1
  end
where
  s_i_id = $5 and
  s_w_id = $4;

prepare insert_order_line_set_based(int, int, order_lines_t, int) as
insert into order_line(
  ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, 
  ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info)

with
  order_lines as (
    select
      row_number() over() as j, u.i_id, u.w_id, u.qty
    from unnest($3) as u(i_id, w_id, qty))
select
  $4, $2, $1,
  u.j, u.i_id, u.w_id, u.qty, u.qty*i.i_price,
  case $2
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

prepare update_stock_set_based(int, order_lines_t) as
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

prepare trace_select_order_line(int, int) as
select
  to_char(ol_o_id,         '9999')::text,
  to_char(ol_number,         '99')::text,
  to_char(ol_i_id,        '99999')::text,
  to_char(ol_supply_w_id,     '9')::text,
  to_char(ol_quantity,    '99.99')::text,
  to_char(ol_amount,        '999')::text,
  ol_dist_info::text
from order_line
where
  ol_w_id = $1 and
  ol_o_id = $2
order by ol_w_id, ol_d_id, ol_o_id, ol_number;

prepare trace_select_stock(int, order_lines_t) as
select
  to_char(s_i_id,        '99999')::text,
  to_char(s_w_id,            '9')::text,
  to_char(s_quantity,       '99')::text,
  to_char(s_ytd,        '999.99')::text,
  to_char(s_order_cnt,      '99')::text,
  to_char(s_remote_cnt,      '9')::text
from stock
where s_w_id = $1
and s_i_id in (select u.i_id from unnest($2) as u(i_id, w_id, qty))
order by s_i_id, s_w_id;
