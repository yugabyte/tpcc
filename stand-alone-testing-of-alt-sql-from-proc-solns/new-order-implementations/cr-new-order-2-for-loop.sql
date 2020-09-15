create or replace procedure new_order(no in new_order_parameters_t)
  language plpgsql
as $body$
declare
  w_id_in             constant int            not null := no.w_id;
  d_id_in             constant int            not null := no.d_id;
  c_id_in             constant int            not null := no.c_id;
  o_all_local_in      constant int            not null := no.o_all_local;
  ols_in              constant order_line_t[] not null := no.ols;
  cardinality_ols_in  constant int            not null := cardinality(no.ols);

  d_next_o_id_v   district.d_next_o_id%type  not null := 0;

  i_price_v       item.i_price%type          not null := 0;

  s_quantity_v    stock.s_quantity%type      not null := 0;
  ol_dist_info_v  stock.s_dist_01%type       not null := '?';

  ol              order_line_t               not null := (0, 0, 0);
  n                                          int not null := 0;
begin
  ------------------------------------------------------------------------------
  declare
    c_discount_v    customer.c_discount%type;
    c_last_v        customer.c_last%type;
    c_credit_v      customer.c_credit%type;
    w_tax_v         warehouse.w_tax%type;
  begin
      select  c_discount,   c_last,   c_credit
      into    c_discount_v, c_last_v, c_credit_v
      from    customer
      where
        c_w_id = w_id_in  and
        c_d_id = d_id_in  and
        c_id   = c_id_in;

      select  w_tax
      into    w_tax_v  
      from    warehouse
      where   w_id = w_id_in;
  end;

  select d_next_o_id
  into d_next_o_id_v
  from district
  where d_w_id = w_id_in and d_id = d_id_in for update;

  update district
  set d_next_o_id = d_next_o_id + 1
  where d_w_id = w_id_in and d_id = d_id_in;

  insert into oorder    (o_id,          o_d_id,  o_w_id,  o_c_id,  o_entry_d, o_ol_cnt,           o_all_local)
  values                (d_next_o_id_v, d_id_in, w_id_in, c_id_in, now(),     cardinality_ols_in, o_all_local_in);

  insert into new_order (no_o_id,       no_d_id, no_w_id)
  values                (d_next_o_id_v, d_id_in, w_id_in);

  foreach ol in array ols_in loop
    n := n + 1;

    -- NOTE: bllewell added this (shown in the TPCC spec):
    --   alter table only stock
    --   add constraint s_fkey_i foreign key (s_i_id) references item(i_id);
    ---
    -- Probably not a measurable performance improvement.
    -- But it's good practice (one SELECT instead of TWO), so why not?
    --
    -- Select item-stock (single-row).
    select
      i.i_price, s.s_quantity,
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

    into i_price_v, s_quantity_v, ol_dist_info_v
 
    from item i inner join stock s on i.i_id = s.s_i_id

    where
      i.i_id =   ol.i_id and
      s.s_i_id = ol.i_id and
      s.s_w_id = ol.w_id
    for update of s;

    -- Insert order_line (single-row).
    insert into order_line (ol_o_id,       ol_d_id,  ol_w_id,  ol_number, ol_i_id, ol_supply_w_id, ol_quantity,  ol_amount,        ol_dist_info)
    values                 (d_next_o_id_v, d_id_in,  w_id_in,  n,         ol.i_id, ol.w_id,        ol.qty,       ol.qty*i_price_v, ol_dist_info_v);

    -- Update stock (single-row).
    update stock set
      s_quantity =
        case (s_quantity_v - ol.qty >= 10)
          when true then      s_quantity_v - ol.qty
          else           91 + s_quantity_v - ol.qty
        end,
      s_ytd        = s_ytd + ol.qty,
      s_order_cnt  = s_order_cnt + 1,
      s_remote_cnt = s_remote_cnt +
        case (w_id_in = ol.w_id)
          when true then 0
          else           1
        end
    where
      s_i_id = ol.i_id and
      s_w_id = ol.w_id;
  end loop;
end;
$body$;
