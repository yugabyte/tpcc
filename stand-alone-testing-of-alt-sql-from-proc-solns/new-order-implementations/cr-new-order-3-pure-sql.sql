-- Issue #738 and "ysql_suppress_unsupported_error=true" flag.
-- Done here for self-doc. But done at the start of each top-directory
-- "master script for maximum visibility.
set client_min_messages = error;

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
begin
  /*
  ------------------------------------------------------------------

  -- WEIRD: "w_tax" and "c_discount" should be used to honor
  -- this from the spec:
  --
  --   The total-amount for the complete order is computed as:
  --   sum(ol_amount) * (1 - c_discount) * (1 + w_tax + d_tax)
  --
  -- This must be referring to the "orders table", oorder.
  -- But it has no suitable column for this. It has only:
  --
  --    o_w_id       | integer
  --    o_d_id       | integer
  --    o_id         | integer
  --    o_c_id       | integer
  --    o_carrier_id | integer
  --    o_ol_cnt     | numeric(2,0)
  --    o_all_local  | numeric(1,0)
  --    o_entry_d    | timestamp without time zone

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

  ------------------------------------------------------------------
  */

  <<"using d_next_o_id_v">>
  declare
    -- Used in these SQLs:
    --   INSERT INTO oorder
    --   INSERT INTO new_order
    --   INSERT INTO order_line
    d_next_o_id_v district.d_next_o_id%type not null := 0;
  begin
    /*
    SEE ISSUE #5366

    -- UPDATE district is a lightening fast single-row update.
    -- So no opportunity to optimize.
    update district set
      d_next_o_id = d_next_o_id + 1
    where
      d_w_id = w_id_in and
      d_id   = d_id_in
    returning d_next_o_id
    into      d_next_o_id_v;

    -- Set d_next_o_id_v to the starting value of d_next_o_id
    -- for use below.

    d_next_o_id_v := d_next_o_id_v - 1;
    */

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

----------------------------------------------------------------------------------------------------------------------
    -- Notice that the correctness of the item-stock INNER JOIN
    -- depends on the FK "s_fkey_i". The TPCC spec requires this.
    -- But it was missing in the supplied Googled-for DDLs.

    insert into order_line(
      ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, 
      ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info)

    with
      -- The order is important to get the proper correlation
      -- between the order-line number and what item it specified.
      --
      -- It's a property of "unnest()" that it produces the rows
      -- in subscript order, so the empty OVER() is OK here.
      -- If we do away with the WITH view "order_lines" and put
      -- "unnest()" directly in the FROM list, then the result order,
      -- and therefore the "row_number()" values, seem to depend
      -- on the plan selection. And by this point, we can't access
      -- the original array element ordering.
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
  end "using d_next_o_id_v";
  ----------------------------------------------------------------------------------------------------------------------

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
