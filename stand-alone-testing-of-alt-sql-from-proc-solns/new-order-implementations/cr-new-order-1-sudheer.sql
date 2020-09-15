create or replace procedure new_order(no in new_order_parameters_t)
  language plpgsql
as $body$
declare
  ------------------------------------------------------------------------------
  -- Bllewell:                                                                --
  -- This is a trivial API shim to allow Sudheer's code to be called          --
  -- the same way as Bllewell's.                                              --
  w_id_in         constant int            not null := no.w_id;                --
  d_id_in         constant int            not null := no.d_id;                --
  c_id_in         constant int            not null := no.c_id;                --
  o_all_local_in  constant int            not null := no.o_all_local;         --
  ols_in          constant order_line_t[] not null := no.ols;                 --
  ols_in_cnt      constant int            not null := cardinality(ols_in);    --
                                                                              --
  -- Assigned immediately after "begin".                                      --
  item_ids        int[]                   not null := '{}';                   --
  supplier_wh     int[]                   not null := '{}';                   --
  order_qts       int[]                   not null := '{}';                   --
  ------------------------------------------------------------------------------

  c_discount_v customer.c_discount%type;
  c_last_v customer.c_last%type;
  c_credit_v customer.c_credit%type;

  w_tax_v warehouse.w_tax%type;

  d_next_o_id_v district.d_next_o_id%type;
  d_tax_v district.d_tax%type;

  i_price_v item.i_price%type;
  i_name_v item.i_name%type;
  i_data_v item.i_data%type;

  s_quantity_v stock.s_quantity%type;
  s_data_v stock.s_data%type;
  s_dist_01_v stock.s_dist_01%type;
  s_dist_02_v stock.s_dist_02%type;
  s_dist_03_v stock.s_dist_03%type;
  s_dist_04_v stock.s_dist_04%type;
  s_dist_05_v stock.s_dist_05%type;
  s_dist_06_v stock.s_dist_06%type;
  s_dist_07_v stock.s_dist_07%type;
  s_dist_08_v stock.s_dist_08%type;
  s_dist_09_v stock.s_dist_09%type;
  s_dist_10_v stock.s_dist_10%type;

  ol_dist_info_v stock.s_dist_01%type;
  j integer := 1;
  s_remote_cnt_increment integer := 0;

  --------------------------------------------------------------------------------
  /*                                                                            --
    Bllewell: As delivered, this was declared as INTEGER.                       --
    This was an error. See "ol_amount_v := order_qts[j] * i_price_v"            --
    and the declation "i_price_v item.I_PRICE%TYPE", i.e. numeric(5,2).         --
    The effect of this bug was to round all values of "order_line.ol_amount"    --
    to a whole number of dollars. Try this on the starting data:                --
                                                                                --
          select ol_amount from order_line                                      --
          where ol_amount > 0.0                                                 --
          limit 5;                                                              --
                                                                                --
    You'll see that the values have non-zero cent amounts.                      --
  */                                                                            --
                                                                                --
  ol_amount_v item.I_PRICE%TYPE;                                                --
                                                                                --
  --------------------------------------------------------------------------------
begin

  -- Bllewell: finish the API shim. ----------------------------------------------
  for j in 1..ols_in_cnt loop                                                   --
    item_ids[j]     := ols_in[j].i_id;                                          --
    supplier_wh[j]  := ols_in[j].w_id;                                          --
    order_qts[j]    := ols_in[j].qty;                                           --
  end loop;                                                                     --
  --------------------------------------------------------------------------------

  select c_discount, c_last, c_credit into c_discount_v, c_last_v, c_credit_v
  from customer
  where c_w_id = w_id_in and c_d_id = d_id_in and c_id = c_id_in;

  select w_tax into w_tax_v
  from warehouse
  where w_id=w_id_in;

  select d_next_o_id, d_tax
  into d_next_o_id_v, d_tax_v
  from district
  where d_w_id = w_id_in and d_id = d_id_in
  for update;

  update district set
  d_next_o_id = d_next_o_id + 1 
  where d_w_id = w_id_in and d_id = d_id_in;

  insert into oorder(o_id,          o_d_id,  o_w_id,  o_c_id,  o_entry_d, o_ol_cnt,   o_all_local)
  values            (d_next_o_id_v, d_id_in, w_id_in, c_id_in, now(),     ols_in_cnt, o_all_local_in);

  insert into new_order(no_o_id,       no_d_id, no_w_id)
  values               (d_next_o_id_v, d_id_in, w_id_in);

  loop
    exit when j = ols_in_cnt + 1;

    select i_price,   i_name,   i_data
    into   i_price_v, i_name_v, i_data_v
    from item
    where i_id = item_ids[j];

    select s_quantity, s_data,   s_dist_01,   s_dist_02,   s_dist_03,   s_dist_04,   s_dist_05,   s_dist_06,   s_dist_07,   s_dist_08,   s_dist_09,   s_dist_10
    into s_quantity_v, s_data_v, s_dist_01_v, s_dist_02_v, s_dist_03_v, s_dist_04_v, s_dist_05_v, s_dist_06_v, s_dist_07_v, s_dist_08_v, s_dist_09_v, s_dist_10_v
    from stock
    where s_i_id = item_ids[j] and s_w_id = supplier_wh[j] for update;

    if s_quantity_v - order_qts[j] >= 10 then
      s_quantity_v := s_quantity_v - order_qts[j];
    else
      s_quantity_v := s_quantity_v + 91 - order_qts[j];
    end if;

    if w_id_in = supplier_wh[j] then
      s_remote_cnt_increment := 0;
    else
      s_remote_cnt_increment := 1;
    end if;

    if d_id_in = 1 then
      ol_dist_info_v := s_dist_01_v;
    elseif d_id_in = 2 then
      ol_dist_info_v := s_dist_02_v;
    elseif d_id_in = 2 then
      ol_dist_info_v := s_dist_02_v;
    elseif d_id_in = 3 then
      ol_dist_info_v := s_dist_03_v;
    elseif d_id_in = 4 then
      ol_dist_info_v := s_dist_04_v;
    elseif d_id_in = 5 then
      ol_dist_info_v := s_dist_05_v;
    elseif d_id_in = 6 then
      ol_dist_info_v := s_dist_06_v;
    elseif d_id_in = 7 then
      ol_dist_info_v := s_dist_07_v;
    elseif d_id_in = 8 then
      ol_dist_info_v := s_dist_08_v;
    elseif d_id_in = 9 then
      ol_dist_info_v := s_dist_09_v;
    else
      ol_dist_info_v := s_dist_10_v;
    end if;

    ol_amount_v := order_qts[j]*i_price_v;
    update stock set
      s_quantity   = s_quantity_v,
      s_ytd        = s_ytd + order_qts[j],
      s_order_cnt  = s_order_cnt + 1,
      s_remote_cnt = s_remote_cnt + s_remote_cnt_increment
    where
      s_i_id = item_ids[j] and
      s_w_id = supplier_wh[j];

    insert into order_line(ol_o_id,       ol_d_id, ol_w_id, ol_number, ol_i_id,     ol_supply_w_id, ol_quantity,  ol_amount,   ol_dist_info)
    values                (d_next_o_id_v, d_id_in, w_id_in, j,         item_ids[j], supplier_wh[j], order_qts[j], ol_amount_v, ol_dist_info_v);

    j := j + 1;
  end loop;
end;
$body$;
