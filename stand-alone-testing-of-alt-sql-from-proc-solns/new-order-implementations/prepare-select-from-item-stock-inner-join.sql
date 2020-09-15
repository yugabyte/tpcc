deallocate all;
prepare select_from_item_stock_join(int, int, int) as
select
  i.i_price, s.s_quantity,
  case $1
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

from item i inner join stock s on i.i_id = s.s_i_id

where
  i.i_id =   $2 and
  s.s_i_id = $2 and
  s.s_w_id = $3
for update of s;
