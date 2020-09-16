with
  v as (
    select new_order_parameters() as no)
select
  (no).w_id,
  (no).d_id,
  (no).c_id,
  (no).o_all_local,
  (no).ols
from v
\gset p_

\set w_id_in         :p_w_id
\set d_id_in         :p_d_id
\set c_id_in         :p_c_id
\set o_all_local_in  :p_o_all_local
\set ols_in '\'':p_ols'\'::order_line_t[]'
