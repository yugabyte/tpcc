\set wid  1
\set did  4
\set cid 21

-- Lightening fast
select  c_discount,   c_last,   c_credit
from    customer
where
  c_w_id = :wid and
  c_d_id = :did and
  c_id   = :cid;

-- Lightening fast
select  w_tax
from    warehouse
where
  w_id = :wid;

update district set
  d_next_o_id = d_next_o_id + 1
where
  d_w_id = :wid and
  d_id   = :did
returning d_next_o_id, d_tax;
