create unique index idx_order
on oorder (o_w_id, o_d_id, o_c_id, o_id desc);

create unique index idx_s_i_id
on stock(s_i_id);

alter table only new_order add constraint no_fkey_o
  foreign key(no_w_id, no_d_id, no_o_id)
  references oorder(o_w_id, o_d_id, o_id)
  on delete cascade;

alter table only order_line add constraint ol_fkey_o
  foreign key (ol_w_id, ol_d_id, ol_o_id)
  references oorder(o_w_id, o_d_id, o_id)
  on delete cascade;

alter table only order_line add constraint ol_fkey_s
  foreign key (ol_supply_w_id, ol_i_id)
  references stock(s_w_id, s_i_id)
  on delete cascade;

alter table only stock add constraint s_fkey_i
  foreign key (s_i_id)
  references item(i_id)
  on delete cascade;
