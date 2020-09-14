-- Bllewell:
-- Good practice recomments creating an index on every column list
-- that defines a table's FK constraint.

create index idx_customer_name
on customer (c_w_id, c_d_id, c_last, c_first);

create unique index idx_order
on oorder (o_w_id, o_d_id, o_c_id, o_id desc);

alter table only customer
add constraint c_fkey_d foreign key(c_w_id, c_d_id) references district(d_w_id, d_id);
create index c_fkey_d on customer(c_w_id, c_d_id); -- Bllewell

alter table only history
add constraint h_fkey_c foreign key(h_c_w_id, h_c_d_id, h_c_id) references customer(c_w_id, c_d_id, c_id);
create index h_fkey_c on history(h_c_w_id, h_c_d_id, h_c_id); -- Bllewell

alter table only history
add constraint h_fkey_d foreign key(h_w_id, h_d_id) references district(d_w_id, d_id);
create index h_fkey_d on history(h_w_id, h_d_id); -- Bllewell

alter table only new_order
add constraint no_fkey_o foreign key(no_w_id, no_d_id, no_o_id) references oorder(o_w_id, o_d_id, o_id);
create index no_fkey_o on new_order(no_w_id, no_d_id, no_o_id); -- Bllewell

alter table only oorder
add constraint o_fkey_c foreign key(o_w_id, o_d_id, o_c_id) references customer(c_w_id, c_d_id, c_id);
create index o_fkey_c on oorder(o_w_id, o_d_id, o_c_id); -- Bllewell

alter table only order_line
add constraint ol_fkey_o foreign key(ol_w_id, ol_d_id, ol_o_id) references oorder(o_w_id, o_d_id, o_id);
create index ol_fkey_o on order_line(ol_w_id, ol_d_id, ol_o_id); -- Bllewell

alter table only order_line
add constraint ol_fkey_s foreign key(ol_supply_w_id, ol_i_id) references stock(s_w_id, s_i_id);
create index ol_fkey_s on order_line(ol_supply_w_id, ol_i_id); -- Bllewell

----------------------------------------------------------------------
-- Added by bllewell.
alter table only stock
add constraint s_fkey_i foreign key(s_i_id) references item(i_id);
create index s_fkey_i on stock(s_i_id); -- Bllewell

drop function if exists read_only_table() cascade;
create function read_only_table()
  returns trigger
  immutable
  language plpgsql
as $body$
begin
  raise exception 'The "%" table is read-only', tg_argv[0];
end;
$body$;

create trigger customer_read_only
before insert or update or delete on "customer"
execute procedure read_only_table('customer');

create trigger history_read_only
before insert or update or delete on "history"
execute procedure read_only_table('history');

create trigger item_read_only
before insert or update or delete on "item"
execute procedure read_only_table('item');

create trigger warehouse_read_only
before insert or update or delete on "warehouse"
execute procedure read_only_table('warehouse');
----------------------------------------------------------------------
