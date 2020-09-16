drop table if exists customer   cascade;
drop table if exists district   cascade;
drop table if exists history    cascade;
drop table if exists item       cascade;
drop table if exists new_order  cascade;
drop table if exists oorder     cascade;
drop table if exists order_line cascade;
drop table if exists stock      cascade;
drop table if exists warehouse  cascade;

create table customer(
  c_w_id           int not null,
  c_d_id           int not null,
  c_id             int not null,
  c_discount       numeric(4,4)  not null,
  c_credit         char(2) not null,
  c_last           varchar(16) not null,
  c_first          varchar(16) not null,
  c_credit_lim     numeric(12,2) not null,
  c_balance        numeric(12,2) not null,
  c_ytd_payment    double precision not null,
  c_payment_cnt    int not null,
  c_delivery_cnt   int not null,
  c_street_1       varchar(20) not null,
  c_street_2       varchar(20) not null,
  c_city           varchar(20) not null,
  c_state          char(2) not null,
  c_zip            char(9) not null,
  c_phone          char(16) not null,
  c_since          timestamp default current_timestamp not null,
  c_middle         char(2) not null,
  c_data           varchar(500) not null,

  primary key(c_w_id, c_d_id, c_id)
);

create table district(
  d_w_id           int not null,
  d_id             int not null,
  d_ytd            numeric(12,2) not null,
  d_tax            numeric(4,4) not null,
  d_next_o_id      int not null,
  d_name           varchar(10) not null,
  d_street_1       varchar(20) not null,
  d_street_2       varchar(20) not null,
  d_city           varchar(20) not null,
  d_state          char(2) not null,
  d_zip            char(9) not null,

  primary key(d_w_id, d_id)
);

-- The "New order" use-case doesn't use the "history" table.
-- From the TPCC spec:
--   Rows in the History table do not have a primary key as,
--   within the context of the benchmark, there is no need
--   to uniquely identify a row within this table.
--   So add "k serial" just for YB good practice.
create table history(
  k                serial,
  h_c_id           int not null,
  h_c_d_id         int not null,
  h_c_w_id         int not null,
  h_d_id           int not null,
  h_w_id           int not null,
  h_date           timestamp default current_timestamp not null,
  h_amount         numeric(6,2) not null,
  h_data           varchar(24) not null,

  primary key(k)
);
alter sequence history_k_seq cache 100000;

create table item(
  i_id             int not null,
  i_name           varchar(24) not null,
  i_price          numeric(5,2) not null,
  i_data           varchar(50) not null,
  i_im_id          int not null,

  primary key(i_id)
);

create table new_order(
  no_w_id        int not null,
  no_d_id        int not null,
  no_o_id        int not null,

  primary key(no_w_id, no_d_id, no_o_id)
);

create table oorder(
  o_w_id           int not null,
  o_d_id           int not null,
  o_id             int not null,
  o_c_id           int not null,
  o_carrier_id     int,
  o_ol_cnt         numeric(2,0) not null,
  o_all_local      numeric(1,0) not null,
  o_entry_d        timestamp default current_timestamp not null,

  primary key(o_w_id, o_d_id, o_id)
);

create table order_line(
  ol_w_id          int not null,
  ol_d_id          int not null,
  ol_o_id          int not null,
  ol_number        int not null,
  ol_i_id          int not null,
  ol_delivery_d    timestamp,
  ol_amount        numeric(6,2) not null,
  ol_supply_w_id   int not null,
  ol_quantity      numeric(2,0) not null,
  ol_dist_info     char(24) not null,

  primary key(ol_w_id, ol_d_id, ol_o_id, ol_number)
);

create table stock(
  s_w_id           int not null,
  s_i_id           int not null,
  s_quantity       numeric(4,0) not null,
  s_ytd            numeric(8,2) not null,
  s_order_cnt      int not null,
  s_remote_cnt     int not null,
  s_data           varchar(50) not null,
  s_dist_01        char(24) not null,
  s_dist_02        char(24) not null,
  s_dist_03        char(24) not null,
  s_dist_04        char(24) not null,
  s_dist_05        char(24) not null,
  s_dist_06        char(24) not null,
  s_dist_07        char(24) not null,
  s_dist_08        char(24) not null,
  s_dist_09        char(24) not null,
  s_dist_10        char(24) not null,

  primary key(s_w_id, s_i_id)
);

create table warehouse(
  w_id             int not null,
  w_ytd            numeric(12,2) not null,
  w_tax            numeric(4,4) not null,
  w_name           varchar(10) not null,
  w_street_1       varchar(20) not null,
  w_street_2       varchar(20) not null,
  w_city           varchar(20) not null,
  w_state          char(2) not null,
  w_zip            char(9) not null,

  primary key(w_id)
);
