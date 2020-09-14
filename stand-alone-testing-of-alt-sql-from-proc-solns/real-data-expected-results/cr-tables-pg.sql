drop table if exists district_expected_result   cascade;
drop table if exists new_order_expected_result  cascade;
drop table if exists oorder_expected_result     cascade;
drop table if exists order_line_expected_result cascade;
drop table if exists stock_expected_result      cascade;

set default_with_oids = false;

create table district_expected_result(
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

create table new_order_expected_result(
    no_w_id        int not null,
    no_d_id        int not null,
    no_o_id        int not null,

  primary key(no_w_id, no_d_id, no_o_id)
);

create table oorder_expected_result(
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

create table order_line_expected_result(
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

create table stock_expected_result(
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
