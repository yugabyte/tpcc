# This script creates the required tables on a cluster.
# Arguments:
# 1. Number warehouses
# 2. IP of the master leader
# 3. Number of splits. This assumes we have split the cluster into (N + 1)
#    logical regions with the first one dedicated for the yb-master. The 'item'
#    table is pinned here as well.
# 4. Number of tablets per sub-table.
#
# The first sub-table is pinned to $cloud.$region.$zone2, the second to
# $cloud.$region.$zone3 as the first zone is reserved for the yb-master.


warehouses=${warehouses:-15000}
splits=${splits:-20}
tablets=${tablets:-24}
cloud=aws
region=us-west-2
zone=us-west-2c

wh_per_split=$(expr $warehouses / $splits)

while [ $# -gt 0 ]; do
   if [[ $1 == *"--"* ]]; then
        param="${1/--/}"
        declare $param="$2"
   fi
  shift
done

master_addrs=""
for n in $(cat yb_nodes.txt | head -n3);
do
  if [ ! -z "$n" ] 
  then
    master_addrs+=","
  fi
  master_addrs+="$n:7100"
done
ip=$(head -1 yb_nodes.txt)

ysqlsh="/mnt/d0/repositories/yugabyte-db/bin/ysqlsh -h $ip"
ybadmin="/mnt/d0/repositories/yugabyte-db/build/debug-gcc-dynamic/bin/yb-admin  --master_addresses $master_addrs"

# $1: table_name
# $2: column list
# $3: primary key
create_simple_table() {
    tablezone="${zone}0"
    $ybadmin modify_placement_info $cloud.$region.$tablezone 3
    $ysqlsh -d yugabyte -c "DROP TABLE IF EXISTS $1"
    $ysqlsh -d yugabyte -c "CREATE TABLE $1 ($2, PRIMARY KEY($3)) SPLIT INTO 3 TABLETS"
    $ybadmin modify_table_placement_info ysql.yugabyte $1 $cloud.$region.$tablezone 3
    return
}

# $1: table_name
# $2: column list
# $3: partition argument
# $4: column list without type
# $5: PRIMARY key list
create_partitioned_table() {

  # create parent table.
  $ysqlsh -d yugabyte -c "DROP TABLE IF EXISTS $1"
  $ysqlsh -d yugabyte -c "CREATE TABLE $1 ($2) PARTITION BY RANGE($3) SPLIT INTO 1 TABLETS"

  # Only history table does not have a pkey.
  pkey="";
  if [[ $# == '5' ]]
  then
    pkey=", PRIMARY KEY($5)";
  fi

  # create partitions
  for i in `seq 1 $splits`;
  do
    tablezone=$zone$(( i ))
    $ybadmin modify_placement_info $cloud.$region.$tablezone 3

    start=$(( (i-1)*wh_per_split+1 ))
    end=$(( (i*wh_per_split)+1  ))
    $ysqlsh -d yugabyte -c "CREATE TABLE $1$i PARTITION OF $1($4${pkey}) FOR VALUES FROM ($start) TO ($end) SPLIT INTO $tablets TABLETS";

    $ybadmin modify_table_placement_info ysql.yugabyte $1$i $cloud.$region.$tablezone 3
  done
}


# $1 index name.
# $2 table name.
# $3 indexed colummns.
# $4 is_unique
create_index() {
  for i in `seq 1 $splits`;
  do
    tablezone=$zone$(( i ))
    $ybadmin modify_placement_info $cloud.$region.$tablezone 3

    if [[ $4 == 0 ]]
    then
      $ysqlsh -d yugabyte -c "CREATE INDEX $1$i ON $2$i ($3)"
    else
      $ysqlsh -d yugabyte -c "CREATE UNIQUE INDEX $1$i ON $2$i ($3)"
    fi

    $ybadmin modify_table_placement_info ysql.yugabyte $1$i $cloud.$region.$tablezone 3
  done
}

set -x

$ybadmin set_load_balancer_enabled 0

create_simple_table 'item' \
             'i_id int NOT NULL,
              i_name varchar(24) NOT NULL,
              i_price decimal(5,2) NOT NULL,
              i_data varchar(50) NOT NULL,
              i_im_id int NOT NULL' \
              'i_id'


create_partitioned_table 'warehouse' \
             'w_id int NOT NULL,
              w_ytd decimal(12,2) NOT NULL,
              w_tax decimal(4,4) NOT NULL,
              w_name varchar(10) NOT NULL,
              w_street_1 varchar(20) NOT NULL,
              w_street_2 varchar(20) NOT NULL,
              w_city varchar(20) NOT NULL,
              w_state char(2) NOT NULL,
              w_zip char(9) NOT NULL' \
              'w_id'\
              'w_id, w_ytd, w_tax, w_name, w_street_1,  w_street_2, w_city, w_state, w_zip' \
              'w_id'

create_partitioned_table 'district' \
             'd_w_id int NOT NULL,
              d_id int NOT NULL,
              d_ytd decimal(12,2) NOT NULL,
              d_tax decimal(4,4) NOT NULL,
              d_next_o_id int NOT NULL,
              d_name varchar(10) NOT NULL,
              d_street_1 varchar(20) NOT NULL,
              d_street_2 varchar(20) NOT NULL,
              d_city varchar(20) NOT NULL,
              d_state char(2) NOT NULL,
              d_zip char(9) NOT NULL' \
             'd_w_id' \
             'd_w_id, d_id, d_ytd, d_tax, d_next_o_id, d_name, d_street_1, d_street_2, d_city, d_state, d_zip' \
             '(d_w_id,d_id) HASH'

create_partitioned_table 'customer' \
             'c_w_id int NOT NULL,
              c_d_id int NOT NULL,
              c_id int NOT NULL,
              c_discount decimal(4,4) NOT NULL,
              c_credit char(2) NOT NULL,
              c_last varchar(16) NOT NULL,
              c_first varchar(16) NOT NULL,
              c_credit_lim decimal(12,2) NOT NULL,
              c_balance decimal(12,2) NOT NULL,
              c_ytd_payment float NOT NULL,
              c_payment_cnt int NOT NULL,
              c_delivery_cnt int NOT NULL,
              c_street_1 varchar(20) NOT NULL,
              c_street_2 varchar(20) NOT NULL,
              c_city varchar(20) NOT NULL,
              c_state char(2) NOT NULL,
              c_zip char(9) NOT NULL,
              c_phone char(16) NOT NULL,
              c_since timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
              c_middle char(2) NOT NULL,
              c_data varchar(500) NOT NULL' \
             'c_w_id' \
             'c_w_id, c_d_id, c_id, c_discount, c_credit, c_last, c_first, c_credit_lim, c_balance, c_ytd_payment, c_payment_cnt,
              c_delivery_cnt, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_since, c_middle, c_data' \
             '(c_w_id,c_d_id) HASH,c_id' \

create_partitioned_table 'history' \
             'h_c_id int NOT NULL,
              h_c_d_id int NOT NULL,
              h_c_w_id int NOT NULL,
              h_d_id int NOT NULL,
              h_w_id int NOT NULL,
              h_date timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
              h_amount decimal(6,2) NOT NULL,
              h_data varchar(24) NOT NULL' \
             'h_w_id' \
             'h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, h_date, h_amount, h_data'

create_partitioned_table 'oorder' \
             'o_w_id int NOT NULL,
              o_d_id int NOT NULL,
              o_id int NOT NULL,
              o_c_id int NOT NULL,
              o_carrier_id int DEFAULT NULL,
              o_ol_cnt decimal(2,0) NOT NULL,
              o_all_local decimal(1,0) NOT NULL,
              o_entry_d timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP' \
             'o_w_id' \
             'o_w_id, o_d_id, o_id, o_c_id, o_carrier_id, o_ol_cnt, o_all_local, o_entry_d' \
             '(o_w_id,o_d_id) HASH,o_id'

create_partitioned_table 'stock' \
             's_w_id int NOT NULL,
              s_i_id int NOT NULL,
              s_quantity decimal(4,0) NOT NULL,
              s_ytd decimal(8,2) NOT NULL,
              s_order_cnt int NOT NULL,
              s_remote_cnt int NOT NULL,
              s_data varchar(50) NOT NULL,
              s_dist_01 char(24) NOT NULL,
              s_dist_02 char(24) NOT NULL,
              s_dist_03 char(24) NOT NULL,
              s_dist_04 char(24) NOT NULL,
              s_dist_05 char(24) NOT NULL,
              s_dist_06 char(24) NOT NULL,
              s_dist_07 char(24) NOT NULL,
              s_dist_08 char(24) NOT NULL,
              s_dist_09 char(24) NOT NULL,
              s_dist_10 char(24) NOT NULL'\
             's_w_id' \
             's_w_id, s_i_id, s_quantity, s_ytd, s_order_cnt, s_remote_cnt, s_data, s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10' \
             '(s_w_id,s_i_id)HASH' \

create_partitioned_table 'new_order' \
             'no_w_id int NOT NULL,
              no_d_id int NOT NULL,
              no_o_id int NOT NULL' \
             'no_w_id' \
             'no_w_id, no_d_id, no_o_id' \
             '(no_w_id,no_d_id) HASH,no_o_id'

create_partitioned_table 'order_line' \
             'ol_w_id int NOT NULL,
              ol_d_id int NOT NULL,
              ol_o_id int NOT NULL,
              ol_number int NOT NULL,
              ol_i_id int NOT NULL,
              ol_delivery_d timestamp NULL DEFAULT NULL,
              ol_amount decimal(6,2) NOT NULL,
              ol_supply_w_id int NOT NULL,
              ol_quantity decimal(2,0) NOT NULL,
              ol_dist_info char(24) NOT NULL' \
             'ol_w_id' \
             'ol_w_id, ol_d_id, ol_o_id, ol_number, ol_i_id, ol_delivery_d, ol_amount, ol_supply_w_id, ol_quantity, ol_dist_info' \
             '(ol_w_id,ol_d_id) HASH,ol_o_id,ol_number'

create_index 'idx_customer_name' 'customer' '(c_w_id,c_d_id) HASH,c_last,c_first' 0

create_index 'idx_order' 'oorder' '(o_w_id,o_d_id) HASH,o_c_id,o_id DESC' 1

$ybadmin clear_placement_info
$ybadmin set_load_balancer_enabled 1
