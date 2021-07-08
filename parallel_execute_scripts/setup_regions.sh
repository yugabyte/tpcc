set -ex

BASEZONE="us-west-2c"

NUMNODES=$(cat yb_nodes.txt | wc -l)
ybcli="/mnt/d0/repositories/yugabyte-db/build/latest/bin/yb-ts-cli"

shut_down_services() {
  for n in $(cat yb_nodes.txt);
  do
    ssh -i ./ssh/cluster.pem -ostricthostkeychecking=no -p 54422 yugabyte@$n /home/yugabyte/bin/yb-server-ctl.sh tserver stop
  done

  for n in $(cat yb_nodes.txt | head -n 3);
  do
    ssh -i ./ssh/cluster.pem -ostricthostkeychecking=no -p 54422 yugabyte@$n /home/yugabyte/bin/yb-server-ctl.sh master stop
  done
}

startup_services() {
  for n in $(cat yb_nodes.txt | head -n 3);
  do
    ssh -i ./ssh/cluster.pem -ostricthostkeychecking=no -p 54422 yugabyte@$n /home/yugabyte/bin/yb-server-ctl.sh master start
  done

  for n in $(cat yb_nodes.txt);
  do
    ssh -i ./ssh/cluster.pem -ostricthostkeychecking=no -p 54422 yugabyte@$n /home/yugabyte/bin/yb-server-ctl.sh tserver start
  done
}

clear_state() {
  for n in $(cat yb_nodes.txt);
  do
    ssh -i ./ssh/cluster.pem -ostricthostkeychecking=no -p 54422 yugabyte@$n 'rm -rf /mnt/d0/*; rm -rf /mnt/d1/*'
  done
}

set_zones() {
  i=0
  ZONEID=0

  for n in $(cat yb_nodes.txt);
  do
    ZONEID=$(( $i / 3 ))
    i=$(( $i + 1 ))
    ssh -i ./ssh/cluster.pem -ostricthostkeychecking=no -p 54422 yugabyte@$n sed -i "s/--placement_zone.*/--placement_zone=${BASEZONE}${ZONEID}/g" tserver/conf/server.conf
    ssh -i ./ssh/cluster.pem -ostricthostkeychecking=no -p 54422 yugabyte@$n sed -i "/^--placement_uuid/d" tserver/conf/server.conf
  done

  i=0
  ZONEID=0
  #Set the replication factor of the cluster to the number of nodes to make sure that the transaction table has the required number of tablets.
  for n in $(cat yb_nodes.txt | head -n 3);
  do
    ZONEID=$(( $i / 3 ))
    i=$(( $i + 1 ))
    ssh -i ./ssh/cluster.pem -ostricthostkeychecking=no -p 54422 yugabyte@$n sed -i "s/--placement_zone.*/--placement_zone=${BASEZONE}${ZONEID}/g" master/conf/server.conf
    ssh -i ./ssh/cluster.pem -ostricthostkeychecking=no -p 54422 yugabyte@$n sed -i "/^--placement_uuid/d" master/conf/server.conf
  done
}

set_master_replication_factor() {
  i=0
  ZONEID=0
  for n in $(cat yb_nodes.txt | head -n 3);
  do
    ZONEID=$(( $i / 3 ))
    i=$(( $i + 1 ))
    ssh -i ./ssh/cluster.pem -ostricthostkeychecking=no -p 54422 yugabyte@$n sed -i "s/--replication_factor.*/--replication_factor=$1/g" master/conf/server.conf
  done
}

remove_additional_tablets() {
  for n in $(cat yb_nodes.txt | head -n 3);
  do
    $ybcli --server_address $n:7100 set_flag -force load_balancer_max_concurrent_removals 50
  done

#  sleep 100
#
#  for n in $(cat yb_nodes.txt | head -n 3);
#  do
#    $ybcli --server_address $n:7100 set_flag -force load_balancer_max_concurrent_removals 1
#  done
}

#Set the replication factor of the cluster to the number of nodes to make sure that the transaction table has the required number of tablets.
shut_down_services
clear_state
set_zones
set_master_replication_factor $NUMNODES
startup_services

sleep 30

shut_down_services
set_master_replication_factor 3
startup_services
remove_additional_tablets
