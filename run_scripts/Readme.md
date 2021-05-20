## Prerequisites:
Yugabyte cluster is deployed and is reachable.
TPCC client nodes are deployed and they can communicate with the Yugabyte cluster.
A client-manager machine that can run this script and has access to the Yugabyte cluster and the TPCC client nodes.

## Step 1. Create the Loader and execute scripts. 
First create 2 text files for clients and yugabyte nodes named `clients.txt` and `yb_nodes.txt`.
The file `yb_nodes.txt` needs the master node ips to be present first followed by the other ips.

## Step 2. Configure the client nodes.
This can be done as follows. Make sure that the environment has the ssh user
exported to the variable `SSH_USER` and the additional SSH AND SCP arguments
like the pem file or the port exported as `SSH_ARGS` and `SCP_ARGS`.
```sh
./setup_clients.sh
```

## Step 3. Modify variable and compile the program to run TPCC.
The variables that need to be modified are in the beginning of the cpp file.
They are:

1. total_warehouses:          This variable controls the total number of warehouses. 
                              The number of warehouses that each client works on is a factor of this quantity and the number of clients.
2. num_ips_per_client:        The number of ips supplied to each client.
3. client_repeat_count:       The number of clients that is given the same set of ips.
                              When num_ips_per_client is 3 and client_repeat_count is 2:
			      First client will get 3 YB node ips: ip1, ip2, ip3
			      Second client will get the same 3 YB node ips: ip1, ip2, ip3
			      Third client will get the next 3 YB node ips: ip4, ip5, ip6
			      Fourth client will also get the same 3 YB node ips: ip4, ip5, ip6
4. loader_threads_per_client: The number of threads used for loading per client.
5. num_connections_per_client:The number of connections used during the execute phase by each client.
5. delay_per_client:          The amount of delay between the start of consecutive clients during the execute phase.
			      When delay_per_client is 30:
			      First client starts at delay 0
			      Second client starts with a delay of 30 seconds
			      Third client starts with a delay of 60 seconds and so on.
6. load_delay_per_client:     Similar to the earlier variable but for the load phase.
7. ignore_masters:            Whether we have to ignore the master ips {The first 3 ips in the yb_nodes.txt file} during the load/execute phase. 
8. ssh_args:                  The additional options needed to login to the client nodes.
9. ssh_user:                  The user used to login to the client nodes.

Now compile the program:
```sh
g++ --std=c++11 run_tpcc_on_clients.cpp -o run_tpcc_on_clients
```

## Step 4. Create the TPCC tables.
This can be done from by:
```sh
./run_tpcc_on_clients create
```

## Step 5. Load the data.
This can be done by:
```
./run_tpcc_on_clients load
```

## Step 6. Enable the foreign keys.
This can be done as:
```sh
./run_tpcc_on_clients enable-foreign-keys
```

## Step 7. Execute the program.
This can be done as:
```sh
./run_tpcc_on_clients execute
```
This script creates an output file per client in /tmp/ directory with each client’s ip as part of the name of the output file.

If you do need to kill the currently running program at any stage, we can do
that as:
```sh
./run_tpcc_on_clients kill
```

## Step 8. Aggregate results from the various clients.
We can aggregate the TPM-C from the various clients as follows:
```sh
grep -i "tpm-c" /tmp/*execute*txt | awk '{print $4}' | paste -s -d+ - | bc 
```
