#include <iostream>
#include <fstream>
#include <vector>
#include <numeric>
#include <sstream>
#include <stdio.h>
#include <string.h>
#include <thread>
#include <chrono>

using namespace std;

// This variable controls the total number of warehouses. 
const int total_warehouses = 100100;

// The number of ips supplied to each client.
const int num_ips_per_client = 3; 

// The number of clients that is given the same set of ips.
// When num_ips_per_client is 3 and client_repeat_count is 2:
// First client will get 3 YB node ips: ip1, ip2, ip3.
// Second client will get the same 3 YB node ips: ip1, ip2, ip3.
// Third client will get the next 3 YB node ips: ip4, ip5, ip6.
// Fourth client will also get the same 3 YB node ips: ip4, ip5, ip6.
const int client_repeat_count = 2;

// The number of threads used for loading per client.
const int loader_threads_per_client = 21;

// The number of connections used during the execute phase by each client.
const int num_connections_per_client = 200;

// The amount of delay between the start of consecutive clients during the execute phase.
// When delay_per_client is 30:
// First client starts at delay 0
// Second client starts with a delay of 30 seconds
// Third client starts with a delay of 60 seconds and so on.
const int delay_per_client = 30;

// Similar to the earlier variable but for the load phase. 
const int load_delay_per_client = 20;

// Whether we have to ignore the master ips {The first 3 ips in the yb_nodes.txt file} during the load/execute phase.
const bool ignore_masters = true;

// Name of the file that contains the client ips.
const string clients_file = "clients.txt";

// Name of the file containing the YB nodes.
const string yb_nodes_file = "yb_nodes.txt";

// SSH args for logging into the client nodes.
const string ssh_args = "-i key.pem -ostricthostkeychecking=no -p 54422";

// SSH user for logging into the client node.
const string ssh_user = "centos";

// Suffix added to every output file created by the program.
string file_suffix = "";

void ExecOnServer(const string& cmd, string ip, string out_file) {
  stringstream ss;
  ss << "nohup ssh " << ssh_args << " " << ssh_user << "@" << ip << " \'" << cmd << "\' > /tmp/" << ip << "_" << out_file << "_" << file_suffix << ".txt" << " &";
  string ssh_cmd = ss.str();

  cout << "SSH command " << ssh_cmd << "\n";
  system(ssh_cmd.data());
}


void ReadServerIps(vector<string>& ips) {
  ifstream nodes(yb_nodes_file.data());
  string line;
  int i = 0;
  while (getline(nodes, line)) {
    // Skipping masters. The first 3 ips are assumed to be the master IPs.
    if (ignore_masters && i < 3) {
      i++;
      continue;
    }
    ips.emplace_back(line);
  }
}

void ReadClientIps(vector<string>& ips) {
  ifstream clients(clients_file.data());
  string line;
  while (getline(clients, line)) {
    ips.emplace_back(line);
  }
}

class IpsIterator {
 public:
  IpsIterator(const vector<string>& ips, int num_ips_per_client, int repeat_count) :
    ips_(ips),
    num_ips_per_client_(num_ips_per_client),
    repeat_count_(repeat_count),
    idx_(0),
    current_count_(0) {
  }

  string GetNext() {
    string execute_ips = "";
    int idx = idx_;
    for (int j = 0; j < num_ips_per_client_ && idx < ips_.size(); ++j) {
      if (j != 0) execute_ips += ",";
      execute_ips += ips_.at(idx++);
    }

    ++current_count_;
    if (current_count_ == repeat_count_) {
      current_count_ = 0;
      idx_ = idx;
    }

    return execute_ips;
  }

  int GetNumSplits() {
    return (ips_.size() + num_ips_per_client_ - 1) / num_ips_per_client_ * repeat_count_;
  }

 private:
  const vector<string>& ips_;
  const int num_ips_per_client_;
  const int repeat_count_;
  int idx_;
  int current_count_;
};

// This runs the create stage where we create the database, tables and procedures.
// This is run from one client.
void RunCreate(const vector<string>& ips, const vector<string>& client_ips) {
  IpsIterator ip_iterator(ips, num_ips_per_client, client_repeat_count);
  stringstream ss;
  ss << "cd tpcc; ~/tpcc/tpccbenchmark --create=true"
     << " --nodes=" << ip_iterator.GetNext();
  ExecOnServer(ss.str(), client_ips.at(0), "create");
}

// This function performs the Load stage where we load the table with the initial data.
// This is run from all the clients.
void RunLoad(const vector<string>& ips, const vector<string>& client_ips) {
  IpsIterator ip_iterator(ips, num_ips_per_client, client_repeat_count);
  int load_splits = ip_iterator.GetNumSplits();
  int warehouses_per_split = total_warehouses / load_splits;
  int initial_delay_per_client = 0;

  cout << "LOAD SPLITS: " << load_splits << "\nWH per split " << warehouses_per_split << "\n";
  for (int i = 0; i < load_splits; ++i) {
    stringstream ss;
    ss << "cd tpcc; ~/tpcc/tpccbenchmark --load=true"
       << " --nodes=" << ip_iterator.GetNext()
       << " --total-warehouses=" << total_warehouses
       << " --warehouses=" << warehouses_per_split
       << " --start-warehouse-id=" << i * warehouses_per_split + 1
       << " --loaderthreads=" << loader_threads_per_client
       << " --initial-delay-secs=" << initial_delay_per_client;

    initial_delay_per_client += load_delay_per_client;
    ExecOnServer(ss.str(), client_ips.at(i), "loader");
    //cout << ss.str() << endl;
  }
}

// This function performs the execute stage.
// This is run from all the clients.
void RunExecute(const vector<string>& ips, const vector<string>& client_ips) {
  IpsIterator ip_iterator(ips, num_ips_per_client, client_repeat_count);
  int execute_splits = ip_iterator.GetNumSplits();
  int warehouses_per_split = total_warehouses / execute_splits;
  int initial_delay_per_client = 0;
  int warmup_time = 60 * 22;

  for (int i = 0; i < execute_splits; ++i) {
    stringstream ss;
    ss << "cd tpcc; ~/tpcc/tpccbenchmark --execute=true"
       << " --nodes=" << ip_iterator.GetNext()
       << " --num-connections=" << num_connections_per_client
       << " --total-warehouses=" << total_warehouses
       << " --warehouses=" << warehouses_per_split
       << " --start-warehouse-id=" << i * warehouses_per_split + 1
       << " --warmup-time-secs=" << warmup_time
       << " --initial-delay-secs=" << initial_delay_per_client;

    ExecOnServer(ss.str(), client_ips.at(i), "execute");
    //cout << ss.str() << endl;

    initial_delay_per_client += delay_per_client;
    warmup_time -= delay_per_client;
  }
}

// This function creates the SQL procedures if they are not already created.
// This is run from one client.
void RunCreateProcedures(const vector<string>& ips, const vector<string>& client_ips) {
  IpsIterator ip_iterator(ips, num_ips_per_client, client_repeat_count);
  stringstream ss;
  ss << "cd tpcc; ~/tpcc/tpccbenchmark --create-sql-procedures=true"
     << " --nodes=" << ip_iterator.GetNext();
 ExecOnServer(ss.str(), client_ips.at(0), "create-procedures");
}

// This step needs to be done after the loading is done.It enables all the foreign key constraints.
// This is run from one client.
void EnableForeignKeys(const vector<string>& ips, const vector<string>& client_ips) {
  IpsIterator ip_iterator(ips, num_ips_per_client, client_repeat_count);
  stringstream ss;
  ss << "cd tpcc; ~/tpcc/tpccbenchmark --enable-foreign-keys=true"
     << " --nodes=" << ip_iterator.GetNext();
 ExecOnServer(ss.str(), client_ips.at(0), "enable-foreign-keys");
}

// Kills the currently running process on each TPCC client.
void KillTpcc(const vector<string>& client_ips) {
  for (auto& ip: client_ips) {
    ExecOnServer("pkill -9 java", ip, "kill");
  }
}

int main(int argc, char** argv) {
  vector<string> ips;
  ReadServerIps(ips);

  vector<string> client_ips;
  ReadClientIps(client_ips);

  cout << argc <<endl;
  if (argc > 2) {
    string x(argv[2]);
    file_suffix = x;
  }

  if (strcmp(argv[1], "load") == 0) {
    RunLoad(ips, client_ips);
  } else if (strcmp(argv[1], "create") == 0) {
    RunCreate(ips, client_ips);
  } else if (strcmp(argv[1], "execute") == 0) {
    RunExecute(ips, client_ips);
  } else if (strcmp(argv[1], "create-procedures") == 0) {
    RunCreateProcedures(ips, client_ips);
  } else if (strcmp(argv[1], "enable-foreign-keys") == 0) {
    EnableForeignKeys(ips, client_ips);
  } else if (strcmp(argv[1], "kill") == 0) {
    KillTpcc(client_ips);
  }
}
