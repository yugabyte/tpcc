#include <iostream>
#include <fstream>
#include <vector>
#include <numeric>

using namespace std;

void GetIps(const char *ip_string, vector<string>& ips) {
  string input(ip_string);
  int start = -1;
  int count = 0; // count of '.'
  for (int i = 0; i < input.size(); ++i) {
    if (input.at(i) == ',' || input.at(i) == ':') {
      if (count == 3) {
        ips.emplace_back(input.substr(start, i - start));
      }
      count = 0;
      start = -1;
      continue;
    }
    if (input.at(i) == '.') {
      ++count;
    }
    if (start == -1) {
      start = i;
    }
  }
  if (start != -1 && count == 3) {
    ips.emplace_back(input.substr(start, input.size() - start));
  }

  for (const auto& ip: ips) {
    cout << ip << " ";
  }
  cout << endl;
}

void OutputToFile(string filename, vector<string> ips, bool should_number) {
  ofstream out_file(filename.data());
  int i = 0;
  for (const auto& ip: ips) {
    out_file << (should_number ? (to_string(i++) + ":") : "") << ip << "\n";
  }
}

int main(int argc, char **argv) {
  if (argc < 3) {
    cout << "Usage : ./binary client_ips yb_nodes_ips";
    return 1;
  }
  vector<string> client_ips;
  vector<string> yb_node_ips;
  GetIps(argv[1], client_ips);
  GetIps(argv[2], yb_node_ips);

  OutputToFile("clients.txt", client_ips, true);
  OutputToFile("yb_nodes.txt", yb_node_ips, false);
}
