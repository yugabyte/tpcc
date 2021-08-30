package com.oltpbenchmark.benchmarks.tpcc.pojo;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;


public class TPCC_Metrics {

    public TestConfigurationObject TestConfiguration;

    public ResultObject Results = new ResultObject();

    public List<LatencyList> Latencies = new ArrayList<>();

    public List<LatencyList> Failure_Latencies = new ArrayList<>();

    public Map<String, List<LatencyList>> Worker_Task_Latency = new LinkedHashMap<>();

    public List<RetryAttemptsObject> Retry_Attempts = new ArrayList<>();

    public class LatencyList {
        public String Transaction;
        public int Count;
        public double min_Latency;
        public double Avg_Latency;
        public double max_Latency;
        public double P99_Latency;
        public double Connection_Acq_Latency;
        public double min_Conn_Acq_Latency;
        public double max_Conn_Acq_Latency;
    }

    public class TestConfigurationObject {
        public int Num_Nodes;
        public int Num_Warehouses;
        public int Num_DBConnections;
        public int WarmupTime_secs;
        public int RunTime_secs;
        public int Num_Retries;
        public String TestStartTime;
    }

    public class ResultObject {
        public double TPMC;
        public double Efficiency;
        public double Throughput;
        public double Throughput_min;
        public double Throughput_max;
    }

    public class RetryAttemptsObject {
        public String Transaction;
        public int Count;
        public List<String> Retries_FailureCount;
    }

}

