package com.oltpbenchmark.benchmarks.tpcc.pojo;

import java.util.List;
import java.util.Map;


public class TPCC_Metrics {

    public TestConfigurationObject TestConfiguration;

    public ResultObject Results;

    public List<LatencyList> Latencies;

    public List<LatencyList> Failure_Latencies;

    public Map<String, List<LatencyList>> Worker_Task_Latency;

    public List<RetryAttemptsObject> Retry_Attempts;

    public class LatencyList {
        public String Transaction;
        public String Count;
        public String Avg_Latency;
        public String P99_Latency;
        public String Connection_Acq_Latency;
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
        public String TPMC;
        public String Efficiency;
        public String Throughput;
    }

    public class RetryAttemptsObject {
        public String Transaction;
        public String Count;
        public List<String> Retries_FailureCount;
    }

}

