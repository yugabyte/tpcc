package com.oltpbenchmark.benchmarks.tpcc.pojo;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;


public class TPCCMetrics {

    public TestConfigurationObject TestConfiguration;

    public ResultObject Results = new ResultObject();

    public List<LatencyList> Latencies = new ArrayList<>();

    public List<LatencyList> FailureLatencies = new ArrayList<>();

    public Map<String, List<LatencyList>> WorkerTaskLatency = new LinkedHashMap<>();

    public List<RetryAttemptsObject> RetryAttempts = new ArrayList<>();

    public class LatencyList {
        public String Transaction;
        public int Count;
        public double minLatency;
        public double avgLatency;
        public double maxLatency;
        public double P99Latency;
        public double connectionAcqLatency;
        public double minConnAcqLatency;
        public double maxConnAcqLatency;
    }

    public class TestConfigurationObject {
        public int numNodes;
        public int numWarehouses;
        public int numDBConnections;
        public int warmupTimeInSecs;
        public int runTimeInSecs;
        public int numRetries;
        public String testStartTime;
    }

    public class ResultObject {
        public double tpmc;
        public double efficiency;
        public double throughput;
        public double throughputMin;
        public double throughputMax;
    }

    public class RetryAttemptsObject {
        public String transaction;
        public int count;
        public List retriesFailureCount;
    }

}

