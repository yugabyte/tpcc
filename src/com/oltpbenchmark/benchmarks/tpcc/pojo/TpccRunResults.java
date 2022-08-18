package com.oltpbenchmark.benchmarks.tpcc.pojo;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;


public class TpccRunResults {
    public TestConf TestConfiguration = new TestConf();

    public RunResults Results = new RunResults();

    public Map<String, LatencyList> Latencies = new LinkedHashMap<>();

    public Map<String, LatencyList> FailureLatencies = new LinkedHashMap<>();

    public Map<String, List<LatencyList>> WorkerTaskLatency = new LinkedHashMap<>();

    public Map<String, RetryAttemptsData> RetryAttempts = new LinkedHashMap<>();

    public class LatencyList {
        public String WorkerTask;
        public int Count;
        public Double avgLatency;
        public Double minLatency;
        public Double maxLatency;
        public Double P99Latency;
        public Double connectionAcqLatency;
        public Double minConnAcqLatency;
        public Double maxConnAcqLatency;
    }

    public class TestConf {
        public int numNodes;
        public int totalWarehouses;
        public int numWarehouses;
        public int numDBConnections;
        public int warmupTimeInSecs;
        public int runTimeInSecs;
        public int numRetries;
        public String testStartTime;
    }

    public class RunResults {
        public Double tpmc;
        public Double efficiency;
        public Double throughput;
        public Double throughputMin;
        public Double throughputMax;
    }

    public class RetryAttemptsData {
        public int count;
        public List retriesFailureCount;
    }
}

