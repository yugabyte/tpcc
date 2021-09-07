package com.oltpbenchmark.benchmarks.tpcc.pojo;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;


public class TpccRunResults {
    public TestConf TestConfiguration = new TestConf();

    public RunResults Results = new RunResults();

    public List<LatencyList> Latencies = new ArrayList<>();

    public List<LatencyList> FailureLatencies = new ArrayList<>();

    public Map<String, List<LatencyList>> WorkerTaskLatency = new LinkedHashMap<>();

    public Map<String, RetryAttemptsData> RetryAttempts = new LinkedHashMap<>();

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

    public class TestConf {
        public int numNodes;
        public int numWarehouses;
        public int numDBConnections;
        public int warmupTimeInSecs;
        public int runTimeInSecs;
        public int numRetries;
        public String testStartTime;
    }

    public class RunResults {
        public double tpmc;
        public double efficiency;
        public double throughput;
        public double throughputMin;
        public double throughputMax;
    }

    public class RetryAttemptsData {
        public int count;
        public List retriesFailureCount;
    }
}

