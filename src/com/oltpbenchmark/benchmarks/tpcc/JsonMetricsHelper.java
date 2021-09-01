package com.oltpbenchmark.benchmarks.tpcc;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.oltpbenchmark.benchmarks.tpcc.pojo.TpccRunResults;
import com.oltpbenchmark.util.LatencyMetricsUtil;
import com.oltpbenchmark.util.FileUtil;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class JsonMetricsHelper {
    private static final Logger LOG = Logger.getLogger(JsonMetricsHelper.class);
    private TpccRunResults tpccRunResults;

    public JsonMetricsHelper() {
        tpccRunResults = new TpccRunResults();
    }

    public void setTestConfig(int numNodes, int numWH, int numDBConn, int warmuptime, int runtime, int numRetries) {
        tpccRunResults.TestConfiguration.numNodes = numNodes;
        tpccRunResults.TestConfiguration.numWarehouses = numWH;
        tpccRunResults.TestConfiguration.numDBConnections = numDBConn;
        tpccRunResults.TestConfiguration.warmupTimeInSecs = warmuptime;
        tpccRunResults.TestConfiguration.runTimeInSecs = runtime;
        tpccRunResults.TestConfiguration.numRetries = numRetries;
        tpccRunResults.TestConfiguration.testStartTime = new SimpleDateFormat("dd-MM-yy_HH:mm:ss").format(new Date());
    }

    public void setTestResults(String tpmc, String efficiency, String throughput) {
        tpccRunResults.Results.tpmc = Double.parseDouble(tpmc);
        tpccRunResults.Results.efficiency = Double.parseDouble(efficiency);
        tpccRunResults.Results.throughput = Double.parseDouble(throughput);
    }

    public void addLatency(String op, List<Integer> latencyList, List<Integer> connLatencyList) {
        tpccRunResults.Latencies.add(getLatencyValueList(op, latencyList, connLatencyList));
    }

    public void addFailureLatency(String op, List<Integer> latencyList, List<Integer> connLatencyList) {
        tpccRunResults.FailureLatencies.add(getLatencyValueList(op, latencyList, connLatencyList));
    }

    public void addWorkerTaskLatency(String op, String task, List<Integer> latencyList) {
        if (!tpccRunResults.WorkerTaskLatency.containsKey(op)) {
            tpccRunResults.WorkerTaskLatency.put(op, new ArrayList<>());
        }
        tpccRunResults.WorkerTaskLatency.get(op).add(getLatencyValueList(task, latencyList, null));
    }

    public void addRetry(String op, int count,int[] retryOpList) {
        TpccRunResults.RetryAttemptsData retryObj = tpccRunResults.new RetryAttemptsData();
        retryObj.count = count;
        retryObj.retriesFailureCount = Arrays.asList(retryOpList);
        tpccRunResults.RetryAttempts.put(op,retryObj);
    }

    private TpccRunResults.LatencyList getLatencyValueList(String op, List<Integer> latencyList,
                                                           List<Integer> connAcqLatencyList) {
        DecimalFormat df = new DecimalFormat();
        df.setMaximumFractionDigits(2);
        df.setGroupingUsed(false);
        TpccRunResults.LatencyList valueList = tpccRunResults.new LatencyList();
        valueList.Transaction = op;
        valueList.Count = latencyList.size();
        valueList.avgLatency = Double.parseDouble(df.format(LatencyMetricsUtil.getAverageLatency(latencyList)));
        valueList.P99Latency = Double.parseDouble(df.format(LatencyMetricsUtil.getP99Latency(latencyList)));
        if (connAcqLatencyList != null)
            valueList.connectionAcqLatency =
                    Double.parseDouble(df.format(LatencyMetricsUtil.getAverageLatency(connAcqLatencyList)));
        return valueList;
    }

    /* Writes the Json object to a JSON file */
    public void writeMetricsToJSONFile() {
        String outputDirectory = "results/json";
        FileUtil.makeDirIfNotExists(outputDirectory);
        String currentDir = "";
        try {
            currentDir = new File(".").getCanonicalPath();
        } catch (IOException e) {
            LOG.error("Exception occurred fetching current dir. " +
                    "\nError Message:" + e.getMessage());
        }
        String dest = currentDir + File.separator + outputDirectory + File.separator + "output.json";

        try (FileWriter fw = new FileWriter(dest)) {
            new GsonBuilder().setPrettyPrinting().create().toJson(tpccRunResults, fw);
        } catch (IOException e) {
            LOG.error("Got exception while writing JSON metrics to file.");
            e.printStackTrace();
            return;
        }
        LOG.info("Output json data into file: " + dest);
    }

    private double computeAverage(double old_value, double newValue, int count) {
        return ((old_value * count) + newValue) / (count + 1);
    }

    /*
    Will merge the results from json files in the given directory
     */
    public void mergeJsonResults(String dirPath) {
        List<TpccRunResults> listTpccRunResults = new ArrayList<>();
        String[] fileNames = new File(dirPath).list();
        Gson gson = new Gson();
        for (String file : fileNames) {
            if (!file.endsWith("json")) {
                continue;
            }
            try {
                listTpccRunResults.add(gson.fromJson(
                        new String(Files.readAllBytes(Paths.get(file))), TpccRunResults.class));
            } catch (IOException ie) {
                LOG.error("Got exception while reading json file - " + file + " : ", ie);
                return;
            }
        }
        int numNewOrder = 0;

        int fileCnt = 0;
        for (TpccRunResults tpccResult : listTpccRunResults) {
            tpccRunResults.TestConfiguration = tpccResult.TestConfiguration;

            if(fileCnt == 0) {
                tpccRunResults.Results.throughputMin = tpccResult.Results.throughput;
                tpccRunResults.Results.throughputMax = tpccResult.Results.throughput;
            } else {
                if (tpccRunResults.Results.throughputMin > tpccResult.Results.throughput)
                    tpccRunResults.Results.throughputMin = tpccResult.Results.throughput;
                if (tpccRunResults.Results.throughputMax < tpccResult.Results.throughput)
                    tpccRunResults.Results.throughputMax = tpccResult.Results.throughput;
            }
            tpccRunResults.Results.throughput = computeAverage(tpccRunResults.Results.throughput,
                    tpccResult.Results.throughput, fileCnt);

            List<TpccRunResults.LatencyList> latList = tpccResult.Latencies;
            for (int i = 0; i < latList.size() ; i++) {
                TpccRunResults.LatencyList opLatency = latList.get(i);
                TpccRunResults.LatencyList latency;
                if(opLatency.Transaction.equalsIgnoreCase("NewOrder"))
                    numNewOrder += opLatency.Count;
                if(tpccRunResults.Latencies.size() <= i) {
                    latency = tpccRunResults.new LatencyList();
                    latency.Transaction = opLatency.Transaction;
                    latency.minLatency = opLatency.avgLatency;
                    latency.maxLatency = opLatency.avgLatency;
                    latency.P99Latency = opLatency.P99Latency;
                    latency.minConnAcqLatency = opLatency.connectionAcqLatency;
                    latency.maxConnAcqLatency = opLatency.connectionAcqLatency;
                    tpccRunResults.Latencies.add(latency);
                } else {
                    latency = tpccRunResults.Latencies.get(i);
                    latency.minLatency = latency.minLatency > opLatency.avgLatency ?
                            opLatency.avgLatency : latency.minLatency;
                    latency.maxLatency = latency.maxLatency < opLatency.avgLatency ?
                            opLatency.avgLatency : latency.maxLatency;
                    latency.P99Latency = latency.P99Latency > opLatency.P99Latency ?
                            opLatency.P99Latency : latency.P99Latency;
                    latency.minConnAcqLatency = latency.minConnAcqLatency > opLatency.connectionAcqLatency ?
                            opLatency.connectionAcqLatency : latency.minConnAcqLatency;
                    latency.maxConnAcqLatency = latency.maxConnAcqLatency < opLatency.connectionAcqLatency ?
                            opLatency.connectionAcqLatency : latency.maxConnAcqLatency;
                }
                latency.Count +=  opLatency.Count;
                latency.avgLatency = computeAverage(latency.avgLatency, opLatency.avgLatency, fileCnt);
                latency.connectionAcqLatency = computeAverage(latency.connectionAcqLatency,
                        opLatency.connectionAcqLatency, fileCnt);
                tpccRunResults.Latencies.set(i, latency);
            }

            List<TpccRunResults.LatencyList> failureLatList = tpccResult.FailureLatencies;
            for (int i = 0; i < failureLatList.size() ; i++) {
                TpccRunResults.LatencyList opLatency = failureLatList.get(i);
                TpccRunResults.LatencyList failureLat;
                if (tpccRunResults.FailureLatencies.size() <= i) {
                    failureLat = tpccRunResults.new LatencyList();
                    failureLat.Transaction = opLatency.Transaction;
                    failureLat.minLatency = opLatency.avgLatency;
                    failureLat.maxLatency = opLatency.avgLatency;
                    failureLat.P99Latency = opLatency.P99Latency;
                    failureLat.minConnAcqLatency = opLatency.connectionAcqLatency;
                    failureLat.maxConnAcqLatency = opLatency.connectionAcqLatency;
                    tpccRunResults.FailureLatencies.add(failureLat);
                } else {
                    failureLat = tpccRunResults.FailureLatencies.get(i);
                    failureLat.minLatency = failureLat.minLatency > opLatency.avgLatency ?
                            opLatency.avgLatency : failureLat.minLatency;
                    failureLat.maxLatency = failureLat.maxLatency < opLatency.avgLatency ?
                            opLatency.avgLatency : failureLat.maxLatency;
                    failureLat.P99Latency = failureLat.P99Latency > opLatency.P99Latency ?
                            opLatency.P99Latency : failureLat.P99Latency;
                    failureLat.minConnAcqLatency = failureLat.minConnAcqLatency > opLatency.connectionAcqLatency ?
                            opLatency.connectionAcqLatency : failureLat.minConnAcqLatency;
                    failureLat.maxConnAcqLatency = failureLat.maxConnAcqLatency < opLatency.connectionAcqLatency ?
                            opLatency.connectionAcqLatency : failureLat.maxConnAcqLatency;
                }
                failureLat.Count += opLatency.Count;
                failureLat.avgLatency = computeAverage(failureLat.avgLatency, opLatency.avgLatency, fileCnt);
                failureLat.connectionAcqLatency = computeAverage(failureLat.connectionAcqLatency,
                        opLatency.connectionAcqLatency, fileCnt);
                tpccRunResults.FailureLatencies.set(i, failureLat);
            }
            fileCnt++;
        }

        double tpmc = 1.0 * numNewOrder * 60 / tpccRunResults.TestConfiguration.runTimeInSecs;
        tpccRunResults.Results.efficiency = 1.0 * tpmc * 100 / tpccRunResults.TestConfiguration.numWarehouses / 12.86;
        tpccRunResults.Results.tpmc = tpmc;

        writeMetricsToJSONFile();
    }

    public static void main(String args[]) {
        new JsonMetricsHelper().mergeJsonResults(args[0]);
    }
}
