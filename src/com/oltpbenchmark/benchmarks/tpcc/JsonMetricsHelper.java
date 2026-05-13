package com.oltpbenchmark.benchmarks.tpcc;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.oltpbenchmark.benchmarks.tpcc.pojo.TpccRunResults;
import com.oltpbenchmark.util.FileUtil;
import com.oltpbenchmark.util.LatencyMetricsUtil;
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

    public void setTestConfig(int numNodes, int totalWH, int numWH, int numDBConn, int warmuptime, int runtime, int numRetries ) {
        tpccRunResults.TestConfiguration.numNodes = numNodes;
        tpccRunResults.TestConfiguration.totalWarehouses = totalWH;
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
        tpccRunResults.Latencies.put(op,getLatencyValueList(null, latencyList, connLatencyList));
    }

    public void addFailureLatency(String op, List<Integer> latencyList, List<Integer> connLatencyList) {
        tpccRunResults.FailureLatencies.put(op,getLatencyValueList(null, latencyList, connLatencyList));
    }

    public void addWorkerTaskLatency(String op, String task, List<Integer> latencyList) {
        if (!tpccRunResults.WorkerTaskLatency.containsKey(op)) {
            tpccRunResults.WorkerTaskLatency.put(op, new ArrayList<>());
        }
        tpccRunResults.WorkerTaskLatency.get(op).add(getLatencyValueList(task, latencyList, null));
    }

    public void addRetry(String op, int count, int[] retryOpList) {
        TpccRunResults.RetryAttemptsData retryObj = tpccRunResults.new RetryAttemptsData();
        retryObj.count = count;
        retryObj.retriesFailureCount = Arrays.asList(retryOpList);
        tpccRunResults.RetryAttempts.put(op, retryObj);
    }

    private TpccRunResults.LatencyList getLatencyValueList(String task, List<Integer> latencyList,
                                                           List<Integer> connAcqLatencyList) {
        DecimalFormat df = new DecimalFormat();
        df.setMaximumFractionDigits(2);
        df.setGroupingUsed(false);
        TpccRunResults.LatencyList valueList = tpccRunResults.new LatencyList();
        valueList.WorkerTask = task;
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

    private static double computeAverage(double old_value, double newValue, int count) {
        DecimalFormat df = new DecimalFormat();
        df.setMaximumFractionDigits(2);
        df.setGroupingUsed(false);
        return Double.parseDouble(df.format(((old_value * count) + newValue) / (count + 1)));
    }

    /*
    Will merge the results from json files in the given directory.
    The given directory should contain all the json results of a run copied from different clients.
     */
    public static void mergeJsonResults(String dirPath) {
        List<TpccRunResults> listTpccRunResults = new ArrayList<>();
        TpccRunResults mergedTpccResults = new TpccRunResults();
        String[] fileNames = new File(dirPath).list();
        Gson gson = new Gson();
        for (String file : fileNames) {
            if (!file.endsWith("json")) {
                continue;
            }
            try {
                file = dirPath + File.separator + file;
                if (new File(file).isFile()) {
                    listTpccRunResults.add(gson.fromJson(
                            new String(Files.readAllBytes(Paths.get(file))), TpccRunResults.class));
                }
            } catch (IOException ie) {
                LOG.error("Got exception while reading json file - " + file + " : ", ie);
                return;
            }
        }
        int numNewOrder = 0;

        int filesMergedIdx = 0;
        boolean firstFile = true;
        for (TpccRunResults tpccResult : listTpccRunResults) {

            if(filesMergedIdx == 0) {
                mergedTpccResults.TestConfiguration = tpccResult.TestConfiguration;
                mergedTpccResults.Results.throughputMin =
                        mergedTpccResults.Results.throughputMax = tpccResult.Results.throughput;
                mergedTpccResults.Results.throughput = tpccResult.Results.throughput;
            } else {
                if (mergedTpccResults.Results.throughputMin > tpccResult.Results.throughput)
                    mergedTpccResults.Results.throughputMin = tpccResult.Results.throughput;
                if (mergedTpccResults.Results.throughputMax < tpccResult.Results.throughput)
                    mergedTpccResults.Results.throughputMax = tpccResult.Results.throughput;
                mergedTpccResults.Results.throughput = computeAverage(mergedTpccResults.Results.throughput,
                        tpccResult.Results.throughput, filesMergedIdx);
            }

            Map<String, TpccRunResults.LatencyList> latList = tpccResult.Latencies;
            for (Map.Entry<String, TpccRunResults.LatencyList> entry : latList.entrySet()) {
                String op = entry.getKey();
                TpccRunResults.LatencyList opLatency = entry.getValue();
                TpccRunResults.LatencyList latency;
                if (op.equalsIgnoreCase("NewOrder"))
                    numNewOrder += opLatency.Count;

                if (!mergedTpccResults.Latencies.containsKey(op)) {
                    latency = mergedTpccResults.new LatencyList();
                    latency.avgLatency = opLatency.avgLatency;
                    latency.minLatency = opLatency.avgLatency;
                    latency.maxLatency = opLatency.avgLatency;
                    latency.P99Latency = opLatency.P99Latency;
                    latency.connectionAcqLatency = opLatency.connectionAcqLatency;
                    latency.minConnAcqLatency = opLatency.connectionAcqLatency;
                    latency.maxConnAcqLatency = opLatency.connectionAcqLatency;
                    mergedTpccResults.Latencies.put(op,latency);
                } else {
                    latency = mergedTpccResults.Latencies.get(op);
                    latency.avgLatency = computeAverage(latency.avgLatency,
                            opLatency.avgLatency, filesMergedIdx);
                    latency.connectionAcqLatency = computeAverage(latency.connectionAcqLatency,
                            opLatency.connectionAcqLatency, filesMergedIdx);
                    latency.minLatency = latency.minLatency > opLatency.avgLatency ?
                            opLatency.avgLatency : latency.minLatency;
                    latency.maxLatency = latency.maxLatency < opLatency.avgLatency ?
                            opLatency.avgLatency : latency.maxLatency;
                    latency.P99Latency = latency.P99Latency < opLatency.P99Latency ?
                            opLatency.P99Latency : latency.P99Latency;
                    latency.minConnAcqLatency = latency.minConnAcqLatency > opLatency.connectionAcqLatency ?
                            opLatency.connectionAcqLatency : latency.minConnAcqLatency;
                    latency.maxConnAcqLatency = latency.maxConnAcqLatency < opLatency.connectionAcqLatency ?
                            opLatency.connectionAcqLatency : latency.maxConnAcqLatency;
                    latency.avgLatency = computeAverage(latency.avgLatency, opLatency.avgLatency, filesMergedIdx);
                    latency.connectionAcqLatency = computeAverage(latency.connectionAcqLatency,
                            opLatency.connectionAcqLatency, filesMergedIdx);
                }
                latency.Count += opLatency.Count;
                mergedTpccResults.Latencies.put(op, latency);

            }

            Map<String, TpccRunResults.LatencyList> failureLatList = tpccResult.FailureLatencies;
            for (Map.Entry<String, TpccRunResults.LatencyList> entry : failureLatList.entrySet()) {
                String op = entry.getKey();
                TpccRunResults.LatencyList opLatency = entry.getValue();
                TpccRunResults.LatencyList failureLat;
                if (mergedTpccResults.FailureLatencies.containsKey(op)) {
                    failureLat = mergedTpccResults.new LatencyList();
                    failureLat.minLatency = opLatency.avgLatency;
                    failureLat.maxLatency = opLatency.avgLatency;
                    failureLat.P99Latency = opLatency.P99Latency;
                    failureLat.minConnAcqLatency = opLatency.connectionAcqLatency;
                    failureLat.maxConnAcqLatency = opLatency.connectionAcqLatency;
                    mergedTpccResults.FailureLatencies.put(op, failureLat);
                } else {
                    failureLat = mergedTpccResults.FailureLatencies.get(op);
                    failureLat.minLatency = failureLat.minLatency > opLatency.avgLatency ?
                            opLatency.avgLatency : failureLat.minLatency;
                    failureLat.maxLatency = failureLat.maxLatency < opLatency.avgLatency ?
                            opLatency.avgLatency : failureLat.maxLatency;
                    failureLat.P99Latency = failureLat.P99Latency < opLatency.P99Latency ?
                            opLatency.P99Latency : failureLat.P99Latency;
                    failureLat.minConnAcqLatency = failureLat.minConnAcqLatency > opLatency.connectionAcqLatency ?
                            opLatency.connectionAcqLatency : failureLat.minConnAcqLatency;
                    failureLat.maxConnAcqLatency = failureLat.maxConnAcqLatency < opLatency.connectionAcqLatency ?
                            opLatency.connectionAcqLatency : failureLat.maxConnAcqLatency;
                    failureLat.Count += opLatency.Count;
                    failureLat.avgLatency = computeAverage(failureLat.avgLatency, opLatency.avgLatency, filesMergedIdx);
                    failureLat.connectionAcqLatency = computeAverage(failureLat.connectionAcqLatency,
                            opLatency.connectionAcqLatency, filesMergedIdx);
                }

                failureLat.Count += opLatency.Count;
                if (failureLat.avgLatency != null) {
                    failureLat.avgLatency = computeAverage(failureLat.avgLatency,
                            opLatency.avgLatency, filesMergedIdx);
                } else failureLat.avgLatency = opLatency.avgLatency;
                if (failureLat.connectionAcqLatency != null) {
                    failureLat.connectionAcqLatency = computeAverage(failureLat.connectionAcqLatency,
                            opLatency.connectionAcqLatency, filesMergedIdx);
                } else failureLat.connectionAcqLatency = opLatency.connectionAcqLatency;
                mergedTpccResults.FailureLatencies.put(op, failureLat);
            }

            Map<String, TpccRunResults.RetryAttemptsData> retryAttempts = tpccResult.RetryAttempts;
            for (Map.Entry<String, TpccRunResults.RetryAttemptsData> entry : retryAttempts.entrySet()) {
                String op = entry.getKey();
                TpccRunResults.RetryAttemptsData retryAttemp = entry.getValue();
                TpccRunResults.RetryAttemptsData retryAttemptsData;
                if (filesMergedIdx == 0) {
                    retryAttemptsData = mergedTpccResults. new RetryAttemptsData();
                    retryAttemptsData.count = retryAttemp.count;
                    retryAttemptsData.retriesFailureCount = retryAttemp.retriesFailureCount;
                    mergedTpccResults.RetryAttempts.put(op, retryAttemptsData);
                } else {
                    retryAttemptsData = mergedTpccResults.RetryAttempts.get(op);
                    retryAttemptsData.count += retryAttemp.count;
                    List retryFailures = (List<Double>)retryAttemptsData.retriesFailureCount.get(0);
                    for(int i=0; i < retryFailures.size(); i++) {
                        retryFailures.set(i, (((List<Double>)retryAttemptsData.retriesFailureCount.get(0)).get(i)) + (((List<Double>)retryAttemp.retriesFailureCount.get(0)).get(i)));
                    }
                    retryAttemptsData.retriesFailureCount.set(0, retryFailures);
                    mergedTpccResults.RetryAttempts.put(op, retryAttemptsData);
                }
            }
            filesMergedIdx++;
        }

        JsonMetricsHelper jsonHelper = new JsonMetricsHelper();
        jsonHelper.tpccRunResults = mergedTpccResults;
        double tpmc = 1.0 * numNewOrder * 60 / mergedTpccResults.TestConfiguration.runTimeInSecs;
        mergedTpccResults.Results.efficiency = 1.0 * tpmc * 100 / mergedTpccResults.TestConfiguration.totalWarehouses / 12.86;
        mergedTpccResults.Results.tpmc = tpmc;
        if (mergedTpccResults.RetryAttempts.size() == 0) mergedTpccResults.RetryAttempts = null;
        if (mergedTpccResults.WorkerTaskLatency.size() == 0)
            mergedTpccResults.WorkerTaskLatency = null;

        jsonHelper.writeMetricsToJSONFile();
    }

    public static void main(String[] args) {
        mergeJsonResults(args[0]);
    }
}
