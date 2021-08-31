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
        if(tpccRunResults.TestConfiguration == null)
            tpccRunResults.TestConfiguration = tpccRunResults.new TestConf();
        tpccRunResults.TestConfiguration.numNodes = numNodes;
        tpccRunResults.TestConfiguration.numWarehouses = numWH;
        tpccRunResults.TestConfiguration.numDBConnections = numDBConn;
        tpccRunResults.TestConfiguration.warmupTimeInSecs = warmuptime;
        tpccRunResults.TestConfiguration.runTimeInSecs = runtime;
        tpccRunResults.TestConfiguration.numRetries = numRetries;
        tpccRunResults.TestConfiguration.testStartTime = new SimpleDateFormat("dd-MM-yy_HH:mm:ss").format(new Date());
    }

    public void setTestResults(String tpmc, String efficiency, String throughput) {
        if(tpccRunResults.Results == null)
            tpccRunResults.Results = tpccRunResults.new RunResults();
        tpccRunResults.Results.tpmc = Double.parseDouble(tpmc);
        tpccRunResults.Results.efficiency = Double.parseDouble(efficiency);
        tpccRunResults.Results.throughput = Double.parseDouble(throughput);
    }

    public void addLatency(String op, List<Integer> latencyList, List<Integer> connLatencyList) {
        tpccRunResults.Latencies.add(getValueList(op, latencyList, connLatencyList));
    }

    public void addFailureLatency(String op, List<Integer> latencyList, List<Integer> connLatencyList) {
        tpccRunResults.FailureLatencies.add(getValueList(op, latencyList, connLatencyList));
    }

    public void addWorkerTaskLatency(String op, String task, List<Integer> latencyList) {
        if (!tpccRunResults.WorkerTaskLatency.containsKey(op)) {
            tpccRunResults.WorkerTaskLatency.put(op, new ArrayList<>());
        }
        tpccRunResults.WorkerTaskLatency.get(op).add(getValueList(task, latencyList, null));
    }

    public void addRetry(String op, int count,int[] retryOpList) {
        TpccRunResults.RetryAttemptsData retryObj = tpccRunResults.new RetryAttemptsData();
        retryObj.count = count;
        retryObj.retriesFailureCount = Arrays.asList(retryOpList);
        tpccRunResults.RetryAttempts.put(op,retryObj);
    }

    private TpccRunResults.LatencyList getValueList(String op, List<Integer> latencyList,
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
        List<TpccRunResults> listTpccMetrics = new ArrayList<>();
        String[] fileNames = new File(dirPath).list();
        TpccRunResults mergedTpccRunResults = new TpccRunResults();
        Gson gson = new Gson();
        for (String file : fileNames) {
            if (!file.endsWith("json")) {
                continue;
            }
            try {
                listTpccMetrics.add(gson.fromJson(
                        new String(Files.readAllBytes(Paths.get(file))), TpccRunResults.class));
            } catch (IOException ie) {
                LOG.error("Got exception while reading json file - " + file + " : ", ie);
                return;
            }
        }
        int numNewOrder = 0;

        int fileCnt = 0;
        for (TpccRunResults tpcc_metrics : listTpccMetrics) {
            tpccRunResults.TestConfiguration = tpcc_metrics.TestConfiguration;

            if(fileCnt == 0) {
                mergedTpccRunResults.Results.throughputMin = tpcc_metrics.Results.throughput;
                mergedTpccRunResults.Results.throughputMax = tpcc_metrics.Results.throughput;
            } else {
                if (mergedTpccRunResults.Results.throughputMin > tpcc_metrics.Results.throughput)
                    mergedTpccRunResults.Results.throughputMin = tpcc_metrics.Results.throughput;
                if (mergedTpccRunResults.Results.throughputMax < tpcc_metrics.Results.throughput)
                    mergedTpccRunResults.Results.throughputMax = tpcc_metrics.Results.throughput;
            }
            mergedTpccRunResults.Results.throughput = computeAverage(mergedTpccRunResults.Results.throughput,
                    tpcc_metrics.Results.throughput, fileCnt);

            List<TpccRunResults.LatencyList> latList = tpcc_metrics.Latencies;
            for (int i = 0; i < latList.size() ; i++) {
                TpccRunResults.LatencyList opLatency = latList.get(i);
                TpccRunResults.LatencyList sumLat;
                if(opLatency.Transaction.equalsIgnoreCase("NewOrder"))
                    numNewOrder += opLatency.Count;
                if(mergedTpccRunResults.Latencies.size() <= i) {
                    sumLat = mergedTpccRunResults.new LatencyList();
                    sumLat.Transaction = opLatency.Transaction;
                    sumLat.minLatency = opLatency.avgLatency;
                    sumLat.maxLatency = opLatency.avgLatency;
                    sumLat.P99Latency = opLatency.P99Latency;
                    sumLat.minConnAcqLatency = opLatency.connectionAcqLatency;
                    sumLat.maxConnAcqLatency = opLatency.connectionAcqLatency;
                    mergedTpccRunResults.Latencies.add(sumLat);
                } else {
                    sumLat = mergedTpccRunResults.Latencies.get(i);
                    sumLat.minLatency = sumLat.minLatency > opLatency.avgLatency ?
                            opLatency.avgLatency : sumLat.minLatency;
                    sumLat.maxLatency = sumLat.maxLatency < opLatency.avgLatency ?
                            opLatency.avgLatency : sumLat.maxLatency;
                    sumLat.P99Latency = sumLat.P99Latency > opLatency.P99Latency ?
                            opLatency.P99Latency : sumLat.P99Latency;
                    sumLat.minConnAcqLatency = sumLat.minConnAcqLatency > opLatency.connectionAcqLatency ?
                            opLatency.connectionAcqLatency : sumLat.minConnAcqLatency;
                    sumLat.maxConnAcqLatency = sumLat.maxConnAcqLatency < opLatency.connectionAcqLatency ?
                            opLatency.connectionAcqLatency : sumLat.maxConnAcqLatency;
                }
                sumLat.Count +=  opLatency.Count;
                sumLat.avgLatency = computeAverage(sumLat.avgLatency, opLatency.avgLatency, fileCnt);
                sumLat.connectionAcqLatency = computeAverage(sumLat.connectionAcqLatency,
                        opLatency.connectionAcqLatency, fileCnt);
                mergedTpccRunResults.Latencies.set(i, sumLat);
            }

            List<TpccRunResults.LatencyList> failureLatList = tpcc_metrics.FailureLatencies;
            for (int i = 0; i < failureLatList.size() ; i++) {
                TpccRunResults.LatencyList opLatency = failureLatList.get(i);
                TpccRunResults.LatencyList sumFailureLat;
                if (mergedTpccRunResults.FailureLatencies.size() <= i) {
                    sumFailureLat = mergedTpccRunResults.new LatencyList();
                    sumFailureLat.Transaction = opLatency.Transaction;
                    sumFailureLat.minLatency = opLatency.avgLatency;
                    sumFailureLat.maxLatency = opLatency.avgLatency;
                    sumFailureLat.P99Latency = opLatency.P99Latency;
                    sumFailureLat.minConnAcqLatency = opLatency.connectionAcqLatency;
                    sumFailureLat.maxConnAcqLatency = opLatency.connectionAcqLatency;
                    mergedTpccRunResults.FailureLatencies.add(sumFailureLat);
                } else {
                    sumFailureLat = mergedTpccRunResults.FailureLatencies.get(i);
                    sumFailureLat.minLatency = sumFailureLat.minLatency > opLatency.avgLatency ?
                            opLatency.avgLatency : sumFailureLat.minLatency;
                    sumFailureLat.maxLatency = sumFailureLat.maxLatency < opLatency.avgLatency ?
                            opLatency.avgLatency : sumFailureLat.maxLatency;
                    sumFailureLat.P99Latency = sumFailureLat.P99Latency > opLatency.P99Latency ?
                            opLatency.P99Latency : sumFailureLat.P99Latency;
                    sumFailureLat.minConnAcqLatency = sumFailureLat.minConnAcqLatency > opLatency.connectionAcqLatency ?
                            opLatency.connectionAcqLatency : sumFailureLat.minConnAcqLatency;
                    sumFailureLat.maxConnAcqLatency = sumFailureLat.maxConnAcqLatency < opLatency.connectionAcqLatency ?
                            opLatency.connectionAcqLatency : sumFailureLat.maxConnAcqLatency;
                }
                sumFailureLat.Count += opLatency.Count;
                sumFailureLat.avgLatency = computeAverage(sumFailureLat.avgLatency, opLatency.avgLatency, fileCnt);
                sumFailureLat.connectionAcqLatency = computeAverage(sumFailureLat.connectionAcqLatency,
                        opLatency.connectionAcqLatency, fileCnt);
                mergedTpccRunResults.FailureLatencies.set(i, sumFailureLat);
            }
            fileCnt++;
        }

        tpccRunResults = mergedTpccRunResults;

        double tpmc = 1.0 * numNewOrder * 60 / tpccRunResults.TestConfiguration.runTimeInSecs;
        tpccRunResults.Results.efficiency = 1.0 * tpmc * 100 / tpccRunResults.TestConfiguration.numWarehouses / 12.86;
        tpccRunResults.Results.tpmc = tpmc;

        writeMetricsToJSONFile();
    }

    public static void main(String args[]) {
        new JsonMetricsHelper().mergeJsonResults(args[0]);
    }
}
