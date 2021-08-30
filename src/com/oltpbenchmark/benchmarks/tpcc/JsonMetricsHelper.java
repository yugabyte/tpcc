package com.oltpbenchmark.benchmarks.tpcc;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.oltpbenchmark.benchmarks.tpcc.pojo.TPCCMetrics;
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
    public TPCCMetrics tpccMetrics;

    List <TPCCMetrics.LatencyList> workerTaskLatList;

    public JsonMetricsHelper() {
        tpccMetrics = new TPCCMetrics();
    }

    public void buildTestConfig(int numNodes, int numWH, int numDBConn, int warmuptime, int runtime, int numRetries) {
        if(tpccMetrics.TestConfiguration == null)
            tpccMetrics.TestConfiguration = tpccMetrics.new TestConfigurationObject();
        tpccMetrics.TestConfiguration.numNodes = numNodes;
        tpccMetrics.TestConfiguration.numWarehouses = numWH;
        tpccMetrics.TestConfiguration.numDBConnections = numDBConn;
        tpccMetrics.TestConfiguration.warmupTimeInSecs = warmuptime;
        tpccMetrics.TestConfiguration.runTimeInSecs = runtime;
        tpccMetrics.TestConfiguration.numRetries = numRetries;
        tpccMetrics.TestConfiguration.testStartTime = new SimpleDateFormat("dd-MM-yy_HH:mm:ss").format(new Date());
    }

    public void buildTestResults(String tpmc, String efficiency, String throughput) {
        if(tpccMetrics.Results == null)
            tpccMetrics.Results = tpccMetrics.new ResultObject();
        tpccMetrics.Results.tpmc = Double.parseDouble(tpmc);
        tpccMetrics.Results.efficiency = Double.parseDouble(efficiency);
        tpccMetrics.Results.throughput = Double.parseDouble(throughput);
    }

    public void addLatency(String op, List<Integer> latencyList, List<Integer> connLatencyList) {
        if (tpccMetrics.Latencies == null)
            tpccMetrics.Latencies = new ArrayList<>();
        tpccMetrics.Latencies.add(getValueList(op, latencyList, connLatencyList));
    }

    public void addFailureLatency(String op, List<Integer> latencyList, List<Integer> connLatencyList) {
        if (tpccMetrics.FailureLatencies == null)
            tpccMetrics.FailureLatencies = new ArrayList<>();
        tpccMetrics.FailureLatencies.add(getValueList(op, latencyList, connLatencyList));
    }

    public void buildWorkerTaskLat(String op) {
        if (tpccMetrics.WorkerTaskLatency == null)
            tpccMetrics.WorkerTaskLatency = new LinkedHashMap<>();
        tpccMetrics.WorkerTaskLatency.put(op,workerTaskLatList);
        workerTaskLatList = null;
    }

    public void addWorkerTaskLatency(String task, List<Integer> latencyList) {
        if (workerTaskLatList == null)
            workerTaskLatList = new ArrayList<>();
        workerTaskLatList.add(getValueList(task, latencyList, null));
    }

    public void addRetry(String op, int count,int[] retryOpList) {
        if(tpccMetrics.RetryAttempts == null)
            tpccMetrics.RetryAttempts = new ArrayList<>();
        TPCCMetrics.RetryAttemptsObject retryObj = tpccMetrics.new RetryAttemptsObject();
        retryObj.transaction = op;
        retryObj.count = count;
        retryObj.retriesFailureCount = Arrays.asList(retryOpList);
        tpccMetrics.RetryAttempts.add(retryObj);
    }

    private TPCCMetrics.LatencyList getValueList(String op, List<Integer> latencyList, List<Integer> connAcqLatencyList) {
        DecimalFormat df = new DecimalFormat();
        df.setMaximumFractionDigits(2);
        df.setGroupingUsed(false);
        TPCCMetrics.LatencyList valueList = tpccMetrics.new LatencyList();
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
            new GsonBuilder().setPrettyPrinting().create().toJson(tpccMetrics, fw);
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

    public void mergeJsonResults(String dirPath) {
        List<TPCCMetrics> listTpccMetrics = new ArrayList<>();
        String[] fileNames = new File(dirPath).list();
        TPCCMetrics mergedTPCC_Metrics = new TPCCMetrics();
        Gson gson = new Gson();
        for (String file : fileNames) {
            if (!file.endsWith("json")) {
                continue;
            }
            try {
                listTpccMetrics.add(
                        gson.fromJson(new String(Files.readAllBytes(Paths.get(file))), TPCCMetrics.class));
            } catch (IOException ie) {
                LOG.error("Got exception while reading json file - " + file + " : ", ie);
                return;
            }
        }
        int numNewOrder = 0;

        int fileCnt = 0;
        for (TPCCMetrics tpcc_metrics : listTpccMetrics) {
            tpccMetrics.TestConfiguration = tpcc_metrics.TestConfiguration;

            if(fileCnt == 0) {
                mergedTPCC_Metrics.Results.throughputMin = tpcc_metrics.Results.throughput;
                mergedTPCC_Metrics.Results.throughputMax = tpcc_metrics.Results.throughput;
            } else {
                if (mergedTPCC_Metrics.Results.throughputMin > tpcc_metrics.Results.throughput)
                    mergedTPCC_Metrics.Results.throughputMin = tpcc_metrics.Results.throughput;
                if (mergedTPCC_Metrics.Results.throughputMax < tpcc_metrics.Results.throughput)
                    mergedTPCC_Metrics.Results.throughputMax = tpcc_metrics.Results.throughput;
            }
            mergedTPCC_Metrics.Results.throughput = computeAverage(mergedTPCC_Metrics.Results.throughput,
                    tpcc_metrics.Results.throughput, fileCnt);

            List<TPCCMetrics.LatencyList> latList = tpcc_metrics.Latencies;
            for (int i = 0; i < latList.size() ; i++) {
                TPCCMetrics.LatencyList opLatency = latList.get(i);
                TPCCMetrics.LatencyList sumLat;
                if(opLatency.Transaction.equalsIgnoreCase("NewOrder"))
                    numNewOrder += opLatency.Count;
                if(mergedTPCC_Metrics.Latencies.size() <= i) {
                    sumLat = mergedTPCC_Metrics.new LatencyList();
                    sumLat.Transaction = opLatency.Transaction;
                    sumLat.minLatency = opLatency.avgLatency;
                    sumLat.maxLatency = opLatency.avgLatency;
                    sumLat.P99Latency = opLatency.P99Latency;
                    sumLat.minConnAcqLatency = opLatency.connectionAcqLatency;
                    sumLat.maxConnAcqLatency = opLatency.connectionAcqLatency;
                    mergedTPCC_Metrics.Latencies.add(sumLat);
                } else {
                    sumLat = mergedTPCC_Metrics.Latencies.get(i);
                    sumLat.minLatency = sumLat.minLatency > opLatency.avgLatency ? opLatency.avgLatency : sumLat.minLatency;
                    sumLat.maxLatency = sumLat.maxLatency < opLatency.avgLatency ? opLatency.avgLatency : sumLat.maxLatency;
                    sumLat.P99Latency = sumLat.P99Latency > opLatency.P99Latency ? opLatency.P99Latency : sumLat.P99Latency;
                    sumLat.minConnAcqLatency = sumLat.minConnAcqLatency > opLatency.connectionAcqLatency ?
                            opLatency.connectionAcqLatency : sumLat.minConnAcqLatency;
                    sumLat.maxConnAcqLatency = sumLat.maxConnAcqLatency < opLatency.connectionAcqLatency ?
                            opLatency.connectionAcqLatency : sumLat.maxConnAcqLatency;
                }
                sumLat.Count +=  opLatency.Count;
                sumLat.avgLatency = computeAverage(sumLat.avgLatency, opLatency.avgLatency, fileCnt);
                sumLat.connectionAcqLatency = computeAverage(sumLat.connectionAcqLatency,
                        opLatency.connectionAcqLatency, fileCnt);
                mergedTPCC_Metrics.Latencies.set(i, sumLat);
            }

            List<TPCCMetrics.LatencyList> failureLatList = tpcc_metrics.FailureLatencies;
            for (int i = 0; i < failureLatList.size() ; i++) {
                TPCCMetrics.LatencyList opLatency = failureLatList.get(i);
                TPCCMetrics.LatencyList sumFailureLat;
                if (mergedTPCC_Metrics.FailureLatencies.size() <= i) {
                    sumFailureLat = mergedTPCC_Metrics.new LatencyList();
                    sumFailureLat.Transaction = opLatency.Transaction;
                    sumFailureLat.minLatency = opLatency.avgLatency;
                    sumFailureLat.maxLatency = opLatency.avgLatency;
                    sumFailureLat.P99Latency = opLatency.P99Latency;
                    sumFailureLat.minConnAcqLatency = opLatency.connectionAcqLatency;
                    sumFailureLat.maxConnAcqLatency = opLatency.connectionAcqLatency;
                    mergedTPCC_Metrics.FailureLatencies.add(sumFailureLat);
                } else {
                    sumFailureLat = mergedTPCC_Metrics.FailureLatencies.get(i);
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
                mergedTPCC_Metrics.FailureLatencies.set(i, sumFailureLat);
            }
            fileCnt++;
        }

        tpccMetrics = mergedTPCC_Metrics;

        double tpmc = 1.0 * numNewOrder * 60 / tpccMetrics.TestConfiguration.runTimeInSecs;
        tpccMetrics.Results.efficiency = 1.0 * tpmc * 100 / tpccMetrics.TestConfiguration.numWarehouses / 12.86;
        tpccMetrics.Results.tpmc = tpmc;

        writeMetricsToJSONFile();
    }

    public static void main(String args[]) {
        new JsonMetricsHelper().mergeJsonResults(args[0]);
    }
}
