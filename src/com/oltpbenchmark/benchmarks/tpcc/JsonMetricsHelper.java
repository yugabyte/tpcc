package com.oltpbenchmark.benchmarks.tpcc;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.oltpbenchmark.benchmarks.tpcc.pojo.TPCC_Metrics;
import com.oltpbenchmark.util.ComputeUtil;
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
    public TPCC_Metrics tpccMetrics;

    List <TPCC_Metrics.LatencyList> workerTaskLatList;

    public JsonMetricsHelper() {
        tpccMetrics = new TPCC_Metrics();
    }

    public void buildTestConfig(int numNodes, int warehouses, int numDBConn, int warmuptime, int runtime, int numRetries) {
        if(tpccMetrics.TestConfiguration == null)
            tpccMetrics.TestConfiguration = tpccMetrics.new TestConfigurationObject();
        tpccMetrics.TestConfiguration.Num_Nodes = numNodes;
        tpccMetrics.TestConfiguration.Num_Warehouses = warehouses;
        tpccMetrics.TestConfiguration.Num_DBConnections = numDBConn;
        tpccMetrics.TestConfiguration.WarmupTime_secs = warmuptime;
        tpccMetrics.TestConfiguration.RunTime_secs = runtime;
        tpccMetrics.TestConfiguration.Num_Retries = numRetries;
        tpccMetrics.TestConfiguration.TestStartTime = new SimpleDateFormat("dd-MM-yy_HH:mm:ss").format(new Date());
    }

    public void buildTestResults(String tpmc, String efficiency, String throughput) {
        if(tpccMetrics.Results == null)
            tpccMetrics.Results = tpccMetrics.new ResultObject();
        tpccMetrics.Results.TPMC = Double.parseDouble(tpmc);
        tpccMetrics.Results.Efficiency = Double.parseDouble(efficiency);
        tpccMetrics.Results.Throughput = Double.parseDouble(throughput);
    }

    public void addLatency(String op, List<Integer> latencyList, List<Integer> connLatencyList) {
        if (tpccMetrics.Latencies == null)
            tpccMetrics.Latencies = new ArrayList<>();
        tpccMetrics.Latencies.add(getValueList(op, latencyList, connLatencyList));
    }

    public void addFailureLatency(String op, List<Integer> latencyList, List<Integer> connLatencyList) {
        if (tpccMetrics.Failure_Latencies == null)
            tpccMetrics.Failure_Latencies = new ArrayList<>();
        tpccMetrics.Failure_Latencies.add(getValueList(op, latencyList, connLatencyList));
    }

    public void buildWorkerTaskLat(String op) {
        if (tpccMetrics.Worker_Task_Latency == null)
            tpccMetrics.Worker_Task_Latency = new LinkedHashMap<>();
        tpccMetrics.Worker_Task_Latency.put(op,workerTaskLatList);
        workerTaskLatList = null;
    }

    public void addWorkerTaskLatency(String task, List<Integer> latencyList) {
        if (workerTaskLatList == null)
            workerTaskLatList = new ArrayList<>();
        workerTaskLatList.add(getValueList(task, latencyList, null));
    }

    public void addRetry(String op, int count, List<String> retryOpList) {
        if(tpccMetrics.Retry_Attempts == null)
            tpccMetrics.Retry_Attempts = new ArrayList<>();
        TPCC_Metrics.RetryAttemptsObject retryObj = tpccMetrics.new RetryAttemptsObject();
        retryObj.Transaction = op;
        retryObj.Count = count;
        retryObj.Retries_FailureCount = retryOpList;
        tpccMetrics.Retry_Attempts.add(retryObj);
    }

    private TPCC_Metrics.LatencyList getValueList(String op, List<Integer> latencyList, List<Integer> connAcqLatencyList) {
        DecimalFormat df = new DecimalFormat();
        df.setMaximumFractionDigits(2);
        df.setGroupingUsed(false);
        TPCC_Metrics.LatencyList valueList = tpccMetrics.new LatencyList();
        valueList.Transaction = op;
        valueList.Count = latencyList.size();
        valueList.Avg_Latency = Double.parseDouble(df.format(ComputeUtil.getAverageLatency(latencyList)));
        valueList.P99_Latency = Double.parseDouble(df.format(ComputeUtil.getP99Latency(latencyList)));
        if (connAcqLatencyList != null)
            valueList.Connection_Acq_Latency = Double.parseDouble(df.format(ComputeUtil.getAverageLatency(connAcqLatencyList)));
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
        LOG.info("Output Raw data into file: " + dest);
    }

    private double computeAverage(double old_value, double newValue, int count) {
        return ((old_value * count) + newValue) / (count + 1);
    }

    private int computeAverage(int old_value, int newValue, int count) {
        return ((old_value * count) + newValue) / (count + 1);
    }

    public void mergeJsonResults(String dirPath) {
        List<TPCC_Metrics> tpcc_metrics_list = new ArrayList<>();
        String[] fileNames = new File(dirPath).list();
        TPCC_Metrics mergedTPCC_Metrics = new TPCC_Metrics();
        Gson gson = new Gson();
        for (String file : fileNames) {
            if (!file.endsWith("json")) {
                continue;
            }
            try {
                tpcc_metrics_list.add(
                        gson.fromJson(new String(Files.readAllBytes(Paths.get(file))), TPCC_Metrics.class));
            } catch (IOException ie) {
                LOG.error("Got exception while reading json file - " + file + " : ", ie);
                return;
            }
        }
        int numNewOrder = 0;
        LOG.info("Printing JSON info.. ");
        int fileCnt = 0;
        for (TPCC_Metrics tpcc_metrics : tpcc_metrics_list) {
            tpccMetrics.TestConfiguration = tpcc_metrics.TestConfiguration;
            LOG.info("Efficiency  : " + tpcc_metrics.Results.Efficiency);
            LOG.info("TPMC  : " + tpcc_metrics.Results.TPMC);
            LOG.info("Throughput  : " + tpcc_metrics.Results.Throughput);
            if(fileCnt == 0) {
                System.out.println("Inside If...");
                mergedTPCC_Metrics.Results.Throughput_min = tpcc_metrics.Results.Throughput;
                mergedTPCC_Metrics.Results.Throughput_max = tpcc_metrics.Results.Throughput;
            } else {
                if (mergedTPCC_Metrics.Results.Throughput_min > tpcc_metrics.Results.Throughput)
                    mergedTPCC_Metrics.Results.Throughput_min = tpcc_metrics.Results.Throughput;
                if (mergedTPCC_Metrics.Results.Throughput_max < tpcc_metrics.Results.Throughput)
                    mergedTPCC_Metrics.Results.Throughput_max = tpcc_metrics.Results.Throughput;
            }
            mergedTPCC_Metrics.Results.Throughput = computeAverage(mergedTPCC_Metrics.Results.Throughput, tpcc_metrics.Results.Throughput, fileCnt);

            List<TPCC_Metrics.LatencyList> latList = tpcc_metrics.Latencies;
            for (int i = 0; i < latList.size() ; i++) {
                TPCC_Metrics.LatencyList opLatency = latList.get(i);
                TPCC_Metrics.LatencyList sumLat;
                if(opLatency.Transaction.equalsIgnoreCase("NewOrder"))
                    numNewOrder += opLatency.Count;
                if(mergedTPCC_Metrics.Latencies.size() <= i) {
                    sumLat = mergedTPCC_Metrics.new LatencyList();
                    sumLat.Transaction = opLatency.Transaction;
                    sumLat.min_Latency = opLatency.Avg_Latency;
                    sumLat.max_Latency = opLatency.Avg_Latency;
                    sumLat.P99_Latency = opLatency.P99_Latency;
                    sumLat.min_Conn_Acq_Latency = opLatency.Connection_Acq_Latency;
                    sumLat.max_Conn_Acq_Latency = opLatency.Connection_Acq_Latency;
                    mergedTPCC_Metrics.Latencies.add(sumLat);
                } else {
                    sumLat = mergedTPCC_Metrics.Latencies.get(i);
                    sumLat.min_Latency = sumLat.min_Latency > opLatency.Avg_Latency ? opLatency.Avg_Latency : sumLat.min_Latency;
                    sumLat.max_Latency = sumLat.max_Latency < opLatency.Avg_Latency ? opLatency.Avg_Latency : sumLat.max_Latency;
                    sumLat.P99_Latency = sumLat.P99_Latency > opLatency.P99_Latency ? opLatency.P99_Latency : sumLat.P99_Latency;
                    sumLat.min_Conn_Acq_Latency = sumLat.min_Conn_Acq_Latency > opLatency.Connection_Acq_Latency ? opLatency.Connection_Acq_Latency : sumLat.min_Conn_Acq_Latency;
                    sumLat.max_Conn_Acq_Latency = sumLat.max_Conn_Acq_Latency < opLatency.Connection_Acq_Latency ? opLatency.Connection_Acq_Latency : sumLat.max_Conn_Acq_Latency;
                }
                sumLat.Count +=  opLatency.Count;
                sumLat.Avg_Latency = computeAverage(sumLat.Avg_Latency, opLatency.Avg_Latency, fileCnt);
                sumLat.Connection_Acq_Latency = computeAverage(sumLat.Connection_Acq_Latency, opLatency.Connection_Acq_Latency, fileCnt);
                mergedTPCC_Metrics.Latencies.set(i, sumLat);
            }

            List<TPCC_Metrics.LatencyList> failureLatList = tpcc_metrics.Failure_Latencies;
            for (int i = 0; i < failureLatList.size() ; i++) {
                TPCC_Metrics.LatencyList opLatency = failureLatList.get(i);
                TPCC_Metrics.LatencyList sumFailureLat = mergedTPCC_Metrics.new LatencyList();
                if (mergedTPCC_Metrics.Failure_Latencies.size() <= i) {
                    sumFailureLat = mergedTPCC_Metrics.new LatencyList();
                    sumFailureLat.Transaction = opLatency.Transaction;
                    sumFailureLat.min_Latency = opLatency.Avg_Latency;
                    sumFailureLat.max_Latency = opLatency.Avg_Latency;
                    sumFailureLat.P99_Latency = opLatency.P99_Latency;
                    sumFailureLat.min_Conn_Acq_Latency = opLatency.Connection_Acq_Latency;
                    sumFailureLat.max_Conn_Acq_Latency = opLatency.Connection_Acq_Latency;
                    mergedTPCC_Metrics.Failure_Latencies.add(sumFailureLat);
                } else {
                    sumFailureLat = mergedTPCC_Metrics.Failure_Latencies.get(i);
                    sumFailureLat.min_Latency = sumFailureLat.min_Latency > opLatency.Avg_Latency ? opLatency.Avg_Latency : sumFailureLat.min_Latency;
                    sumFailureLat.max_Latency = sumFailureLat.max_Latency < opLatency.Avg_Latency ? opLatency.Avg_Latency : sumFailureLat.max_Latency;
                    sumFailureLat.P99_Latency = sumFailureLat.P99_Latency > opLatency.P99_Latency ? opLatency.P99_Latency : sumFailureLat.P99_Latency;
                    sumFailureLat.min_Conn_Acq_Latency = sumFailureLat.min_Conn_Acq_Latency > opLatency.Connection_Acq_Latency ? opLatency.Connection_Acq_Latency : sumFailureLat.min_Conn_Acq_Latency;
                    sumFailureLat.max_Conn_Acq_Latency = sumFailureLat.max_Conn_Acq_Latency < opLatency.Connection_Acq_Latency ? opLatency.Connection_Acq_Latency : sumFailureLat.max_Conn_Acq_Latency;
                }
                sumFailureLat.Count += opLatency.Count;
                sumFailureLat.Avg_Latency = computeAverage(sumFailureLat.Avg_Latency, opLatency.Avg_Latency, fileCnt);
                sumFailureLat.Connection_Acq_Latency = computeAverage(sumFailureLat.Connection_Acq_Latency, opLatency.Connection_Acq_Latency, fileCnt);
                mergedTPCC_Metrics.Failure_Latencies.set(i, sumFailureLat);
            }


            fileCnt++;
        }

        tpccMetrics.Results = mergedTPCC_Metrics.Results;
        double tpmc = 1.0 * numNewOrder * 60 / tpccMetrics.TestConfiguration.RunTime_secs;
        tpccMetrics.Results.Efficiency = 1.0 * tpmc * 100 / tpccMetrics.TestConfiguration.Num_Warehouses / 12.86;
        tpccMetrics.Results.TPMC = tpmc;

        tpccMetrics.Latencies = mergedTPCC_Metrics.Latencies;
        tpccMetrics.Failure_Latencies = mergedTPCC_Metrics.Failure_Latencies;

        writeMetricsToJSONFile();
    }

    public static void main(String args[]) {
        new JsonMetricsHelper().mergeJsonResults("results_dir");
    }
}
