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
        tpccMetrics.Results.TPMC = tpmc;
        tpccMetrics.Results.Efficiency = efficiency;
        tpccMetrics.Results.Throughput = throughput;
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
        retryObj.Count = String.valueOf(count);
        retryObj.Retries_FailureCount = retryOpList;
        tpccMetrics.Retry_Attempts.add(retryObj);
    }

    private TPCC_Metrics.LatencyList getValueList(String op, List<Integer> latencyList, List<Integer> connAcqLatencyList) {
        DecimalFormat df = new DecimalFormat();
        df.setMaximumFractionDigits(2);
        df.setGroupingUsed(false);
        TPCC_Metrics.LatencyList valueList = tpccMetrics.new LatencyList();
        valueList.Transaction = op;
        valueList.Count = df.format(latencyList.size());
        valueList.Avg_Latency = df.format(ComputeUtil.getAverageLatency(latencyList));
        valueList.P99_Latency = df.format(ComputeUtil.getP99Latency(latencyList));
        if (connAcqLatencyList != null)
            valueList.Connection_Acq_Latency = df.format(ComputeUtil.getAverageLatency(connAcqLatencyList));
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

    public static void mergeJsonResults(String dirPath, String[] fileNames) {
        List<TPCC_Metrics> tpcc_metrics_list = new ArrayList<>();
        TPCC_Metrics tpccMetrics_avg =  new TPCC_Metrics();
        TPCC_Metrics tpccMetrics_sum = new TPCC_Metrics();
        Gson gson = new Gson();

        double tpmc, efficiency, throughput;
        double sumEfficiency = 0, sum_tpmc = 0, sumThroughput = 0;

        double[] newOrderLat = {0, 0, 0}, newOrderFailLat, newOrderRetry;
        double[] paymentLat, paymentFailLat, paymentRetry;
        double[] orderStatusLat, orderStatusFailLat, orderStatusRetry;
        double[] deliveryLat, deliveryFailLat, deliveryRetry;
        double[] stockLevelLat, stockLevelFailLat, stockLevelRetry;

        double[] sum_newOrderLat = {0, 0, 0, 0}, sum_newOrderFailLat = {0, 0, 0, 0};
        double[] sum_paymentLat = {0, 0, 0, 0}, sum_paymentFailLat = {0, 0, 0, 0};
        double[] sum_orderStatusLat = {0, 0, 0, 0}, sum_orderStatusFailLat = {0, 0, 0, 0};
        double[] sum_deliveryLat = {0, 0, 0, 0}, sum_deliveryFailLat = {0, 0, 0, 0};
        double[] sum_stockLevelLat = {0, 0, 0, 0}, sum_stockLevelFailLat = {0, 0, 0, 0};
        double[] sum_AllLat = {0, 0, 0, 0}, sum_AllFailLat = {0, 0, 0, 0};

        List<String> jsonStrings = new ArrayList<>();
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
        LOG.info("Printing JSON info.. ");

        for (TPCC_Metrics tpccMetrics : tpcc_metrics_list) {
            LOG.info("Efficiency  : " + tpccMetrics.Results.Efficiency);
            LOG.info("Latency : " + tpccMetrics.Latencies.size() );
            LOG.info("Failure Latency : " + tpccMetrics.Failure_Latencies.size());
            LOG.info("WorkerTaskLatencies : " + tpccMetrics.Worker_Task_Latency.size());
            LOG.info("RetryAttempts : " + tpccMetrics.Retry_Attempts.size());

            tpccMetrics_sum.Results.Efficiency += tpccMetrics.Results.Efficiency;
            /*

            sum_tpmc += Double.parseDouble(resultJson.get("TPM-C").getAsString());
            sumEfficiency += Double.parseDouble(resultJson.get("Efficiency").getAsString());
            sumThroughput += Double.parseDouble(resultJson.get("Throughput").getAsString());

            JsonArray latJsonArr = (JsonArray) jsonObj.get("Latencies");
            JsonArray failLatJsonArr = (JsonArray) jsonObj.get("Failure Latencies");
            JsonArray retryLatJsonArr = (JsonArray) jsonObj.get("Retry Attempts");
            JsonObject workerTaskAggJson = (JsonObject) jsonObj.get("Work Task Latencies");

            for (int i = 0; i < latJsonArr.size(); i++) {
                JsonObject jObj = (JsonObject) latJsonArr.get(i);
                switch (jObj.get("Transaction").getAsString()) {
                    case "NewOrder":
                        sum_newOrderLat[0] += Double.parseDouble(jObj.get("Count").getAsString());
                        sum_newOrderLat[1] += Double.parseDouble(jObj.get("Avg_Latency").getAsString());
                        sum_newOrderLat[2] += Double.parseDouble(jObj.get("P99_Latency").getAsString());
                        sum_newOrderLat[3] += Double.parseDouble(jObj.get("Connection_Acq_Latency").getAsString());
                        break;
                    case "Payment":
                        sum_paymentLat[0] += Double.parseDouble(jObj.get("Count").getAsString());
                        sum_paymentLat[1] += Double.parseDouble(jObj.get("Avg_Latency").getAsString());
                        sum_paymentLat[2] += Double.parseDouble(jObj.get("P99_Latency").getAsString());
                        sum_paymentLat[3] += Double.parseDouble(jObj.get("Connection_Acq_Latency").getAsString());
                        break;
                    case "OrderStatus":
                        sum_orderStatusLat[0] += Double.parseDouble(jObj.get("Count").getAsString());
                        sum_orderStatusLat[1] += Double.parseDouble(jObj.get("Avg. Latency").getAsString());
                        sum_orderStatusLat[2] += Double.parseDouble(jObj.get("P99Latency").getAsString());
                        sum_orderStatusLat[3] += Double.parseDouble(jObj.get("Connection Acq Latency").getAsString());
                        break;
                    case "Delivery":
                        sum_deliveryLat[0] += Double.parseDouble(jObj.get("Count").getAsString());
                        sum_deliveryLat[1] += Double.parseDouble(jObj.get("Avg_Latency").getAsString());
                        sum_deliveryLat[2] += Double.parseDouble(jObj.get("P99_Latency").getAsString());
                        sum_deliveryLat[3] += Double.parseDouble(jObj.get("Connection_Acq_Latency").getAsString());
                        break;
                    case "StockLevel":
                        sum_stockLevelLat[0] += Double.parseDouble(jObj.get("Count").getAsString());
                        sum_stockLevelLat[1] += Double.parseDouble(jObj.get("Avg_Latency").getAsString());
                        sum_stockLevelLat[2] += Double.parseDouble(jObj.get("P99_Latency").getAsString());
                        sum_stockLevelLat[3] += Double.parseDouble(jObj.get("Connection_Acq_Latency").getAsString());
                        break;
                    case "All":
                        sum_AllLat[0] += Double.parseDouble(jObj.get("Count").getAsString());
                        sum_AllLat[1] += Double.parseDouble(jObj.get("Avg_Latency").getAsString());
                        sum_AllLat[2] += Double.parseDouble(jObj.get("P99_Latency").getAsString());
                        sum_AllLat[3] += Double.parseDouble(jObj.get("Connection_Acq_Latency").getAsString());
                        break;
                }
            }

            for (int i = 0; i < failLatJsonArr.size(); i++) {
                JsonObject jObj = (JsonObject) failLatJsonArr.get(i);
                switch (jObj.get("Transaction").getAsString()) {
                    case "NewOrder":
                        sum_newOrderFailLat[0] += Double.parseDouble(jObj.get("Count").getAsString());
                        sum_newOrderFailLat[1] += Double.parseDouble(jObj.get("Avg. Latency").getAsString());
                        sum_newOrderFailLat[2] += Double.parseDouble(jObj.get("P99Latency").getAsString());
                        sum_newOrderFailLat[3] += Double.parseDouble(jObj.get("Connection Acq Latency").getAsString());
                        break;
                    case "Payment":
                        sum_paymentFailLat[0] += Double.parseDouble(jObj.get("Count").getAsString());
                        sum_paymentFailLat[1] += Double.parseDouble(jObj.get("Avg. Latency").getAsString());
                        sum_paymentFailLat[2] += Double.parseDouble(jObj.get("P99Latency").getAsString());
                        sum_paymentFailLat[3] += Double.parseDouble(jObj.get("Connection Acq Latency").getAsString());
                        break;
                    case "OrderStatus":
                        sum_orderStatusFailLat[0] += Double.parseDouble(jObj.get("Count").getAsString());
                        sum_orderStatusFailLat[1] += Double.parseDouble(jObj.get("Avg. Latency").getAsString());
                        sum_orderStatusFailLat[2] += Double.parseDouble(jObj.get("P99Latency").getAsString());
                        sum_orderStatusFailLat[3] += Double.parseDouble(jObj.get("Connection Acq Latency").getAsString());
                        break;
                    case "Delivery":
                        sum_deliveryFailLat[0] += Double.parseDouble(jObj.get("Count").getAsString());
                        sum_deliveryFailLat[1] += Double.parseDouble(jObj.get("Avg. Latency").getAsString());
                        sum_deliveryFailLat[2] += Double.parseDouble(jObj.get("P99Latency").getAsString());
                        sum_deliveryFailLat[3] += Double.parseDouble(jObj.get("Connection Acq Latency").getAsString());
                        break;
                    case "StockLevel":
                        sum_stockLevelFailLat[0] += Double.parseDouble(jObj.get("Count").getAsString());
                        sum_stockLevelFailLat[1] += Double.parseDouble(jObj.get("Avg. Latency").getAsString());
                        sum_stockLevelFailLat[2] += Double.parseDouble(jObj.get("P99Latency").getAsString());
                        sum_stockLevelFailLat[3] += Double.parseDouble(jObj.get("Connection Acq Latency").getAsString());
                        break;
                    case "All":
                        sum_AllFailLat[0] += Double.parseDouble(jObj.get("Count").getAsString());
                        sum_AllFailLat[1] += Double.parseDouble(jObj.get("Avg. Latency").getAsString());
                        sum_AllFailLat[2] += Double.parseDouble(jObj.get("P99Latency").getAsString());
                        sum_AllFailLat[3] += Double.parseDouble(jObj.get("Connection Acq Latency").getAsString());
                        break;
                }
            }

         /*
          LOG.info("Results : " + resultJson);
          LOG.info("Latencies : " + latJsonArr);
          LOG.info("Failure Latencies" + failLatJsonArr);
          LOG.info("Retry Attempts : " + retryLatJsonArr);
          LOG.info("Work Task Latencies" + workerTaskAggJson);
          */
        }

        LOG.info("Aggregate Results : ");
        efficiency = sumEfficiency / jsonStrings.size();
        tpmc = sum_tpmc / jsonStrings.size();
        throughput = sumThroughput / jsonStrings.size();
        LOG.info("Agg Efficiency : " + efficiency);
        LOG.info("Agg TPM-C : " + tpmc);
        LOG.info("Agg Throughput : " + throughput);

        LOG.info("Aggregate Latencies : ");

      /*
      jsonObject = new JsonObject();
      buildTestResults(String.valueOf(tpmc),String.valueOf(efficiency),String.valueOf(throughput));
      buildLatencyJson(latencyList);
      buildFailureLatencyJson(failureLatencyList);
      buildWorkerTaskLatencyJson(workerLatencyMap);
      buildQueryAttemptsJson(numRetries,retryOpList);
      writeMetricsToJSONFile();
      */
    }

    public static void main(String args[]) {
        mergeJsonResults("results_dir",new String[]{"results_dir/try.json", "results_dir/try1.json"});
    }
}
