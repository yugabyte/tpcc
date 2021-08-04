package com.oltpbenchmark.benchmarks.tpcc;

import com.google.gson.JsonParser;
import com.oltpbenchmark.benchmarks.tpcc.pojo.TPCCJsonMetrics;
import com.oltpbenchmark.util.FileUtil;
import org.apache.log4j.Logger;

import com.google.gson.*;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.oltpbenchmark.util.ComputeUtil;


import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class JsonMetricsBuilder {

    private static final Logger LOG = Logger.getLogger(JsonMetricsBuilder.class);
    public JsonObject jsonObject;
    public TPCCJsonMetrics tpccJsonMetrics;

    List <TPCCJsonMetrics.LatencyList> workerTaskLatList;


    public JsonMetricsBuilder() {
        jsonObject = new JsonObject();
        tpccJsonMetrics = new TPCCJsonMetrics();
    }

    public void buildTestConfigJson(int numNodes, int warehouses, int numDBConn, int warmuptime, int runtime, int numRetries) {
        if(tpccJsonMetrics.TestConfiguration == null)
            tpccJsonMetrics.TestConfiguration = tpccJsonMetrics.new TestConfigurationObject();
        tpccJsonMetrics.TestConfiguration.Num_Nodes = numNodes;
        tpccJsonMetrics.TestConfiguration.Num_Warehouses = warehouses;
        tpccJsonMetrics.TestConfiguration.Num_DBConnections = numDBConn;
        tpccJsonMetrics.TestConfiguration.WarmupTime_secs = warmuptime;
        tpccJsonMetrics.TestConfiguration.RunTime_secs = runtime;
        tpccJsonMetrics.TestConfiguration.Num_Retries = numRetries;
        tpccJsonMetrics.TestConfiguration.TestStartTime = new SimpleDateFormat("dd-MM-yy_HHmm").format(new Date());
    }

    public void buildResultJson(String tpmc, String efficiency, String throughput) {
        if(tpccJsonMetrics.Results == null)
            tpccJsonMetrics.Results = tpccJsonMetrics.new ResultObject();
        tpccJsonMetrics.Results.TPMC = tpmc;
        tpccJsonMetrics.Results.Efficiency = efficiency;
        tpccJsonMetrics.Results.Throughput = throughput;
    }

    public void addLatencyJson(String op, List<Integer> latencyList, List<Integer> connLatencyList) {
        if (tpccJsonMetrics.Latencies == null)
            tpccJsonMetrics.Latencies = new ArrayList<>();
        tpccJsonMetrics.Latencies.add(getValueList(op, latencyList, connLatencyList));
    }

    public void addFailureLatencyJson(String op, List<Integer> latencyList, List<Integer> connLatencyList) {
        if (tpccJsonMetrics.Failure_Latencies == null)
            tpccJsonMetrics.Failure_Latencies = new ArrayList<>();
        tpccJsonMetrics.Failure_Latencies.add(getValueList(op, latencyList, connLatencyList));
    }

    public void buildWorkerTaskLatJson(String op) {
        if (tpccJsonMetrics.Worker_Task_Latency == null)
            tpccJsonMetrics.Worker_Task_Latency = new HashMap<>();
        tpccJsonMetrics.Worker_Task_Latency.put(op,workerTaskLatList);
        workerTaskLatList = null;
    }

    public void addWorkerTaskLatencyJson(String task, List<Integer> latencyList) {
        if (workerTaskLatList == null)
            workerTaskLatList = new ArrayList<>();
        workerTaskLatList.add(getValueList(task, latencyList, null));
    }

    public void addRetryJson( String op, int count, int numTriesPerProc, List<String> retryOpList) {
        if(tpccJsonMetrics.Retry_Attempts == null)
            tpccJsonMetrics.Retry_Attempts = new ArrayList<>();
        TPCCJsonMetrics.RetryAttemptsObject retryObj = tpccJsonMetrics.new RetryAttemptsObject();
        retryObj.Transaction = op;
        retryObj.Count = String.valueOf(count);
        retryObj.Retries = retryOpList;
        tpccJsonMetrics.Retry_Attempts.add(retryObj);
    }

    private TPCCJsonMetrics.LatencyList getValueList(String op, List<Integer> latencyList, List<Integer> connAcqLatencyList) {
        DecimalFormat df = new DecimalFormat();
        df.setMaximumFractionDigits(2);
        df.setGroupingUsed(false);
        TPCCJsonMetrics.LatencyList valueList = tpccJsonMetrics.new LatencyList();
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
            LOG.error("Exception occurred fetching current dir" +
                    "\nError Message:" + e.getMessage());
        }
        String dest = currentDir + File.separator + outputDirectory + File.separator + "output.json";
        // + "json_" + numWarehouses + "WH_" + numDBConnections + "Conn_"
        // + new SimpleDateFormat("dd-MM-yy_HHmm").format(new Date()) + "_"
        // + UUID.randomUUID() + ".json";

        String jsonString = new GsonBuilder().setPrettyPrinting().create().toJson(jsonObject);
        try {
            FileWriter file = new FileWriter(dest);
            file.write(jsonString);
            file.close();
        } catch (IOException e) {
            LOG.error("Got exception while writing JSON metrics to file.");
            e.printStackTrace();
            return;
        }
        LOG.info("Output Raw data into file: " + dest);
    }

    public static void mergeJsonResults(String dirPath, String[] fileNames) {
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
                jsonStrings.add(new String(Files.readAllBytes(Paths.get(file))));
            } catch (IOException ie) {
                LOG.error("Got exception while reading file", ie);
                return;
            }
        }
        LOG.info("Printing JSON info.. ");

        for (String jsonStr : jsonStrings) {
            TPCCJsonMetrics jsonObj = new Gson().fromJson(jsonStr, TPCCJsonMetrics.class);
            LOG.info("Efficiency  : " + jsonObj.Results.Efficiency);
            LOG.info("Latency : " + jsonObj.Latencies.size() );
            LOG.info("Failure Latency : " + jsonObj.Failure_Latencies.size());
            LOG.info("WorkerTaskLatencies : " + jsonObj.Worker_Task_Latency.size());
            LOG.info("RetryAttempts : " + jsonObj.Retry_Attempts.size());

            /*JsonObject jsonObj = (JsonObject) new JsonParser().parse(jsonStr);

            JsonObject resultJson = (JsonObject) jsonObj.get("Results");
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
      buildResultJson(String.valueOf(tpmc),String.valueOf(efficiency),String.valueOf(throughput));
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
