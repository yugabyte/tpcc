package com.oltpbenchmark.benchmarks.tpcc;

import com.google.gson.JsonParser;
import com.oltpbenchmark.util.FileUtil;
import org.apache.log4j.Logger;

import com.google.gson.GsonBuilder;
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
    private JsonArray latJsonArr;
    private JsonObject aggLatJsonObject;
    private JsonArray aggLatJsonArr;
    private JsonArray retryJsonArr;

    private int numWarehouses;
    private int numDBConnections;
    private int warmupTime;
    private int numNodes;

    private final List<String> latencyKeyList = new ArrayList<String>() {{
        add("Transaction");
        add("Count");
        add("Avg. Latency");
        add("P99Latency");
        add("Connection Acq Latency");
    }};

    private final List<String> workerTaskKeyList = new ArrayList<String>() {
        {
            add("Task");
            add("Count");
            add("Avg. Latency");
            add("P99Latency");
        }
    };

    List<String> retryKeyList;

    public JsonMetricsBuilder() {
        jsonObject = new JsonObject();
    }

    private JsonObject getJson(List<String> keyList, List<String> valueList) {
        JsonObject jsonObject = new JsonObject();
        for (int i = 0; i < keyList.size(); i++)
            jsonObject.addProperty(keyList.get(i), valueList.get(i));
        return jsonObject;
    }

    public void buildTestConfigJson(int numNodes, int warehouses, int numDBConn, int warmuptime, int runtime) {
        this.numWarehouses = warehouses;
        this.numDBConnections = numDBConn;
        this.warmupTime = warmuptime;
        this.numNodes = numNodes;

        JsonObject testConfigJson = new JsonObject();
        testConfigJson.addProperty("#Nodes", numNodes);
        testConfigJson.addProperty("#Warehouses", warehouses);
        testConfigJson.addProperty("#DBConnections", numDBConn);
        testConfigJson.addProperty("WarmupTime (secs)", warmuptime);
        testConfigJson.addProperty("RunTime (secs)", runtime);
        testConfigJson.addProperty("Test start time", new SimpleDateFormat("dd-MM-yy_HHmm").format(new Date()));
        jsonObject.add("Test Configuration", testConfigJson);
    }

    public void buildResultJson(String tpmc, String efficiency, String throughput) {
        JsonObject resultJson = new JsonObject();
        resultJson.addProperty("TPM-C", tpmc);
        resultJson.addProperty("Efficiency", efficiency);
        resultJson.addProperty("Throughput", throughput);
        jsonObject.add("Results", resultJson);
    }

    public void buildLatencyJson(String latType) {
        jsonObject.add(latType, latJsonArr);
        latJsonArr = null;
    }

    public void addLatencyJson(String op, List<Integer> latencyList, List<Integer> connLatencyList) {
        if (latJsonArr == null)
            latJsonArr = new JsonArray();
        latJsonArr.add(getJson(latencyKeyList, getValueList(op, latencyList, connLatencyList)));
    }

    public void buildWorkerTaskLatJson(String op) {
        if (op.equalsIgnoreCase("Worker Task Latency")) {
            jsonObject.add(op, aggLatJsonObject);
            aggLatJsonObject = null;
        } else {
            aggLatJsonObject.add(op, aggLatJsonArr);
            aggLatJsonArr = null;
        }

    }

    public void addWorkerTaskLatencyJson(String task, List<Integer> latencyList) {
        if (aggLatJsonObject == null)
            aggLatJsonObject = new JsonObject();
        if (aggLatJsonArr == null)
            aggLatJsonArr = new JsonArray();
        aggLatJsonArr.add(getJson(workerTaskKeyList, getValueList(task, latencyList, null)));
    }

    public void buildQueryAttemptsJson() {
        jsonObject.add("Retry Attempts", retryJsonArr);
    }

    public void addRetryJson(String op, int numTriesPerProc, List<String> retryOpList) {
        if (retryJsonArr == null) {
            retryKeyList = new ArrayList<String>() {
                {
                    add("Transaction");
                    add("Count");
                }
            };
            for (int j = 0; j < numTriesPerProc; ++j)
                retryKeyList.add(String.format("Retry #%1d - Failure Count", j));
            retryJsonArr = new JsonArray();
        }
        retryJsonArr.add(getJson(retryKeyList, retryOpList));
    }

    private List<String> getValueList(String op, List<Integer> latencyList, List<Integer> connAcqLatencyList) {
        DecimalFormat df = new DecimalFormat();
        df.setMaximumFractionDigits(2);

        List<String> valueList = new ArrayList<String>();
        valueList.add(op);
        valueList.add(df.format(latencyList.size()));
        valueList.add(df.format(ComputeUtil.getAverageLatency(latencyList)));
        valueList.add(df.format(ComputeUtil.getP99Latency(latencyList)));
        if (connAcqLatencyList != null)
            valueList.add(df.format(ComputeUtil.getAverageLatency(connAcqLatencyList)));
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
                LOG.info("\nJSON file retrieved : " + jsonStrings.get(jsonStrings.size() - 1));
            } catch (IOException ie) {
                LOG.error("Got exception while reading file", ie);
                return;
            }
        }
        LOG.info("Printing JSON info.. ");

        for (String jsonStr : jsonStrings) {
            JsonObject jsonObj = (JsonObject) new JsonParser().parse(jsonStr);
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
                        sum_newOrderLat[1] += Double.parseDouble(jObj.get("Avg. Latency").toString());
                        sum_newOrderLat[2] += Double.parseDouble(jObj.get("P99Latency").getAsString());
                        sum_newOrderLat[3] += Double.parseDouble(jObj.get("Connection Acq Latency").getAsString());
                        break;
                    case "Payment":
                        sum_paymentLat[0] += Double.parseDouble(jObj.get("Count").getAsString());
                        sum_paymentLat[1] += Double.parseDouble(jObj.get("Avg. Latency").getAsString());
                        sum_paymentLat[2] += Double.parseDouble(jObj.get("P99Latency").getAsString());
                        sum_paymentLat[3] += Double.parseDouble(jObj.get("Connection Acq Latency").getAsString());
                        break;
                    case "OrderStatus":
                        sum_orderStatusLat[0] += Double.parseDouble(jObj.get("Count").getAsString());
                        sum_orderStatusLat[1] += Double.parseDouble(jObj.get("Avg. Latency").getAsString());
                        sum_orderStatusLat[2] += Double.parseDouble(jObj.get("P99Latency").getAsString());
                        sum_orderStatusLat[3] += Double.parseDouble(jObj.get("Connection Acq Latency").getAsString());
                        break;
                    case "Delivery":
                        sum_deliveryLat[0] += Double.parseDouble(jObj.get("Count").getAsString());
                        sum_deliveryLat[1] += Double.parseDouble(jObj.get("Avg. Latency").getAsString());
                        sum_deliveryLat[2] += Double.parseDouble(jObj.get("P99Latency").getAsString());
                        sum_deliveryLat[3] += Double.parseDouble(jObj.get("Connection Acq Latency").getAsString());
                        break;
                    case "StockLevel":
                        sum_stockLevelLat[0] += Double.parseDouble(jObj.get("Count").getAsString());
                        sum_stockLevelLat[1] += Double.parseDouble(jObj.get("Avg. Latency").getAsString());
                        sum_stockLevelLat[2] += Double.parseDouble(jObj.get("P99Latency").getAsString());
                        sum_stockLevelLat[3] += Double.parseDouble(jObj.get("Connection Acq Latency").getAsString());
                        break;
                    case "All":
                        sum_AllLat[0] += Double.parseDouble(jObj.get("Count").getAsString());
                        sum_AllLat[1] += Double.parseDouble(jObj.get("Avg. Latency").getAsString());
                        sum_AllLat[2] += Double.parseDouble(jObj.get("P99Latency").getAsString());
                        sum_AllLat[3] += Double.parseDouble(jObj.get("Connection Acq Latency").getAsString());
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

}
