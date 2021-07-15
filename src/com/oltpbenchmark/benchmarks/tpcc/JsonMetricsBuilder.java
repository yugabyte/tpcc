package com.oltpbenchmark.benchmarks.tpcc;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;


public class JsonMetricsBuilder {

    JsonObject jsonObject;

    int numWarehouses;
    int numDBConnections;
    int warmupTime;

    public JsonMetricsBuilder() {
        if (jsonObject == null)
            jsonObject = new JsonObject();
    }

    public void buildTestConfigJson(int nodes, int warehouses, int dbConn, int warmuptime, int runtime) {
        this.numWarehouses = warehouses;
        this.numDBConnections = dbConn;
        this.warmupTime = warehouses;

        JsonObject testConfigJson = new JsonObject();
        testConfigJson.addProperty("#Nodes", nodes);
        testConfigJson.addProperty("Warehouses", warehouses);
        testConfigJson.addProperty("#DBConnections", dbConn);
        testConfigJson.addProperty("WarmupTime (secs)", warmuptime);
        testConfigJson.addProperty("RunTime (secs)", runtime);
        jsonObject.add("Test Configuration", testConfigJson);
    }

    public void buildResultJson(double tpmc, double efficiency, double throughput) {
        JsonObject resultJson = new JsonObject();
        resultJson.addProperty("TPMC", tpmc);
        resultJson.addProperty("Efficiency", efficiency);
        resultJson.addProperty("Throughput", throughput);
        jsonObject.add("Results", resultJson);
    }

    public void buildLatJsonObject(String latType, List<List<String>> latencyList) {
        List<String> keyList = new ArrayList<String>() {{
            add("Transaction");
            add("Count");
            add("Avg. Latency");
            add("P99Latency");
            add("Connection Acq Latency");
        }};

        JsonArray latJsonArr = new JsonArray();

        for (int i = 0; i < latencyList.size(); i++) {
            List<String> valueList = latencyList.get(i);
            JsonObject latJson = new JsonObject();
            for (int j = 0; j < keyList.size(); j++)
                latJson.addProperty(keyList.get(j), valueList.get(j));
            latJsonArr.add(latJson);
        }
        jsonObject.add("Latencies", latJsonArr);
    }

    public void buildLatencyJsonObject(List<List<String>> latencyList) {
        buildLatJsonObject("Latencies", latencyList);
    }

    public void buildFailureLatencyJsonObject(List<List<String>> failureLatencyList) {
        buildLatJsonObject("Failure Latencies", failureLatencyList);
    }

    public void buildAggLatJsonObject(Map<String, List<List<String>>> workLatenciesMap) {
        List<String> keyList = new ArrayList<String>() {
            {
                add("Task");
                add("Count");
                add("Avg. Latency");
                add("P99Latency");
            }
        };
        JsonObject aggLatJsonArr = new JsonObject();
        for (Map.Entry<String, List<List<String>>> entry : workLatenciesMap.entrySet()) {
            List<List<String>> opWorkList = entry.getValue();
            JsonArray aggLatOpArr = new JsonArray();
            for (int j = 0; j < opWorkList.size(); j++) {
                List<String> valueList = opWorkList.get(j);
                JsonObject aggLatJson = new JsonObject();
                for (int i = 0; i < keyList.size(); i++)
                    aggLatJson.addProperty(keyList.get(i), valueList.get(i));
                if (valueList.get(0).equalsIgnoreCase("All"))
                    aggLatJson.addProperty("All", valueList.get(keyList.size() + 1));
                aggLatOpArr.add(aggLatJson);
            }
            aggLatJsonArr.add(entry.getKey(), aggLatOpArr);
        }
        jsonObject.add("Work Task Latencies", aggLatJsonArr);
    }

    public void buildRetryJsonObject(int numTriesPerProc, List<List<String>> retryOpList) {
        List<String> keyList = new ArrayList<String>() {
            {
                add("Transaction");
                add("Count");
            }
        };
        for (int j = 0; j < numTriesPerProc; ++j) {
            keyList.add(String.format(" Retry #%1d - Failure Count ", j));
        }
        JsonArray retryJsonArr = new JsonArray();
        for (int i = 0; i < retryOpList.size(); i++) {
            JsonObject retryJson = new JsonObject();
            List<String> valueList = retryOpList.get(i);
            for (int j = 0; j < keyList.size(); j++)
                retryJson.addProperty(keyList.get(j), valueList.get(j));
            retryJsonArr.add(retryJson);
        }
        jsonObject.add("Retry Attempts", retryJsonArr);
    }

    public void writeMetricsToJSONFile() {
        String dest = "";
        try {
            dest = new File(".").getCanonicalPath() + File.separator
                    + "jsonOutput_" + numWarehouses + "WH_" + numDBConnections + "Conn_" + warmupTime + "_"
                    + new SimpleDateFormat("dd-MM-yy_HHmm").format(new Date())
                    + ".JSON";
        } catch (IOException e) {
            System.out.println("Exception occurred while creating log file" +
                    "\nError Message:" + e.getMessage());
        }
        String jsonString = new GsonBuilder().setPrettyPrinting().create().toJson(jsonObject);
        try {
            FileWriter file = new FileWriter(dest);
            file.write(jsonString);
            file.close();
        } catch (IOException e) {
            System.out.println("Got exception while writing JSON metrics to file.");
            e.printStackTrace();
        }
    }
}
