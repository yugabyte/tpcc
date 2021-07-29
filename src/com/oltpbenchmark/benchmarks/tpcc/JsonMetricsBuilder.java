package com.oltpbenchmark.benchmarks.tpcc;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;


public class JsonMetricsBuilder {

    JsonObject jsonObject;

    int numWarehouses;
    int numDBConnections;
    int warmupTime;
    int numNodes;


    public static JsonObject getJson(List<String> keyList, List<String> valueList) {
        JsonObject jsonObject = new JsonObject();
        for (int i = 0; i < keyList.size(); i++) {
            jsonObject.addProperty(keyList.get(i), valueList.get(i));
        }
        return jsonObject;
    }

    public void buildTestConfigJson(int numNodes, int warehouses, int numDBConn, int warmuptime, int runtime) {
        this.numWarehouses = warehouses;
        this.numDBConnections = numDBConn;
        this.warmupTime = warmuptime;
        this.numNodes = numNodes;

        JsonObject testConfigJson = new JsonObject();
        testConfigJson.addProperty("#Nodes", numNodes);
        testConfigJson.addProperty("Warehouses", warehouses);
        testConfigJson.addProperty("#DBConnections", numDBConn);
        testConfigJson.addProperty("WarmupTime (secs)", warmuptime);
        testConfigJson.addProperty("RunTime (secs)", runtime);
        jsonObject.add("Test Configuration", testConfigJson);
    }

    public void buildResultJson(double tpmc, double efficiency, double throughput) {
        JsonObject resultJson = new JsonObject();
        resultJson.addProperty("TPM-C", tpmc);
        resultJson.addProperty("Efficiency", efficiency);
        resultJson.addProperty("Throughput", throughput);
        jsonObject.add("Results", resultJson);
    }

    public void buildLatencyJson(String latType, List<List<String>> latencyList) {
        List<String> keyList = new ArrayList<String>() {{
            add("Transaction");
            add("Count");
            add("Avg. Latency");
            add("P99Latency");
            add("Connection Acq Latency");
        }};

        JsonArray latJsonArr = new JsonArray();

        for (int i = 0; i < latencyList.size(); i++) {
            latJsonArr.add(getJson(keyList, latencyList.get(i)));
        }
        jsonObject.add(latType, latJsonArr);
    }

    public void buildLatencyJson(List<List<String>> latencyList) {
        buildLatencyJson("Latencies", latencyList);
    }

    public void buildFailureLatencyJson(List<List<String>> failureLatencyList) {
        buildLatencyJson("Failure Latencies", failureLatencyList);
    }

    public void buildWorkerTaskLatencyJson(Map<String, List<List<String>>> workLatenciesMap) {
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
                aggLatOpArr.add(getJson(keyList,opWorkList.get(j)));
            }
            aggLatJsonArr.add(entry.getKey(), aggLatOpArr);
        }
        jsonObject.add("Work Task Latencies", aggLatJsonArr);
    }

    public void buildQueryAttemptsJson(int numTriesPerProc, List<List<String>> retryOpList) {
        List<String> keyList = new ArrayList<String>() {
            {
                add("Transaction");
                add("Count");
            }
        };
        for (int j = 0; j < numTriesPerProc; ++j)
            keyList.add(String.format(" Retry #%1d - Failure Count ", j));

        JsonArray retryJsonArr = new JsonArray();
        for (int i = 0; i < retryOpList.size(); i++) {
            retryJsonArr.add(getJson(keyList,retryOpList.get(i)));
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
