package com.oltpbenchmark.benchmarks.tpcc;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.oltpbenchmark.util.ComputeUtil;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;


public class JsonMetricsBuilder {

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

    public JsonMetricsBuilder () {
        jsonObject = new JsonObject();
    }

    private JsonObject getJson(List<String> keyList, List<String> valueList) {
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

    public void buildLatencyJson(String latType) {
        jsonObject.add(latType, latJsonArr);
        latJsonArr = null;
    }

    public void addLatencyJson(String op, List<Integer> latencyList, List<Integer> connLatencyList) {
        if(latJsonArr == null)
            latJsonArr = new JsonArray();
        latJsonArr.add(getJson(latencyKeyList, getValueList(op,latencyList,connLatencyList)));
    }

    public void buildWorkerTaskLatJson(String op) {
        if(op.equalsIgnoreCase("Worker Task Latency")) {
            jsonObject.add(op, aggLatJsonObject);
            aggLatJsonObject = null;
        }
        else {
            aggLatJsonObject.add(op, aggLatJsonArr);
            aggLatJsonArr = null;
        }
    }

    public void addWorkerTaskLatencyJson(String task, List<Integer> latencyList){
        if(aggLatJsonObject == null)
            aggLatJsonObject = new JsonObject();
        if(aggLatJsonArr == null)
            aggLatJsonArr = new JsonArray();
        aggLatJsonArr.add(getJson(workerTaskKeyList,getValueList(task,latencyList,null)));
    }

    public void buildQueryAttemptsJson() {
        jsonObject.add("Retry Attempts", retryJsonArr);
    }

    public void addRetryJson(String op, int numTriesPerProc, List<String> retryOpList) {
        if(retryJsonArr == null) {
           retryKeyList = new ArrayList<String>() {
                {
                    add("Transaction");
                    add("Count");
                }
            };
            for (int j = 0; j < numTriesPerProc; ++j)
                retryKeyList.add(String.format(" Retry #%1d - Failure Count ", j));
            retryJsonArr = new JsonArray();
        }
        retryJsonArr.add(getJson(retryKeyList,retryOpList));
    }

    private List<String> getValueList (String op, List<Integer> latencyList, List<Integer> connAcqLatencyList) {
        List<String> valueList = new ArrayList<String>();
        valueList.add(op);
        valueList.add(String.valueOf(latencyList.size()));
        valueList.add(String.valueOf(ComputeUtil.getAverageLatency(latencyList)));
        valueList.add(String.valueOf(ComputeUtil.getP99Latency(latencyList)));
        if(connAcqLatencyList!=null)
            valueList.add(String.valueOf(ComputeUtil.getAverageLatency(connAcqLatencyList)));
        return valueList;
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
