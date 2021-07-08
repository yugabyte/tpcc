package com.oltpbenchmark.benchmarks.tpcc;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;


public class WriteMetricsToJSON {


    public static JsonObject getJson(List<String> keyList, List<String> valueList){
        JsonObject jsonObject = new JsonObject();
        for(int i = 0; i< keyList.size(); i++){
            jsonObject.addProperty(keyList.get(i), valueList.get(i));
        }
        return jsonObject;
    }

    public static void writeMetricsToJSONFile(int numWareHouses, int warmupTime, int numDBConnections, JsonObject jsonObject) {
        String dest = "";
        try {
            dest = new File(".").getCanonicalPath() + File.separator
                    + "jsonOutput_" + numWareHouses + "WH_" + numDBConnections + "Conn_" + warmupTime + "_"
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
