package com.oltpbenchmark.benchmarks.tpcc;

import java.io.FileWriter;
import java.io.IOException;

import com.google.gson.Gson;
import com.google.gson.JsonSerializer;

import com.google.gson.JsonObject;


public class WriteMetricsToJSON {


    public static void writeMetricsToJSONFile(JsonObject jsonObject) {

        String jsonString = new Gson().toJson(jsonObject);
        try {
            FileWriter file = new FileWriter("/Users/sagarwal/output.json");
            file.write(jsonString);
            file.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        System.out.println("JSON file created: " + jsonObject);
    }


}
