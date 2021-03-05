package com.oltpbenchmark.api;

import org.HdrHistogram.ConcurrentHistogram;
import org.HdrHistogram.Histogram;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class InstrumentedSQLStmt {
    Histogram histogram;
    SQLStmt sqlStmt;

    public static int numSigDigits = 4;
    /**
     * Constructor
     *
     * @param histogram
     * @param sqlStmt
     */
    public InstrumentedSQLStmt(Histogram histogram, SQLStmt sqlStmt) {
        this.histogram = histogram;
        this.sqlStmt = sqlStmt;
    }
    public InstrumentedSQLStmt(Histogram histogram, String sql, int...substitutions) {
        this.histogram = histogram;
        this.sqlStmt = new SQLStmt(sql, substitutions);
    }
    public InstrumentedSQLStmt(String sql, int...substitutions) {
        this.sqlStmt = new SQLStmt(sql, substitutions);
        this.histogram = new ConcurrentHistogram(numSigDigits);
    }

    public SQLStmt getSqlStmt() {
        return sqlStmt;
    }

    public Histogram getHistogram() {
        return histogram;
    }

    public void addLatency(long startNs, long endNs) {
        histogram.recordValue((endNs - startNs) / 1000);
    }

    public String getStats() {
       return getOperationLatencyString(histogram);
   }
 
   public static String getOperationLatencyString(Histogram histogram) {
       double avgLatency = histogram.getMean() / 1000;
       double p99Latency = histogram.getValueAtPercentile(99.0) / 1000;
 
       return "Count : " + histogram.getTotalCount() + " Avg Latency: " + avgLatency +
               " msecs, p99 Latency: " + p99Latency + " msecs";
  }
}
