/******************************************************************************
 *  Copyright 2015 by OLTPBenchmark Project                                   *
 *                                                                            *
 *  Licensed under the Apache License, Version 2.0 (the "License");           *
 *  you may not use this file except in compliance with the License.          *
 *  You may obtain a copy of the License at                                   *
 *                                                                            *
 *    http://www.apache.org/licenses/LICENSE-2.0                              *
 *                                                                            *
 *  Unless required by applicable law or agreed to in writing, software       *
 *  distributed under the License is distributed on an "AS IS" BASIS,         *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  *
 *  See the License for the specific language governing permissions and       *
 *  limitations under the License.                                            *
 ******************************************************************************/


package com.oltpbenchmark;

import java.io.PrintStream;
import java.util.*;

import com.oltpbenchmark.TransactionLatencyRecord.Sample;
import com.oltpbenchmark.ThreadBench.TimeBucketIterable;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.util.Histogram;

public final class Results {
  public final long nanoSeconds;
  public final int measuredRequests;
  public long startTime;
  public long endTime;
  final Histogram<TransactionType> txnSuccess = new Histogram<>();
  final Histogram<TransactionType> txnAbort = new Histogram<>();
  final Histogram<TransactionType> txnRetry = new Histogram<>();
  final Histogram<TransactionType> txnErrors = new Histogram<>();
  final Map<TransactionType, Histogram<String>> txnAbortMessages =
          new HashMap<>();

  public final List<TransactionLatencyRecord.Sample> latencySamples;

  public Results(long nanoSeconds, int measuredRequests, final List<TransactionLatencyRecord.Sample> latencySamples) {
    this.nanoSeconds = nanoSeconds;
    this.measuredRequests = measuredRequests;

      // defensive copy
      this.latencySamples = Collections.unmodifiableList(new ArrayList<>(latencySamples));
      assert !this.latencySamples.isEmpty();
  }

  /**
   * Get a histogram of how often each transaction was executed
   */
  public final Histogram<TransactionType> getTransactionSuccessHistogram() {
    return (this.txnSuccess);
  }
  public final Histogram<TransactionType> getTransactionRetryHistogram() {
    return (this.txnRetry);
  }
  public final Histogram<TransactionType> getTransactionAbortHistogram() {
    return (this.txnAbort);
  }
  public final Histogram<TransactionType> getTransactionErrorHistogram() {
    return (this.txnErrors);
  }
  public final Map<TransactionType, Histogram<String>> getTransactionAbortMessageHistogram() {
    return (this.txnAbortMessages);
  }

  public double getRequestsPerSecond() {
    return (double) measuredRequests / (double) nanoSeconds * 1e9;
  }

  @Override
  public String toString() {
      return "Results(nanoSeconds=" + nanoSeconds + ", measuredRequests=" + measuredRequests +
             ") = " + getRequestsPerSecond() + " requests/sec";
  }

  public void writeCSV(int windowSizeSeconds, PrintStream out) {
      writeCSV(windowSizeSeconds, out, TransactionType.INVALID);
  }

  public void writeCSV(int windowSizeSeconds, PrintStream out, TransactionType txType) {
    out.println("time(sec), throughput(req/sec), avg_lat(ms), sd_lat(ms), min_lat(ms), 25th_lat(ms), " +
                "median_lat(ms), 75th_lat(ms), 90th_lat(ms), 95th_lat(ms), 99th_lat(ms), " +
                "max_lat(ms), tp (req/s) scaled");
    int i = 0;
    for (DistributionStatistics s :
         new TimeBucketIterable(latencySamples, windowSizeSeconds, txType)) {
      final double MILLISECONDS_FACTOR = 1e3;
      out.printf("%d,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f\n",
                 i * windowSizeSeconds, (double) s.getCount() / windowSizeSeconds,
                 s.getAverage() / MILLISECONDS_FACTOR,
                 s.getStandardDeviation() / (MILLISECONDS_FACTOR * MILLISECONDS_FACTOR),
                 s.getMinimum() / MILLISECONDS_FACTOR, s.get25thPercentile() / MILLISECONDS_FACTOR,
                 s.getMedian() / MILLISECONDS_FACTOR,
                 s.get75thPercentile() / MILLISECONDS_FACTOR,
                 s.get90thPercentile() / MILLISECONDS_FACTOR,
                 s.get95thPercentile() / MILLISECONDS_FACTOR,
                 s.get99thPercentile() / MILLISECONDS_FACTOR,
                 s.getMaximum() / MILLISECONDS_FACTOR,
                 MILLISECONDS_FACTOR / s.getAverage());
      i += 1;
    }
  }

  public void writeCSV2(PrintStream out) {
      writeCSV2(1, out, TransactionType.INVALID);
  }

  public void writeCSV2(int windowSizeSeconds, PrintStream out, TransactionType txType) {
    String[] header = {
      "Time (seconds)",
      "Requests",
      "Throughput (requests/second)",
      "Minimum Latency (microseconds)",
      "25th Percentile Latency (microseconds)",
      "Median Latency (microseconds)",
      "Average Latency (microseconds)",
      "Standard Deviation Latency (microseconds)",
      "75th Percentile Latency (microseconds)",
      "90th Percentile Latency (microseconds)",
      "95th Percentile Latency (microseconds)",
      "99th Percentile Latency (microseconds)",
      "Maximum Latency (microseconds)"
    };
    out.println(String.join(",", header));
    int i = 0;
    for (DistributionStatistics s : new TimeBucketIterable(latencySamples, windowSizeSeconds, txType)) {
      out.printf("%d,%d,%.3f,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d\n",
          i * windowSizeSeconds,
          s.getCount(),
          (double) s.getCount() / windowSizeSeconds,
          (int) s.getMinimum(),
          (int) s.get25thPercentile(),
          (int) s.getMedian(),
          (int) s.getAverage(),
          (int) s.getStandardDeviation(),
          (int) s.get75thPercentile(),
          (int) s.get90thPercentile(),
          (int) s.get95thPercentile(),
          (int) s.get99thPercentile(),
          (int) s.getMaximum());
      i += 1;
    }
  }

  public void writeAllCSVAbsoluteTiming(List<TransactionType> activeTXTypes, PrintStream out) {
    out.println("Start," + startTime);
    out.println("End," + endTime);
    String[] header = {
        "Transaction Name",
        "Start Time (nanoseconds)",
        "Connection Latency (microseconds)",
        "OperationLatency (microseconds)"
    };
    out.println(String.join(",", header));
    for (Sample s : latencySamples) {
      String[] row = {
          activeTXTypes.get(s.tranType-1).getName(),
          Long.toString(s.startNs),
          Integer.toString(s.connLatencyUs),
          Integer.toString(s.operationLatencyUs),
      };
      out.println(String.join(",", row));
    }
  }
}
