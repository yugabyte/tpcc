package com.oltpbenchmark.util;

import java.util.Collections;
import java.util.List;

public class LatencyMetricsUtil {
    public static double getAverageLatency(List<Integer> latencies) {
      if (latencies.size() == 0) {
        return -1;
      }
      long sum = 0;
      for (int val : latencies) {
        sum += val;
      }
      return sum * 1.0 / latencies.size() / 1000;
    }

    public static double getP99Latency(List<Integer> latencies) {
      if (latencies.size() == 0) {
        return -1;
      }
      Collections.sort(latencies);
      int p99Index = (int)(latencies.size() * 0.99);
      return latencies.get(p99Index) * 1.0 / 1000;
    }
}
