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

import java.io.*;
import java.text.DecimalFormat;
import java.util.*;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.TransactionTypes;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.tpcc.procedures.*;
import com.oltpbenchmark.jdbc.InstrumentedPreparedStatement;
import com.oltpbenchmark.util.FileUtil;
import com.oltpbenchmark.util.StringBoxUtil;
import com.oltpbenchmark.util.StringUtil;

import static java.lang.Integer.min;

public class DBWorkload {
  private static final Logger LOG = Logger.getLogger(DBWorkload.class);

  private static final String SINGLE_LINE = StringUtils.repeat("=", 70);

  private static int newOrderTxnId = -1;
  private static int numWarehouses = 10;
  private static int time = 0;
  private static int warmupTime = 0;
  private static final Map<Integer, String> transactionTypes = new HashMap<>();

  /**
   * Returns true if asserts are enabled. This assumes that
   * we're always using the default system ClassLoader
   */
  private static boolean isAssertsEnabled() {
    try {
      assert(false);
      return false;
    } catch (AssertionError ex) {
      return true;
    }
  }

  public static void main(String[] args) throws Exception {
    // Initialize log4j
    String log4jPath = System.getProperty("log4j.configuration");
    if (log4jPath != null) {
      org.apache.log4j.PropertyConfigurator.configure(log4jPath);
    } else {
      throw new RuntimeException("Missing log4j.properties file");
    }

    if (isAssertsEnabled()) {
      LOG.warn("\n" + getAssertWarning());
    }

    CommandLineOptions options = new CommandLineOptions();
    options.init(args);

    transactionTypes.put(1, "NewOrder");
    transactionTypes.put(2, "Payment");
    transactionTypes.put(3, "OrderStatus");
    transactionTypes.put(4, "Delivery");
    transactionTypes.put(5, "StockLevel");

    if (options.getMode() == CommandLineOptions.Mode.HELP) {
      options.printHelp();
      return;
    }

    if (options.getMode() == CommandLineOptions.Mode.MERGE_RESULTS) {
      String dirPath = options.getDirPath().orElseThrow(() ->
              new RuntimeException("Must specify directory with results to merge with --dir={directory path}"));
      File dir = new File(dirPath);
      String[] files = dir.list();
      mergeResults(dirPath, files);
      return;
    }

    // Seconds
    int intervalMonitor = options.getIntervalMonitor().orElse(0);

    List<String> nodes = options.getNodes().orElse(Collections.singletonList("127.0.0.1"));

    numWarehouses = options.getWarehouses().orElse(numWarehouses);

    int startWarehouseIdForShard = options.getStartWarehouseId().orElse(1);

    int totalWarehousesAcrossShards = options.getTotalWarehouses().orElse(numWarehouses);

    int loaderThreads = options.getLoaderThreads().orElse(min(10, numWarehouses));

    // -------------------------------------------------------------------
    // GET PLUGIN LIST
    // -------------------------------------------------------------------

    String targetBenchmarks = "tpcc";

    String[] targetList = targetBenchmarks.split(",");
    List<BenchmarkModule> benchList = new ArrayList<>();

    // Use this list for filtering of the output
    List<TransactionType> activeTXTypes = new ArrayList<>();

    String configFile = options.getConfigFile().orElse("config/workload_all.xml");
    ConfigFileOptions configOptions = new ConfigFileOptions(configFile);

    // Load the configuration for each benchmark
    int lastTxnId = 0;
    for (String plugin : targetList) {
      // ----------------------------------------------------------------
      // BEGIN LOADING WORKLOAD CONFIGURATION
      // ----------------------------------------------------------------

      WorkloadConfiguration wrkld = new WorkloadConfiguration();
      wrkld.setBenchmarkName(plugin);

      // Pull in database configuration
      wrkld.setDBDriver(configOptions.getDbDriver());

      wrkld.setNodes(nodes);

      wrkld.setDBName(configOptions.getDbName());
      wrkld.setDBUsername(configOptions.getDbUsername());
      wrkld.setDBPassword(configOptions.getDbPassword());

      configOptions.getSslCert().ifPresent(wrkld::setSslCert);
      configOptions.getSslKey().ifPresent(wrkld::setSslKey);
      configOptions.getJdbcUrl().ifPresent(wrkld::setJdbcURL);

      int terminals = numWarehouses * 10;
      wrkld.setTerminals(terminals);

      wrkld.setLoaderThreads(loaderThreads);

      wrkld.setIsolationMode(configOptions.getIsolationLevel().orElse("TRANSACTION_SERIALIZABLE"));

      wrkld.setNumWarehouses(numWarehouses);
      wrkld.setStartWarehouseIdForShard(startWarehouseIdForShard);
      wrkld.setTotalWarehousesAcrossShards(totalWarehousesAcrossShards);

      configOptions.getUseKeyingTime().ifPresent(wrkld::setUseKeyingTime);
      configOptions.getUseThinkTime().ifPresent(wrkld::setUseThinkTime);
      configOptions.getEnableForeignKeysAfterLoad().ifPresent(wrkld::setEnableForeignKeysAfterLoad);

      if (options.getStartWarehouseId().isPresent()) {
          wrkld.setShouldEnableForeignKeys(false);
      }

      configOptions.getBatchSize().ifPresent(wrkld::setBatchSize);
      configOptions.getMaxRetriesPerTransaction().ifPresent(wrkld::setMaxRetriesPerTransaction);
      configOptions.getMaxLoaderRetries().ifPresent(wrkld::setMaxLoaderRetries);

      configOptions.getTrackPerSQLStmtLatencies().ifPresent(InstrumentedPreparedStatement::trackLatencyMetrics);

      configOptions.getPort().ifPresent(wrkld::setPort);

      int numDBConnections = options.getNumDbConnections().orElse(min(numWarehouses, wrkld.getNodes().size() * 200));
      wrkld.setNumDBConnections(numDBConnections);

      configOptions.getHikariConnectionTimeoutMs().ifPresent(wrkld::setHikariConnectionTimeout);

      if (wrkld.getNumDBConnections() <= 0) {
        wrkld.setNumDBConnections(wrkld.getTerminals());
      }

      if (wrkld.getNumDBConnections() < wrkld.getLoaderThreads()) {
        wrkld.setNumDBConnections(wrkld.getLoaderThreads());
      }

      if (Arrays.asList(CommandLineOptions.Mode.CLEAR, CommandLineOptions.Mode.EXECUTE).contains(options.getMode())) {
        wrkld.setNeedsExecution(true);
      }

      configOptions.getUseStoredProcedures().ifPresent(wrkld::setUseStoredProcedures);

      LOG.info("Configuration -> nodes: " + wrkld.getNodes() +
               ", port: " + wrkld.getPort() +
               ", startWH: " + wrkld.getStartWarehouseIdForShard() +
               ", warehouses: " + wrkld.getNumWarehouses() +
               ", total warehouses across shards: " + wrkld.getTotalWarehousesAcrossShards() +
               ", terminals: " + wrkld.getTerminals() +
               ", dbConnections: " + wrkld.getNumDBConnections() +
               ", loaderThreads: " + wrkld.getLoaderThreads() );

      options.getInitialDelaySeconds().ifPresent((initialDelay) -> {
        LOG.info("Delaying execution of workload for " + initialDelay + " seconds");
        try {
          Thread.sleep(initialDelay * 1000L);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      });

      // ----------------------------------------------------------------
      // CREATE BENCHMARK MODULE
      // ----------------------------------------------------------------
      BenchmarkModule bench = new BenchmarkModule(wrkld);
      Map<String, Object> initDebug = new ListOrderedMap<>();
      initDebug.put("Benchmark", String.format("%s {%s}", plugin.toUpperCase(), "BenchmarkModule"));
      initDebug.put("Configuration", configFile);
      initDebug.put("Driver", wrkld.getDBDriver());
      initDebug.put("URL", wrkld.getNodes());
      initDebug.put("Isolation", wrkld.getIsolationString());
      initDebug.put("Scale Factor", wrkld.getNumWarehouses());

      LOG.info(SINGLE_LINE + "\n\n" + StringUtil.formatMaps(initDebug));
      LOG.info(SINGLE_LINE);

      // ----------------------------------------------------------------
      // LOAD TRANSACTION DESCRIPTIONS
      // ----------------------------------------------------------------
      int numTxnTypes = configOptions.getTransactionTypeCount();
      wrkld.setNumTxnTypes(numTxnTypes);

      List<TransactionType> ttypes = new ArrayList<>();
      ttypes.add(TransactionType.INVALID);
      List<String> weight_strings = new ArrayList<>();
      int txnIdOffset = lastTxnId;
      for (int i = 1; i <= wrkld.getNumTxnTypes(); i++) {
        String txnName = configOptions.getTransactionTypeName(i)
            .orElseThrow(() -> new RuntimeException("Every <transaction> in config must have a <name>."));

        String weight = configOptions.getTransactionTypeWeight(i)
            .orElseThrow(() -> new RuntimeException("Every <transaction> in config must have a <weight>."));
        weight_strings.add(weight);

        TransactionType tmpType = bench.initTransactionType(txnName, i + txnIdOffset);
        if (txnName.equals("NewOrder")) {
          newOrderTxnId = i + txnIdOffset;
        }

        // Keep a reference for filtering
        activeTXTypes.add(tmpType);

        // Add a ref for the active TTypes in this benchmark
        ttypes.add(tmpType);
        lastTxnId = i;
      } // FOR

      // Wrap the list of transactions and save them
      TransactionTypes tt = new TransactionTypes(ttypes);
      wrkld.setTransTypes(tt);
      LOG.debug("Using the following transaction types: " + tt);

      benchList.add(bench);

      int rate = configOptions.getRate()
          .orElseThrow(() ->new RuntimeException("Must specify configuration value for rate."));

      time = configOptions.getRuntime()
          .orElseThrow(() ->new RuntimeException("Must specify configuration value for runtime."));

      warmupTime = options.getWarmupTime().orElse(warmupTime);

      boolean timed = (time > 0);
      if (warmupTime < 0) {
        LOG.fatal("Must provide nonnegative time bound for"
                + " warmup.");
        System.exit(-1);
      }

      wrkld.addWork(time,
                    warmupTime,
                    rate,
                    weight_strings,
                    true, // TODO -- remove all hard-coded parameters here
                    false,
                    false,
                    timed,
                    terminals,
                    Phase.Arrival.REGULAR);

      // CHECKING INPUT PHASES
      int j = 0;
      for (Phase p : wrkld.getAllPhases()) {
        j++;
        if (p.getWeightCount() != wrkld.getNumTxnTypes()) {
          LOG.fatal(String.format("Configuration files is inconsistent, phase %d contains %d weights but you defined %d transaction types",
                                  j, p.getWeightCount(), wrkld.getNumTxnTypes()));
          if (p.isSerial()) {
            LOG.fatal("However, note that since this a serial phase, the weights are irrelevant (but still must be included---sorry).");
          }
          System.exit(-1);
        }
      } // FOR

      // Generate the dialect map
      wrkld.init();

      assert (wrkld.getNumTxnTypes() >= 0);
    }
    assert(!benchList.isEmpty());
    assert(benchList.get(0) != null);

    // Create the Benchmark's Database
    if (options.getMode() == CommandLineOptions.Mode.CREATE) {
      for (BenchmarkModule benchmark : benchList) {
        LOG.info("Creating new " + benchmark.getBenchmarkName().toUpperCase() + " database...");
        runCreator(benchmark);
        benchmark.createSqlProcedures();
        LOG.info("Finished!");
        LOG.info(SINGLE_LINE);
      }
    } else if (LOG.isDebugEnabled()) {
      LOG.debug("Skipping creating benchmark database tables");
      LOG.info(SINGLE_LINE);
    }

    // Clear the Benchmark's Database
    if (options.getMode() == CommandLineOptions.Mode.CLEAR) {
      for (BenchmarkModule benchmark : benchList) {
        LOG.info("Resetting " + benchmark.getBenchmarkName().toUpperCase() + " database...");
        benchmark.clearDatabase();
        LOG.info("Finished!");
        LOG.info(SINGLE_LINE);
      }
    } else if (LOG.isDebugEnabled()) {
      LOG.debug("Skipping creating benchmark database tables");
      LOG.info(SINGLE_LINE);
    }

    // Execute Loader
    if (options.getMode() == CommandLineOptions.Mode.LOAD) {
      for (BenchmarkModule benchmark : benchList) {
        LOG.info(String.format("Loading data into %s database with %d threads...",
                               benchmark.getBenchmarkName().toUpperCase(),
                               benchmark.getWorkloadConfiguration().getLoaderThreads()));
        runLoader(benchmark);
        LOG.info("Finished!");
        LOG.info(SINGLE_LINE);
      }
    } else if (LOG.isDebugEnabled()) {
      LOG.debug("Skipping loading benchmark database records");
      LOG.info(SINGLE_LINE);
    }

    if (options.getMode() == CommandLineOptions.Mode.ENABLE_FOREIGN_KEYS) {
      for (BenchmarkModule benchmark : benchList) {
        benchmark.enableForeignKeys();
      }
    }

    if (options.getIsCreateSqlProceduresSet()) {
      for (BenchmarkModule benchmark : benchList) {
        benchmark.createSqlProcedures();
      }
    }

    // Execute Workload
    if (options.getMode() == CommandLineOptions.Mode.EXECUTE) {
      // Bombs away!
      Results r = null;
      try {
        r = runWorkload(benchList, intervalMonitor, options.getShouldOutputVerboseExecuteResults());
      } catch (Throwable ex) {
        LOG.error("Unexpected error when running benchmarks.", ex);
        System.exit(1);
      }

      // WRITE OUTPUT
      writeRawOutput(r, activeTXTypes);

      // WRITE HISTOGRAMS
      if (options.getIsOutputMetricHistogramsSet()) {
        writeHistograms(r);
      }
    } else {
      LOG.info("Skipping benchmark workload execution");
    }
  }

  private static void writeHistograms(Results r) {
    StringBuilder sb = new StringBuilder();

    sb.append(StringUtil.bold("Completed Transactions:"))
      .append("\n")
      .append(r.getTransactionSuccessHistogram())
      .append("\n\n");

    sb.append(StringUtil.bold("Aborted Transactions:"))
      .append("\n")
      .append(r.getTransactionAbortHistogram())
      .append("\n\n");

    sb.append(StringUtil.bold("Rejected Transactions (Server Retry):"))
      .append("\n")
      .append(r.getTransactionRetryHistogram())
      .append("\n\n");

    sb.append(StringUtil.bold("Unexpected Errors:"))
      .append("\n")
      .append(r.getTransactionErrorHistogram());

    if (!r.getTransactionAbortMessageHistogram().isEmpty())
      sb.append("\n\n")
        .append(StringUtil.bold("User Aborts:"))
        .append("\n")
        .append(r.getTransactionAbortMessageHistogram());

    LOG.info(SINGLE_LINE);
    LOG.info("Workload Histograms:\n" + sb.toString());
    LOG.info(SINGLE_LINE);
  }


  /**
   * Write out the results for a benchmark run to a bunch of files.
   */
  private static void writeRawOutput(Results r, List<TransactionType> activeTXTypes) throws FileNotFoundException {
    String outputDirectory = "results";
    FileUtil.makeDirIfNotExists(outputDirectory.split("/"));
    String baseFile = "oltpbench";

    String nextName = FileUtil.getNextFilename(outputDirectory, baseFile, ".csv");
    PrintStream rs = new PrintStream(nextName);
    LOG.info("Output Raw data into file: " + nextName);
    r.writeAllCSVAbsoluteTiming(activeTXTypes, rs);
    rs.close();
  }

  private static void runCreator(BenchmarkModule bench) {
    LOG.debug(String.format("Creating %s Database", bench));
    bench.createDatabase();
  }

  private static void runLoader(BenchmarkModule bench) {
    LOG.debug(String.format("Loading %s Database", bench));
    bench.loadDatabase();
  }

  private static Results runWorkload(List<BenchmarkModule> benchList, int intervalMonitor, boolean outputVerboseRes) {
    List<Worker> workers = new ArrayList<>();
    List<WorkloadConfiguration> workConfs = new ArrayList<>();

    long start = System.nanoTime();
    long end = start + (long) (warmupTime + time) * 1000 * 1000 * 1000;
    for (BenchmarkModule bench : benchList) {
      LOG.info("Creating " + bench.getWorkloadConfiguration().getTerminals() +
               " virtual terminals...");
      workers.addAll(bench.makeWorkers());
      // LOG.info("done.");

      int num_phases = bench.getWorkloadConfiguration().getNumberOfPhases();
      LOG.info(String.format("Launching the %s Benchmark with %s Phase%s...",
              bench.getBenchmarkName().toUpperCase(), num_phases, (num_phases > 1 ? "s" : "")));
      workConfs.add(bench.getWorkloadConfiguration());
    }
    Results r = ThreadBench.runRateLimitedBenchmark(workers, workConfs, intervalMonitor);
    r.startTime = start;
    r.endTime = end;

    PrintToplineResults(workers, r);
    PrintLatencies(workers);
    if (outputVerboseRes) {
      PrintQueryAttempts(workers, workConfs.get(0));
    }

    // TODO -- pretty print these items.
    if (InstrumentedPreparedStatement.isTrackingLatencyMetrics()) {
      NewOrder.printLatencyStats();
      Payment.printLatencyStats();
      OrderStatus.printLatencyStats();
      Delivery.printLatencyStats();
      StockLevel.printLatencyStats();
    }

    return r;
  }

  private static void PrintToplineResults(List<Worker> workers, Results r) {
    long numNewOrderTransactions = 0;
    for (Worker w : workers) {
      for (LatencyRecord.Sample sample : w.getLatencyRecords()) {
        if (sample.tranType == newOrderTxnId && sample.startNs + 1000L * sample.latencyUs <= r.endTime) {
          ++numNewOrderTransactions;
        }
      }
    }
    LOG.info(SINGLE_LINE);

    double tpmc = 1.0 * numNewOrderTransactions * 60 / time;
    double efficiency = 1.0 * tpmc * 100 / numWarehouses / 12.86;
    DecimalFormat df = new DecimalFormat();
    df.setMaximumFractionDigits(2);

    String resultOut = "\n" +
            "================RESULTS================\n" +
            String.format("%18s | %18.2f\n", "TPM-C", tpmc) +
            String.format("%18s | %17.2f%%\n", "Efficiency", efficiency) +
            String.format("%18s | %18.2f\n", "Throughput (req/s)", r.getRequestsPerSecond());
    LOG.info(resultOut);
  }

  private static void PrintLatencies(List<Worker> workers) {
    List<List<Integer>> list_latencies = new ArrayList<>();
    List<List<Integer>> list_conn_latencies = new ArrayList<>();
    for (int i = 0; i < 5; ++i) {
      list_latencies.add(new ArrayList<>());
      list_conn_latencies.add(new ArrayList<>());
    }

    for (Worker w : workers) {
      for (LatencyRecord.Sample sample : w.getLatencyRecords()) {
        list_latencies.get(sample.tranType - 1).add(sample.operationLatencyUs);
      }
      for (LatencyRecord.Sample sample : w.getAcqConnectionLatencyRecords()) {
        list_conn_latencies.get(sample.tranType - 1).add(sample.operationLatencyUs);
      }
    }

    StringBuilder resultOut = new StringBuilder();
    resultOut.append("\n");
    resultOut.append("================LATENCIES===============\n");
    resultOut.append(" Operation  | Avg. Latency | P99 Latency | Connection Acq Latency\n");
    for (int i = 0; i < list_latencies.size(); ++i) {
      String op = transactionTypes.get(i + 1);
      List<Integer> latencies = list_latencies.get(i);
      List<Integer> conn_latencies = list_conn_latencies.get(i);

      resultOut.append(String.format(
          "%11s |%13.2f |%12.2f |%23.2f\n",
          op, getAverageLatency(latencies), getP99Latency(latencies),
          getAverageLatency(conn_latencies)));
    }
    LOG.info(resultOut.toString());
  }

  private static void PrintQueryAttempts(List<Worker> workers, WorkloadConfiguration workConf) {
    int numTxnTypes = workConf.getNumTxnTypes();
    int numTriesPerProc = workConf.getMaxRetriesPerTransaction() + 1;
    int[][] totalRetries = new int[numTxnTypes][numTriesPerProc];
    int[] totalFailures = new int[numTxnTypes];
    for (Worker w : workers) {
      for (int i = 0; i < numTxnTypes; ++i) {
        totalFailures[i] += w.getTotalFailures()[i];
        for (int tryIdx = 0; tryIdx < numTriesPerProc; ++tryIdx) {
          totalRetries[i][tryIdx] += w.getTotalTries()[i][tryIdx];
        }
      }
    }

    StringBuilder resultOut = new StringBuilder();

    resultOut.append("\n");
    resultOut.append("====================RETRIES===================\n");
    resultOut.append("   Operation   | Total Created |");
    for (int i = 1; i < numTriesPerProc; ++i) {
      resultOut.append(String.format(" Retry Number %1d |", i));
    }
    resultOut.append("    Conflicts  \n");

    for (int i = 0; i < numTxnTypes; ++i) {
      String op = transactionTypes.get(i + 1);
      int totalCreated = totalRetries[i][0];
      resultOut.append(String.format("%14s |%14d |", op, totalCreated));
      for (int retry = 1; retry < numTriesPerProc; ++retry) {
        int numRetries = totalRetries[i][retry];
        double pctRetried = numRetries;
        pctRetried /= totalCreated;
        pctRetried *= 100.0;
        resultOut.append(String.format("%6d (%5.2f%%) |", numRetries, pctRetried));
      }

      int numFailed = totalFailures[i];
      double pctFailed = numFailed;
      pctFailed *= 100.0;
      pctFailed /= totalCreated;
      resultOut.append(String.format("%6d (%5.2f%%)\n", numFailed, pctFailed));
    }

    LOG.info(resultOut.toString());
  }

  private static String getOperationLatencyString(String operation, List<Integer> latencies) {
    if (latencies.size() == 0) {
      return "";
    }
    Collections.sort(latencies);
    long sum = 0;
    for (int val : latencies) {
      sum += val;
    }
    double avgLatency = sum * 1.0 / latencies.size() / 1000;
    int p99Index = (int)(latencies.size() * 0.99);
    double p99Latency = latencies.get(p99Index) * 1.0 / 1000;

    return operation + ", Avg Latency: " + avgLatency +
           " msecs, p99 Latency: " + p99Latency + " msecs";
  }

  private static double getAverageLatency(List<Integer> latencies) {
    if (latencies.size() == 0) {
      return -1;
    }
    long sum = 0;
    for (int val : latencies) {
      sum += val;
    }
    return sum * 1.0 / latencies.size() / 1000;
  }

  private static double getP99Latency(List<Integer> latencies) {
    if (latencies.size() == 0) {
      return -1;
    }
    Collections.sort(latencies);
    int p99Index = (int)(latencies.size() * 0.99);
    return latencies.get(p99Index) * 1.0 / 1000;
  }

  public static void mergeResults(String dirPath, String[] fileNames) {
    List<List<Integer>> list_latencies = new ArrayList<>();
    for (int i = 0; i < 5; ++i) {
      list_latencies.add(new ArrayList<>());
    }

    int  numNewOrderTransactions = 0;
    for (String file : fileNames) {
      if (!file.contains("csv")) {
        continue;
      }
      BufferedReader br;
      String line;
      try {
        br = new BufferedReader(new FileReader(dirPath + "/" + file));
        long end = -1;
        while ((line = br.readLine()) != null) {
          if (line.contains("Transaction Name")) {
            continue;
          }

          String[] arr = line.split(",");
          if (line.contains("Start")) {
            continue;
          }

          if (line.contains("End")) {
            end = Long.parseLong(arr[1]);
            continue;
          }

          int idx = -1;
          if (arr[0].contains("NewOrder")) {
            idx = 0;
          } else if (arr[0].contains("Payment")) {
            idx = 1;
          } else if (arr[0].contains("OrderStatus")) {
            idx = 2;
          } else if (arr[0].contains("Delivery")) {
            idx = 3;
          } else if (arr[0].contains("StockLevel")) {
            idx = 4;
          }
          list_latencies.get(idx).add(Integer.parseInt(arr[3]));
          if (idx == 0) {
            long opEndTime = Long.parseLong(arr[1]) + Long.parseLong(arr[2]) * 1000;
            if (opEndTime < end) {
              ++numNewOrderTransactions;
            }
          }
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    double tpmc = 1.0 * numNewOrderTransactions * 60 / time;
    double efficiency = 1.0 * tpmc * 100 / numWarehouses / 12.86;
    DecimalFormat df = new DecimalFormat();
    df.setMaximumFractionDigits(2);

    LOG.info("Num New Order transactions : " + numNewOrderTransactions + ", time seconds: " + time);
    LOG.info("TPM-C: " + df.format(tpmc));
    LOG.info("Efficiency : " + df.format(efficiency) + "%");
    for (int i = 0; i < list_latencies.size(); ++i) {
      LOG.info(getOperationLatencyString(transactionTypes.get(i+1),
                                         list_latencies.get(i)));
    }
  }



  public static String getAssertWarning() {
    String msg = "!!! WARNING !!!\n" +
                 "OLTP-Bench is executing with JVM asserts enabled. This will degrade runtime performance.\n" +
                 "You can disable them by setting the config option 'assertions' to FALSE";
    return StringBoxUtil.heavyBox(msg);
  }
}
