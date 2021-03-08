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
import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.util.*;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.commons.configuration.SubnodeConfiguration;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.configuration.tree.xpath.XPathExpressionEngine;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.TransactionTypes;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.tpcc.procedures.*;
import com.oltpbenchmark.jdbc.InstrumentedPreparedStatement;
import com.oltpbenchmark.types.DatabaseType;
import com.oltpbenchmark.util.FileUtil;
import com.oltpbenchmark.util.StringBoxUtil;
import com.oltpbenchmark.util.StringUtil;

import static java.lang.Integer.min;

public class DBWorkload {
  private static final Logger LOG = Logger.getLogger(DBWorkload.class);

  private static final String SINGLE_LINE = StringUtils.repeat("=", 70);

  private static final String RATE_DISABLED = "disabled";
  private static final String RATE_UNLIMITED = "unlimited";

  private static int newOrderTxnId = -1;
  private static int numWarehouses = 10;
  private static int startWarehouseIdForShard = -1;
  private static int totalWarehousesAcrossShards = 10;
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

    // create the command line parser
    CommandLineParser parser = new PosixParser();
    Options options = new Options();
    options.addOption(
            "c",
            "config",
            true,
            "Workload configuration file [default: config/workload_all.xml]");
    options.addOption(
            null,
            "create",
            true,
            "Initialize the database for this benchmark");
    options.addOption(
            null,
            "clear",
            true,
            "Clear all records in the database for this benchmark");
    options.addOption(
            null,
            "load",
            true,
            "Load data using the benchmark's data loader");
    options.addOption(
            null,
            "execute",
            true,
            "Execute the benchmark workload");
    options.addOption(
            null,
            "test",
            true,
            "Test the benchmark procedure execution");
    options.addOption(
            null,
            "runscript",
            true,
            "Run an SQL script");
    options.addOption(
            null,
            "upload",
            true,
            "Upload the result");
    options.addOption(
            null,
            "uploadHash",
            true,
            "git hash to be associated with the upload");

    options.addOption("v", "verbose", false, "Display Messages");
    options.addOption("h", "help", false, "Print this help");
    options.addOption("s", "sample", true, "Sampling window");
    options.addOption("im", "interval-monitor", true,
                      "Throughput Monitoring Interval in milliseconds");
    options.addOption("ss", false, "Verbose Sampling per Transaction");
    options.addOption("o", "output", true, "Output file (default System.out)");
    options.addOption("d", "directory", true,
                      "Base directory for the result files, default is current directory");
    options.addOption("t", "timestamp", false,
                      "Result file is prepended with timestamp for the beginning of the experiment");
    options.addOption("ts", "tracescript", true, "Script of transactions to execute");
    options.addOption(null, "histograms", false, "Print txn histograms");
    options.addOption(null, "output-raw", true, "Output raw data");
    options.addOption(null, "output-samples", true, "Output sample data");

    options.addOption(null, "nodes", true, "comma separated list of nodes (default 127.0.0.1)");
    options.addOption(null, "warehouses", true, "Number of warehouses (default 10)");
    options.addOption(null, "start-warehouse-id", true, "Start warehouse id");
    options.addOption(null, "total-warehouses", true,
                      "Total number of warehouses across all executions");
    options.addOption(null, "loaderthreads", true, "Number of loader threads (default 10)");
    options.addOption(null, "enable-foreign-keys", true, "Whether to enable foregin keys");
    options.addOption(null, "create-sql-procedures", true, "Creates the SQL procedures");

    options.addOption(null, "warmup-time-secs", true, "Warmup time in seconds for the benchmark");
    options.addOption(null, "initial-delay-secs", true,
                      "Delay in seconds for starting the benchmark");
    options.addOption(null, "num-connections", true, "Number of connections used");


    options.addOption(null, "merge-results", true, "Merge results from various output files");
    options.addOption(null, "dir", true, "Directory containing the csv files");

    transactionTypes.put(1, "NewOrder");
    transactionTypes.put(2, "Payment");
    transactionTypes.put(3, "OrderStatus");
    transactionTypes.put(4, "Delivery");
    transactionTypes.put(5, "StockLevel");


    // parse the command line arguments
    CommandLine argsLine = parser.parse(options, args);
    if (argsLine.hasOption("h")) {
      printUsage(options);
      return;
    }

    // Seconds
    int intervalMonitor = 0;
    if (argsLine.hasOption("im")) {
      intervalMonitor = Integer.parseInt(argsLine.getOptionValue("im"));
    }

    String val = argsLine.getOptionValue("nodes");

    List<String> nodes = new ArrayList<>();
    nodes.add("127.0.0.1");

    if (argsLine.hasOption("nodes")) {
      nodes = Arrays.asList(val.split(","));
    }

    if (argsLine.hasOption("warehouses")) {
      numWarehouses = Integer.parseInt(argsLine.getOptionValue("warehouses"));
    }

    if (argsLine.hasOption("start-warehouse-id")) {
      startWarehouseIdForShard = Integer.parseInt(argsLine.getOptionValue("start-warehouse-id"));
    } else {
      startWarehouseIdForShard = 1;
    }

    if (argsLine.hasOption("total-warehouses")) {
      totalWarehousesAcrossShards = Integer.parseInt(argsLine.getOptionValue("total-warehouses"));
    } else {
      totalWarehousesAcrossShards = numWarehouses;
    }

    int loaderThreads = min(10,numWarehouses);
    if (argsLine.hasOption("loaderthreads")) {
      loaderThreads = Integer.parseInt(argsLine.getOptionValue("loaderthreads"));
    }

    // -------------------------------------------------------------------
    // GET PLUGIN LIST
    // -------------------------------------------------------------------

    String targetBenchmarks = "tpcc";

    String[] targetList = targetBenchmarks.split(",");
    List<BenchmarkModule> benchList = new ArrayList<>();

    // Use this list for filtering of the output
    List<TransactionType> activeTXTypes = new ArrayList<>();

    String configFile = "config/workload_all.xml";
    if (argsLine.hasOption("c")) {
      configFile = argsLine.getOptionValue("c");
    }
    XMLConfiguration xmlConfig = new XMLConfiguration(configFile);
    xmlConfig.setExpressionEngine(new XPathExpressionEngine());

    // Load the configuration for each benchmark
    int lastTxnId = 0;
    for (String plugin : targetList) {
      String pluginTest = "[@bench='" + plugin + "']";

      // ----------------------------------------------------------------
      // BEGIN LOADING WORKLOAD CONFIGURATION
      // ----------------------------------------------------------------

      WorkloadConfiguration wrkld = new WorkloadConfiguration();
      wrkld.setBenchmarkName(plugin);
      boolean scriptRun = false;
      if (argsLine.hasOption("t")) {
        scriptRun = true;
        String traceFile = argsLine.getOptionValue("t");
        wrkld.setTraceReader(new TraceReader(traceFile));
        if (LOG.isDebugEnabled()) LOG.debug(wrkld.getTraceReader().toString());
      }

      // Pull in database configuration
      wrkld.setDBDriver(xmlConfig.getString("driver"));

      wrkld.setNodes(nodes);

      wrkld.setDBName(xmlConfig.getString("DBName"));
      wrkld.setDBUsername(xmlConfig.getString("username"));
      wrkld.setDBPassword(xmlConfig.getString("password"));

      if (xmlConfig.containsKey("sslCert") && xmlConfig.getString("sslCert").length() > 0) {
        wrkld.setSslCert(xmlConfig.getString("sslCert"));
      }
      if (xmlConfig.containsKey("sslKey") && xmlConfig.getString("sslKey").length() > 0) {
        wrkld.setSslKey(xmlConfig.getString("sslKey"));
      }

      if (xmlConfig.containsKey("jdbcURL") && xmlConfig.getString("jdbcURL").length() > 0) {
        wrkld.setJdbcURL(xmlConfig.getString("jdbcURL"));
      }

      int terminals = xmlConfig.getInt("terminals[not(@bench)]", numWarehouses * 10);
      terminals = xmlConfig.getInt("terminals" + pluginTest, terminals);
      wrkld.setTerminals(terminals);

      wrkld.setLoaderThreads(loaderThreads);

      String isolationMode = xmlConfig.getString("isolation[not(@bench)]", "TRANSACTION_SERIALIZABLE");
      wrkld.setIsolationMode(xmlConfig.getString("isolation" + pluginTest, isolationMode));

      wrkld.setNumWarehouses(numWarehouses);
      wrkld.setStartWarehouseIdForShard(startWarehouseIdForShard);
      wrkld.setTotalWarehousesAcrossShards(totalWarehousesAcrossShards);

      if (xmlConfig.containsKey("useKeyingTime")) {
        wrkld.setUseKeyingTime(xmlConfig.getBoolean("useKeyingTime"));
      }
      if (xmlConfig.containsKey("useThinkTime")) {
        wrkld.setUseKeyingTime(xmlConfig.getBoolean("useThinkTime"));
      }

      if (xmlConfig.containsKey("enableForeignKeysAfterLoad")) {
        wrkld.setEnableForeignKeysAfterLoad(xmlConfig.getBoolean("enableForeignKeysAfterLoad"));
      }
      if (argsLine.hasOption("start-warehouse-id")) {
          wrkld.setShouldEnableForeignKeys(false);
      }

      if (xmlConfig.containsKey("batchSize")) {
        wrkld.setBatchSize(xmlConfig.getInt("batchSize"));
      }

      if (xmlConfig.containsKey("maxRetriesPerTransaction")) {
        wrkld.setMaxRetriesPerTransaction(xmlConfig.getInt("maxRetriesPerTransaction"));
      }

      if (xmlConfig.containsKey("maxLoaderRetries")) {
        wrkld.setMaxLoaderRetries(xmlConfig.getInt("maxLoaderRetries"));
      }

      if (xmlConfig.containsKey("trackPerSQLStmtLatencies")) {
        InstrumentedPreparedStatement.trackLatencyMetrics(xmlConfig.getBoolean("trackPerSQLStmtLatencies"));
      }

      if (xmlConfig.containsKey("port")) {
        wrkld.setPort(xmlConfig.getInt("port"));
      }

      if (argsLine.hasOption("num-connections")) {
         wrkld.setNumDBConnections(Integer.parseInt(argsLine.getOptionValue("num-connections")));
      } else {
        // We use a max of 200 connections per node so as to not overwhelm the DB cluster.
        wrkld.setNumDBConnections(min(numWarehouses, wrkld.getNodes().size() * 200));
      }

      if (xmlConfig.containsKey("hikariConnectionTimeoutMs")) {
        wrkld.setHikariConnectionTimeout(xmlConfig.getInt("hikariConnectionTimeoutMs"));
      }

      if (wrkld.getNumDBConnections() <= 0) {
        wrkld.setNumDBConnections(wrkld.getTerminals());
      }

      if (wrkld.getNumDBConnections() < wrkld.getLoaderThreads()) {
        wrkld.setNumDBConnections(wrkld.getLoaderThreads());
      }

      if (isBooleanOptionSet(argsLine, "execute") || isBooleanOptionSet(argsLine, "test") ||
          isBooleanOptionSet(argsLine, "clear")) {
        wrkld.setNeedsExecution(true);
      }

      if (xmlConfig.containsKey("useStoredProcedures")) {
        wrkld.setUseStoredProcedures(xmlConfig.getBoolean("useStoredProcedures"));
      }

      if (!argsLine.hasOption("merge-results")) {
        LOG.info("Configuration -> nodes: " + wrkld.getNodes() +
                 ", port: " + wrkld.getPort() +
                 ", startWH: " + wrkld.getStartWarehouseIdForShard() +
                 ", warehouses: " + wrkld.getNumWarehouses() +
                 ", total warehouses across shards: " + wrkld.getTotalWarehousesAcrossShards() +
                 ", terminals: " + wrkld.getTerminals() +
                 ", dbConnections: " + wrkld.getNumDBConnections() +
                 ", loaderThreads: " + wrkld.getLoaderThreads() );
      }

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

      if (!argsLine.hasOption("merge-results")) {
        LOG.info(SINGLE_LINE + "\n\n" + StringUtil.formatMaps(initDebug));
        LOG.info(SINGLE_LINE);
      }

      // ----------------------------------------------------------------
      // LOAD TRANSACTION DESCRIPTIONS
      // ----------------------------------------------------------------
      int numTxnTypes = xmlConfig.configurationsAt("transactiontypes" + pluginTest + "/transactiontype").size();
      if (numTxnTypes == 0 && targetList.length == 1) {
        //if it is a single workload run, <transactiontypes /> w/o attribute is used
        pluginTest = "[not(@bench)]";
        numTxnTypes = xmlConfig.configurationsAt("transactiontypes" + pluginTest + "/transactiontype").size();
      }
      wrkld.setNumTxnTypes(numTxnTypes);

      List<TransactionType> ttypes = new ArrayList<>();
      ttypes.add(TransactionType.INVALID);
      int txnIdOffset = lastTxnId;
      for (int i = 1; i <= wrkld.getNumTxnTypes(); i++) {
        String key = "transactiontypes" + pluginTest + "/transactiontype[" + i + "]";
        String txnName = xmlConfig.getString(key + "/name");

        // Get ID if specified; else increment from last one.
        int txnId = i;
        if (xmlConfig.containsKey(key + "/id")) {
          txnId = xmlConfig.getInt(key + "/id");
        }

        TransactionType tmpType = bench.initTransactionType(txnName, txnId + txnIdOffset);
        if (txnName.equals("NewOrder")) {
          newOrderTxnId = txnId + txnIdOffset;
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

      // Read in the groupings of transactions (if any) defined for this
      // benchmark
      HashMap<String,List<String>> groupings = new HashMap<>();
      int numGroupings = xmlConfig.configurationsAt("transactiontypes" + pluginTest + "/groupings/grouping").size();
      LOG.debug("Num groupings: " + numGroupings);
      for (int i = 1; i < numGroupings + 1; i++) {
        String key = "transactiontypes" + pluginTest + "/groupings/grouping[" + i + "]";

        // Get the name for the grouping and make sure it's valid.
        String groupingName = xmlConfig.getString(key + "/name").toLowerCase();
        if (!groupingName.matches("^[a-z]\\w*$")) {
          LOG.fatal(String.format("Grouping name \"%s\" is invalid."
                    + " Must begin with a letter and contain only"
                    + " alphanumeric characters.", groupingName));
          System.exit(-1);
        }
        else if (groupingName.equals("all")) {
          LOG.fatal("Grouping name \"all\" is reserved."
                    + " Please pick a different name.");
          System.exit(-1);
        }

        // Get the weights for this grouping and make sure that there
        // is an appropriate number of them.
        List<String> groupingWeights = xmlConfig.getList(key + "/weights");
        if (groupingWeights.size() != numTxnTypes) {
          LOG.fatal(String.format("Grouping \"%s\" has %d weights, but there are %d transactions " +
                                  "in this benchmark.", groupingName,
                                  groupingWeights.size(), numTxnTypes));
          System.exit(-1);
        }

        LOG.debug("Creating grouping with name, weights: " + groupingName + ", " + groupingWeights);
        groupings.put(groupingName, groupingWeights);
      }

      // All benchmarks should also have an "all" grouping that gives
      // even weight to all transactions in the benchmark.
      List<String> weightAll = new ArrayList<>();
      for (int i = 0; i < numTxnTypes; ++i)
          weightAll.add("1");
      groupings.put("all", weightAll);
      benchList.add(bench);

      // ----------------------------------------------------------------
      // WORKLOAD CONFIGURATION
      // ----------------------------------------------------------------

      int size = xmlConfig.configurationsAt("/works/work").size();
      for (int i = 1; i < size + 1; i++) {
        SubnodeConfiguration work = xmlConfig.configurationAt("works/work[" + i + "]");
        List<String> weight_strings;

        // use a workaround if there multiple workloads or single
        // attributed workload
        if (targetList.length > 1 || work.containsKey("weights[@bench]")) {
          String weightKey = work.getString("weights" + pluginTest).toLowerCase();
          if (groupings.containsKey(weightKey))
              weight_strings = groupings.get(weightKey);
          else
          weight_strings = getWeights(plugin, work);
        } else {
          String weightKey = work.getString("weights[not(@bench)]").toLowerCase();
          if (groupings.containsKey(weightKey))
              weight_strings = groupings.get(weightKey);
          else
          weight_strings = work.getList("weights[not(@bench)]");
        }
        int rate = 1;
        boolean rateLimited = true;
        boolean disabled = false;

        // can be "disabled", "unlimited" or a number
        String rate_string;
        rate_string = work.getString("rate[not(@bench)]", "");
        rate_string = work.getString("rate" + pluginTest, rate_string);
        if (rate_string.equals(RATE_DISABLED)) {
          disabled = true;
        } else if (rate_string.equals(RATE_UNLIMITED)) {
          rateLimited = false;
        } else if (rate_string.isEmpty()) {
          LOG.fatal(String.format("Please specify the rate for phase %d and workload %s",
                                  i, plugin));
          System.exit(-1);
        } else {
          try {
            rate = Integer.parseInt(rate_string);
            if (rate < 1) {
              LOG.fatal("Rate limit must be at least 1. Use unlimited or disabled values instead.");
              System.exit(-1);
            }
          } catch (NumberFormatException e) {
            LOG.fatal(String.format("Rate string must be '%s', '%s' or a number",
                                    RATE_DISABLED, RATE_UNLIMITED));
            System.exit(-1);
          }
        }
        Phase.Arrival arrival=Phase.Arrival.REGULAR;
        String arrive=work.getString("@arrival","regular");
        if(arrive.equalsIgnoreCase("POISSON"))
          arrival=Phase.Arrival.POISSON;

        // If serial is enabled then run all queries exactly once in serial (rather than
        // random) order
        String serial_string;
        serial_string = work.getString("serial[not(@bench)]", "false");
        serial_string = work.getString("serial" + pluginTest, serial_string);
        if (!Arrays.asList("true", "false").contains(serial_string)){
          LOG.fatal(String.format(
                  "Invalid string for serial: '%s'. Serial string must be 'true' or 'false'",
                  serial_string));
          System.exit(-1);
        }
        // We're not actually serial if we're running a script, so make
        // sure to suppress the serial flag in this case.
        boolean serial = serial_string.equals("true") && (wrkld.getTraceReader() == null);

        int activeTerminals;
        activeTerminals = work.getInt("active_terminals[not(@bench)]", terminals);
        activeTerminals = work.getInt("active_terminals" + pluginTest, activeTerminals);
        if (activeTerminals > terminals) {
          LOG.error(String.format("Configuration error in work %d: "
                + "Number of active terminals is bigger than the total number of terminals", i));
          System.exit(-1);
        }

        time = work.getInt("/time", 0);

        if (argsLine.hasOption("warmup-time-secs")) {
          warmupTime = Integer.parseInt(argsLine.getOptionValue("warmup-time-secs"));
        }

        boolean timed = (time > 0);
        if (scriptRun) {
          LOG.info("Running a script; ignoring timer, serial, and weight settings.");
        }
        else if (!timed) {
          if (serial) {
            if (activeTerminals > 1) {
              // For serial executions, we usually want only one terminal, but not always!
              // (e.g. the CHBenCHmark)
              LOG.warn("\n" + StringBoxUtil.heavyBox(String.format(
                      "WARNING: Serial execution is enabled but the number of active terminals[=%d] > 1.\nIs this intentional??",
                      activeTerminals)));
            }
            LOG.info("Timer disabled for serial run; will execute"
                     + " all queries exactly once.");
          } else {
            LOG.fatal("Must provide positive time bound for"
                      + " non-serial executions. Either provide"
                      + " a valid time or enable serial mode.");
            System.exit(-1);
          }
        }
        else if (serial)
          LOG.info("Timer enabled for serial run; will run queries"
                   + " serially in a loop until the timer expires.");
        if (warmupTime < 0) {
          LOG.fatal("Must provide nonnegative time bound for"
                  + " warmup.");
          System.exit(-1);
        }

        wrkld.addWork(time,
                      warmupTime,
                      rate,
                      weight_strings,
                      rateLimited,
                      disabled,
                      serial,
                      timed,
                      activeTerminals,
                      arrival);
      } // FOR

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

    // Note to self and reviewer -- this was working before, but wasn't super useful..

    if (argsLine.hasOption("initial-delay-secs")) {
      int initialDelay = Integer.parseInt(argsLine.getOptionValue("initial-delay-secs"));
      LOG.info("Delaying execution of workload for " + initialDelay + " seconds");
      Thread.sleep(initialDelay * 1000L);
    }

    // Create the Benchmark's Database
    if (isBooleanOptionSet(argsLine, "create")) {
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
    if (isBooleanOptionSet(argsLine, "clear")) {
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
    if (isBooleanOptionSet(argsLine, "load")) {
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

    if (isBooleanOptionSet(argsLine, "enable-foreign-keys")) {
      for (BenchmarkModule benchmark : benchList) {
        benchmark.enableForeignKeys();
      }
    }

    if (isBooleanOptionSet(argsLine, "create-sql-procedures")) {
      for (BenchmarkModule benchmark : benchList) {
        benchmark.createSqlProcedures();
      }
    }

    // Execute a Script
    if (argsLine.hasOption("runscript")) {
      for (BenchmarkModule benchmark : benchList) {
        String script = argsLine.getOptionValue("runscript");
        LOG.info("Running a SQL script: "+script);
        runScript(benchmark, script);
        LOG.info("Finished!");
        LOG.info(SINGLE_LINE);
      }
    }

    if (isBooleanOptionSet(argsLine, "test")) {
        for (BenchmarkModule benchmark : benchList) {
            benchmark.test();
        }
    }

    // Execute Workload
    if (isBooleanOptionSet(argsLine, "execute")) {
      // Bombs away!
      Results r = null;
      try {
        r = runWorkload(benchList, intervalMonitor, xmlConfig);
      } catch (Throwable ex) {
        LOG.error("Unexpected error when running benchmarks.", ex);
        System.exit(1);
      }

      // WRITE OUTPUT
      writeOutputs(r, activeTXTypes, argsLine);

      // WRITE HISTOGRAMS
      if (argsLine.hasOption("histograms")) {
        writeHistograms(r);
      }
    } else {
      LOG.info("Skipping benchmark workload execution");
    }

    if (isBooleanOptionSet(argsLine, "merge-results")) {
      String dirPath = argsLine.getOptionValue("dir");
      File dir = new File(dirPath);
      String[] files = dir.list();
      mergeResults(dirPath, files);
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
  private static void writeOutputs(Results r, List<TransactionType> activeTXTypes, CommandLine argsLine)
                                   throws Exception {
    // If an output directory is used, store the information
    String outputDirectory = "results";
    if (argsLine.hasOption("d")) {
      outputDirectory = argsLine.getOptionValue("d");
    }
    String filePrefix = "";
    if (argsLine.hasOption("t")) {
      filePrefix = new Timestamp(System.currentTimeMillis()) + "_";
    }

    // Output target
    PrintStream ps = null;
    PrintStream rs = null;
    String baseFileName = "oltpbench";
    if (argsLine.hasOption("o")) {
      if (argsLine.getOptionValue("o").equals("-")) {
        ps = System.out;
        rs = System.out;
        baseFileName = null;
      } else {
        baseFileName = argsLine.getOptionValue("o");
      }
    }

    // Build the complex path
    String baseFile = filePrefix;
    String nextName;

    if (baseFileName != null) {
      // Check if directory needs to be created
      if (outputDirectory.length() > 0) {
        FileUtil.makeDirIfNotExists(outputDirectory.split("/"));
      }

      baseFile = filePrefix + baseFileName;

      if (argsLine.getOptionValue("output-raw", "true").equalsIgnoreCase("true")) {
        // RAW OUTPUT
        nextName = FileUtil.getNextFilename(outputDirectory, baseFile, ".csv");
        rs = new PrintStream(nextName);
        LOG.info("Output Raw data into file: " + nextName);
        r.writeAllCSVAbsoluteTiming(activeTXTypes, rs);
        rs.close();
      }

      if (isBooleanOptionSet(argsLine, "output-samples")) {
        // Write samples using 1 second window
        nextName = FileUtil.getNextFilename(outputDirectory, baseFile, ".samples");
        rs = new PrintStream(nextName);
        LOG.info("Output samples into file: " + nextName);
        r.writeCSV2(rs);
        rs.close();
      }
    } else if (LOG.isDebugEnabled()) {
      LOG.debug("No output file specified");
    }

    // SUMMARY FILE
    if (argsLine.hasOption("s")) {
      nextName = FileUtil.getNextFilename(outputDirectory, baseFile, ".res");
      ps = new PrintStream(nextName);
      LOG.info("Output throughput samples into file: " + nextName);

      int windowSize = Integer.parseInt(argsLine.getOptionValue("s"));
      LOG.info("Grouped into Buckets of " + windowSize + " seconds");
      r.writeCSV(windowSize, ps);

      // Allow more detailed reporting by transaction to make it easier to check
      if (argsLine.hasOption("ss")) {
        for (TransactionType t : activeTXTypes) {
          PrintStream ts = ps;
          if (ts != System.out) {
            // Get the actual filename for the output
            baseFile = filePrefix + baseFileName + "_" + t.getName();
            nextName = FileUtil.getNextFilename(outputDirectory, baseFile, ".res");
            ts = new PrintStream(nextName);
            r.writeCSV(windowSize, ts, t);
            ts.close();
          }
        }
      }
    } else if (LOG.isDebugEnabled()) {
      LOG.warn("No bucket size specified");
    }

    if (ps != null) ps.close();
    if (rs != null) rs.close();
  }

  /* buggy piece of shit of Java XPath implementation made me do it
     replaces good old [@bench="{plugin_name}", which doesn't work in Java XPath with lists
   */
  private static List<String> getWeights(String plugin, SubnodeConfiguration work) {

    List<String> weight_strings = new LinkedList<>();
    @SuppressWarnings("unchecked")
    List<SubnodeConfiguration> weights = work.configurationsAt("weights");
    boolean weights_started = false;

    for (SubnodeConfiguration weight : weights) {
      // stop if second attributed node encountered
      if (weights_started && weight.getRootNode().getAttributeCount() > 0) {
          break;
      }
      // start adding node values, if node with attribute equal to current
      // plugin encountered
      if (weight.getRootNode().getAttributeCount() > 0 &&
          weight.getRootNode().getAttribute(0).getValue().equals(plugin)) {
          weights_started = true;
      }
      if (weights_started) {
          weight_strings.add(weight.getString(""));
      }
    }
    return weight_strings;
  }

  private static void runScript(BenchmarkModule bench, String script) {
    LOG.debug(String.format("Running %s", script));
    bench.runScript(script);
  }

  private static void runCreator(BenchmarkModule bench) {
    LOG.debug(String.format("Creating %s Database", bench));
    bench.createDatabase();
  }

  private static void runLoader(BenchmarkModule bench) {
    LOG.debug(String.format("Loading %s Database", bench));
    bench.loadDatabase();
  }

  private static Results runWorkload(List<BenchmarkModule> benchList,
                                     int intervalMonitor,
                                     XMLConfiguration xmlConfig) {
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

    long numNewOrderTransactions = 0;
    for (Worker w : workers) {
      for (LatencyRecord.Sample sample : w.getLatencyRecords()) {
        if (sample.tranType == newOrderTxnId && sample.startNs + 1000L * sample.latencyUs <= end) {
          ++numNewOrderTransactions;
        }
      }
    }
    LOG.info(SINGLE_LINE);

    double tpmc = 1.0 * numNewOrderTransactions * 60 / time;
    double efficiency = 1.0 * tpmc * 100 / numWarehouses / 12.86;
    DecimalFormat df = new DecimalFormat();
    df.setMaximumFractionDigits(2);

    LOG.info("Throughput: " + r + " reqs/sec");
    LOG.info("Num New Order transactions : " + numNewOrderTransactions + ", time seconds: " + time);
    LOG.info("TPM-C: " + df.format(tpmc));
    LOG.info("Efficiency : " + df.format(efficiency) + "%");

    boolean displayEnhancedLatencyMetrics =
      xmlConfig.containsKey("displayEnhancedLatencyMetrics") &&
      xmlConfig.getBoolean("displayEnhancedLatencyMetrics");
    PrintLatencies(workers, displayEnhancedLatencyMetrics);

    int numTxnTypes = workConfs.get(0).getNumTxnTypes();
    int[] totalRetries = new int[numTxnTypes];
    int[] totalFailures = new int[numTxnTypes];
    for (Worker w : workers) {
      for (int i = 0; i < numTxnTypes; ++i) {
        totalRetries[i] += w.getTotalRetries()[i];
        totalFailures[i] += w.getTotalFailures()[i];
      }
    }
    LOG.info(String.format("Total retries: %s Total failures: %s",
                           Arrays.toString(totalRetries), Arrays.toString(totalFailures)));
    return r;
  }

  private static void PrintLatencies(List<Worker> workers,
                                     boolean displayEnhancedLatencyMetrics) {
    List<List<Integer>> list_latencies = new ArrayList<>();
    List<List<Integer>> list_enhanced_latencies = new ArrayList<>();
    for (int i = 0; i < 5; ++i) {
      list_latencies.add(new ArrayList<>());
      list_enhanced_latencies.add(new ArrayList<>());
    }

    for (Worker w : workers) {
      for (LatencyRecord.Sample sample : w.getLatencyRecords()) {
        list_latencies.get(sample.tranType - 1).add(sample.operationLatencyUs);
      }

      if (displayEnhancedLatencyMetrics) {
        for (LatencyRecord.Sample sample : w.getWholeOperationLatencyRecords()) {
          list_enhanced_latencies.get(sample.tranType - 1).add(sample.operationLatencyUs);
        }
      }
    }

    if (InstrumentedPreparedStatement.isTrackingLatencyMetrics()) {
      NewOrder.printLatencyStats();
      Payment.printLatencyStats();
      OrderStatus.printLatencyStats();
      Delivery.printLatencyStats();
      StockLevel.printLatencyStats();
    }

    if (!displayEnhancedLatencyMetrics) {
      for (int i = 0; i < list_latencies.size(); ++i) {
        LOG.info(getOperationLatencyString(transactionTypes.get(i+1),
                                           list_latencies.get(i)));
      }
      return;
    }

    for (int i = 0; i < list_latencies.size(); ++i) {
      LOG.info(getOperationLatencyString(transactionTypes.get(i+1),
                                         list_latencies.get(i)) +
               getOperationLatencyString(", Whole operation",
                                         list_enhanced_latencies.get(i)));
    }

    List<Integer> acqConnectionLatency = new ArrayList<>();
    for (Worker w : workers) {
      for (LatencyRecord.Sample sample : w.getAcqConnectionLatencyRecords()) {
        acqConnectionLatency.add(sample.operationLatencyUs);
      }
    }
    LOG.info(getOperationLatencyString("Acquire Connection", acqConnectionLatency));
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

  private static void printUsage(Options options) {
    HelpFormatter hlpfrmt = new HelpFormatter();
    hlpfrmt.printHelp("tpccbenchmark", options);
  }

  /**
   * Returns true if the given key is in the CommandLine object and is set to
   * true.
   */
  private static boolean isBooleanOptionSet(CommandLine argsLine, String key) {
    if (argsLine.hasOption(key)) {
      LOG.debug("CommandLine has option '" + key + "'. Checking whether set to true");
      String val = argsLine.getOptionValue(key);
      LOG.debug(String.format("CommandLine %s => %s", key, val));
      return val != null && val.equalsIgnoreCase("true");
    }
    return (false);
  }

  public static String getAssertWarning() {
    String msg = "!!! WARNING !!!\n" +
                 "OLTP-Bench is executing with JVM asserts enabled. This will degrade runtime performance.\n" +
                 "You can disable them by setting the config option 'assertions' to FALSE";
    return StringBoxUtil.heavyBox(msg);
  }
}
