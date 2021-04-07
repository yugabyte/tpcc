package com.oltpbenchmark;

import org.apache.commons.cli.*;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class CommandLineOptions {
    private CommandLine argsLine;

    private static final Options COMMAND_LINE_OPTS = new Options();
    static {
        COMMAND_LINE_OPTS.addOption("h", "help", false, "Print this help");
        COMMAND_LINE_OPTS.addOption(
                null,
                "create",
                true,
                "Initialize the database for this benchmark");
        COMMAND_LINE_OPTS.addOption(
                null,
                "clear",
                true,
                "Clear all records in the database for this benchmark");
        COMMAND_LINE_OPTS.addOption(
                null,
                "load",
                true,
                "Load data using the benchmark's data loader");
        COMMAND_LINE_OPTS.addOption(
                null,
                "execute",
                true,
                "Execute the benchmark workload");
        COMMAND_LINE_OPTS.addOption(
                null,
                "run_one",
                true,
                "Run one instance of the process specified by --proc");


        COMMAND_LINE_OPTS.addOption(
                "c",
                "config",
                true,
                "Workload configuration file [default: config/workload_all.xml]");

        COMMAND_LINE_OPTS.addOption("im", "interval-monitor", true,
                "Throughput Monitoring Interval in milliseconds");
        COMMAND_LINE_OPTS.addOption(null, "histograms", false, "Print txn histograms");
        COMMAND_LINE_OPTS.addOption(null, "output-raw", true, "Output raw data");
        COMMAND_LINE_OPTS.addOption(null, "output-samples", true, "Output sample data");
        COMMAND_LINE_OPTS.addOption(null, "nodes", true, "comma separated list of nodes (default 127.0.0.1)");
        COMMAND_LINE_OPTS.addOption(null, "warehouses", true, "Number of warehouses (default 10)");
        COMMAND_LINE_OPTS.addOption(null, "start-warehouse-id", true, "Start warehouse id");
        COMMAND_LINE_OPTS.addOption(null, "total-warehouses", true,
                "Total number of warehouses across all executions");
        COMMAND_LINE_OPTS.addOption(null, "loaderthreads", true, "Number of loader threads (default 10)");
        COMMAND_LINE_OPTS.addOption(null, "enable-foreign-keys", true, "Whether to enable foregin keys");
        COMMAND_LINE_OPTS.addOption(null, "create-sql-procedures", true, "Creates the SQL procedures");
        COMMAND_LINE_OPTS.addOption(null, "warmup-time-secs", true, "Warmup time in seconds for the benchmark");
        COMMAND_LINE_OPTS.addOption(null, "initial-delay-secs", true,
                "Delay in seconds for starting the benchmark");
        COMMAND_LINE_OPTS.addOption(null, "num-connections", true, "Number of connections used");
        COMMAND_LINE_OPTS.addOption(null, "merge-results", true, "Merge results from various output files");
        COMMAND_LINE_OPTS.addOption(null, "dir", true, "Directory containing the csv files");

        COMMAND_LINE_OPTS.addOption(null, "proc", true, "Process to run one of");

        COMMAND_LINE_OPTS.addOption(null, "run_wid", true, "Warehouse ID for single run");
        COMMAND_LINE_OPTS.addOption(null, "run_did", true, "District ID for single run");
    }

    public CommandLineOptions() {}

    public enum Mode {
        HELP,
        CREATE,
        CLEAR,
        LOAD,
        EXECUTE,
        MERGE_RESULTS,
        RUN_ONE
    }

    void init(String[] args) throws ParseException {
        CommandLineParser parser = new PosixParser();
        argsLine = parser.parse(COMMAND_LINE_OPTS, args);
    }

    private boolean isBooleanOptionSet(String key) {
        if (argsLine.hasOption(key)) {
            String val = argsLine.getOptionValue(key);
            return val != null && val.equalsIgnoreCase("true");
        }
        return false;
    }

    public Mode getMode() {
        if (argsLine.hasOption("h")) return Mode.HELP;
        if (isBooleanOptionSet("clear")) return Mode.CLEAR;
        if (isBooleanOptionSet("create")) return Mode.CREATE;
        if (isBooleanOptionSet("load")) return Mode.LOAD;
        if (isBooleanOptionSet("execute")) return Mode.EXECUTE;
        if (isBooleanOptionSet("run_one")) return Mode.RUN_ONE;
        assert isBooleanOptionSet("merge-results");
        return Mode.MERGE_RESULTS;
    }

    public void printHelp() {
        HelpFormatter hlpfrmt = new HelpFormatter();
        hlpfrmt.printHelp("tpccbenchmark", COMMAND_LINE_OPTS);
    }

    public Optional<String> getStringOpt(String key) {
        if (argsLine.hasOption(key)) {
            return Optional.of(argsLine.getOptionValue(key));
        }
        return Optional.empty();
    }

    public Optional<String> getConfigFile() {
        return getStringOpt("c");
    }

    private Optional<Integer> getIntOpt(String key) {
        if (argsLine.hasOption(key)) {
            return Optional.of(Integer.parseInt(argsLine.getOptionValue(key)));
        }
        return Optional.empty();
    }

    public Optional<Integer> getIntervalMonitor() {
        return getIntOpt("im");
    }

    public Optional<List<String>> getNodes() {
        if (argsLine.hasOption("nodes")) {
            return Optional.of(Arrays.asList(argsLine.getOptionValue("nodes").split(",")));
        }
        return Optional.empty();
    }

    public Optional<Integer> getWarehouses() {
        return getIntOpt("warehouses");
    }

    public Optional<Integer> getStartWarehouseId() {
        return getIntOpt("start-warehouse-id");
    }

    public Optional<Integer> getTotalWarehouses() {
        return getIntOpt("total-warehouses");
    }

    public Optional<Integer> getLoaderThreads() {
        return getIntOpt("loaderthreads");
    }

    public Optional<Integer> getNumDbConnections() {
        return getIntOpt("num-connections");
    }

    public Optional<Integer> getWarmupTime() {
        return getIntOpt("num-connections");
    }

    public Optional<Integer> getInitialDelaySeconds() {
        return getIntOpt("initial-delay-secs");
    }

    public boolean getIsEnableForeignKeysSet() {
        return isBooleanOptionSet("enable-foreign-keys");
    }

    public boolean getIsCreateSqlProceduresSet() {
        return isBooleanOptionSet("create-sql-procedures");
    }

    public Optional<String> getDirPath() {
        return getStringOpt("dir");
    }

    public boolean getIsOutputMetricHistogramsSet() {
        return isBooleanOptionSet("histograms");
    }

    public Optional<String> getRunOneProc() {
        return getStringOpt("proc");
    }

    public Optional<Integer> getRunOneWarehouse() {
        return getIntOpt("run_wid");
    }

    public Optional<Integer> getRunDistrict() {
        return getIntOpt("run_did");
    }
}
