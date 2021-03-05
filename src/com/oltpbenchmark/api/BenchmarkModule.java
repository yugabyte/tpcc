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


package com.oltpbenchmark.api;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.sql.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.oltpbenchmark.benchmarks.tpcc.TPCCConfig;
import com.oltpbenchmark.benchmarks.tpcc.procedures.*;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import org.apache.log4j.Logger;

import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.api.Loader.LoaderThread;
import com.oltpbenchmark.catalog.Catalog;
import com.oltpbenchmark.catalog.Table;
import com.oltpbenchmark.types.DatabaseType;
import com.oltpbenchmark.util.ScriptRunner;
import com.oltpbenchmark.util.ThreadUtil;

/**
 * Base class for all benchmark implementations
 */
public class BenchmarkModule {
    private static final Logger LOG = Logger.getLogger(BenchmarkModule.class);

    /**
     * Each benchmark must put their all of the DBMS-specific DDLs
     * in this directory.
     */
    public static final String DDLS_DIR = "ddls";

    /**
     * The identifier for this benchmark
     */
    protected final String benchmarkName;

    /**
     * The workload configuration for this benchmark invocation
     */
    protected final WorkloadConfiguration workConf;

    /**
     * Database Catalog
     */
    protected final Catalog catalog;

    /**
     * A single Random object that should be re-used by all a benchmark's components
     */
    private final Random rng = new Random();

    public BenchmarkModule(WorkloadConfiguration workConf) {
        assert (workConf != null) : "The WorkloadConfiguration instance is null.";

        this.benchmarkName = "tpcc";
        this.workConf = workConf;
        this.catalog = new Catalog(this);
        if (workConf.getNeedsExecution()) {
            try {
                createDataSource();
            } catch (Exception e) {
                LOG.error("Failed to create Data source", e);
                throw e;
            }
        }
    }

    private final List<HikariDataSource> listDataSource = new ArrayList<>();

    public void createDataSource() {
        int numConnections =
            (workConf.getNumDBConnections() + workConf.getNodes().size() - 1) / workConf.getNodes().size();
        for (String ip : workConf.getNodes()) {
            // Sleep for some time so as to allow the postgres process to cache the system information.
            if (listDataSource.size() != 0) {
                ThreadUtil.sleep(5000);
            }
            Properties props = new Properties();
            props.setProperty("dataSourceClassName", "org.postgresql.ds.PGSimpleDataSource");
            props.setProperty("dataSource.serverName", ip);
            props.setProperty("dataSource.portNumber", Integer.toString(workConf.getPort()));
            props.setProperty("dataSource.user", workConf.getDBUsername());
            props.setProperty("dataSource.password", workConf.getDBPassword());
            props.setProperty("dataSource.databaseName", workConf.getDBName());
            props.setProperty("maximumPoolSize", Integer.toString(numConnections));
            props.setProperty("connectionTimeout", Integer.toString(workConf.getHikariConnectionTimeout()));
            props.setProperty("dataSource.reWriteBatchedInserts", "true");
            if (workConf.getSslCert() != null && workConf.getSslCert().length() > 0) {
              assert(workConf.getSslKey().length() > 0) : "The SSL key is empty.";
              props.put("dataSource.sslmode", "require");
              props.put("dataSource.sslcert", workConf.getSslCert());
              props.put("dataSource.sslkey", workConf.getSslKey());
            }
            HikariConfig config = new HikariConfig(props);
            if (workConf.getJdbcURL() != null && workConf.getJdbcURL().length()>0) {
              config.setJdbcUrl(workConf.getJdbcURL());
            }
            listDataSource.add(new HikariDataSource(config));
        }
    }

    public final Connection makeConnection() throws SQLException {
        java.util.Properties props = new java.util.Properties();
        props.put("user", workConf.getDBUsername());
        props.put("password", workConf.getDBPassword());
        props.put("reWriteBatchedInserts", "true");
        if (workConf.getSslCert() != null && workConf.getSslCert().length() > 0) {
          assert(workConf.getSslKey().length() > 0) : "The SSL key is empty.";
          props.put("sslmode", "require");
          props.put("sslcert", workConf.getSslCert());
          props.put("sslkey", workConf.getSslKey());
        }

        int r = dataSourceCounter.getAndIncrement() % workConf.getNodes().size();
        String connectStr;
        if (workConf.getJdbcURL() != null && workConf.getJdbcURL().length()>0) {
            connectStr=workConf.getJdbcURL();
        } else {
            connectStr = String.format("jdbc:postgresql://%s:%d/%s",
                workConf.getNodes().get(r),
                workConf.getPort(),
                workConf.getDBName());
        }
        return DriverManager.getConnection(connectStr, props);
    }

    private static final AtomicInteger dataSourceCounter = new AtomicInteger(0);
    public final HikariDataSource getDataSource() {
        int r = dataSourceCounter.getAndIncrement() % workConf.getNodes().size();
        return listDataSource.get(r);
    }

    // --------------------------------------------------------------------------
    // IMPLEMENTING CLASS INTERFACE
    // --------------------------------------------------------------------------

    protected List<Worker> makeWorkersImpl() {
      ArrayList<Worker> workers = new ArrayList<>();
      try {
        List<Worker> terminals = createTerminals();
        workers.addAll(terminals);
      } catch (Exception e) {
        e.printStackTrace();
      }

      return workers;
    }

    protected Loader makeLoaderImpl() {
      return new Loader(this);
    }

    protected Package getProcedurePackageImpl() {
      return (NewOrder.class.getPackage());
    }

    // --------------------------------------------------------------------------
    // PUBLIC INTERFACE
    // --------------------------------------------------------------------------

    /**
     * Return the Random generator that should be used by all this benchmark's components
     */
    public Random rng() {
        return (this.rng);
    }

    /**
     * Return the URL handle to the DDL used to load the benchmark's database
     * schema.
     */
    public URL getDatabaseDDL(DatabaseType db_type) {
        String[] ddlNames = {
            this.benchmarkName + "-" + (db_type != null ? db_type.name().toLowerCase() : "") + "-ddl.sql",
            this.benchmarkName + "-ddl.sql",
        };

        for (String ddlName : ddlNames) {
            URL ddlURL = this.getClass().getResource(DDLS_DIR + File.separator + ddlName);
            if (ddlURL != null) {
                if (LOG.isDebugEnabled())
                    LOG.debug("Found DDL file for " + db_type + ": " + ddlURL );
                return ddlURL;
            }
        } // FOR
        LOG.trace(ddlNames[0]+" :or: "+ddlNames[1]);
        LOG.error("Failed to find DDL file for " + this.benchmarkName);
        return null;
    }

    public final List<Worker> makeWorkers() {
        return (this.makeWorkersImpl());
    }

    /**
     * Create the Benchmark Database
     * This is the main method used to create all the database
     * objects (e.g., table, indexes, etc) needed for this benchmark
     */
    public final void createDatabase() {
        try {
            Connection conn = makeConnection();
            this.createDatabase(this.workConf.getDBType(), conn);
            conn.close();
        } catch (SQLException ex) {
            throw new RuntimeException(String.format("Unexpected error when trying to create the %s database", this.benchmarkName), ex);
        }
    }

    /**
     * Create the Benchmark Database
     * This is the main method used to create all the database
     * objects (e.g., table, indexes, etc) needed for this benchmark
     */
    public final void createDatabase(DatabaseType dbType, Connection conn) {
        try {
            URL ddl = this.getDatabaseDDL(dbType);
            assert(ddl != null) : "Failed to get DDL for " + this;
            ScriptRunner runner = new ScriptRunner(conn, true, true);
            if (LOG.isDebugEnabled()) LOG.debug("Executing script '" + ddl + "'");
            runner.runScript(ddl);
        } catch (Exception ex) {
            throw new RuntimeException(String.format("Unexpected error when trying to create the %s database", this.benchmarkName), ex);
        }
    }

    /**
     * Run a script on a Database
     */
    public final void runScript(String script) {
        try {
            Connection conn = listDataSource.get(0).getConnection();
            ScriptRunner runner = new ScriptRunner(conn, true, true);
            File scriptFile= new File(script);
            runner.runScript(scriptFile.toURI().toURL());
            conn.close();
        } catch (SQLException ex) {
            throw new RuntimeException(String.format("Unexpected error when trying to run: %s", script), ex);
        } catch (IOException ex) {
            throw new RuntimeException(String.format("Unexpected error when trying to open: %s", script), ex);
        }
    }

    /**
     * Invoke this benchmark's database loader.
     * We return the handle to Loader object that we created to do this.
     * You probably don't need it and can simply ignore. There are some
     * test cases that use it. That's why it's here.
     */
    public final void loadDatabase() {
        Loader loader = this.makeLoaderImpl();
        if (loader != null) {
            List<? extends LoaderThread> loaderThreads = loader.createLoaderThreads();
            if (loaderThreads != null) {
                int maxConcurrent = workConf.getLoaderThreads();
                assert (maxConcurrent > 0);
                if (LOG.isDebugEnabled())
                    LOG.debug(String.format("Starting %d %s.LoaderThreads [maxConcurrent=%d]",
                            loaderThreads.size(),
                            loader.getClass().getSimpleName(),
                            maxConcurrent));
                ThreadUtil.runNewPool(loaderThreads, maxConcurrent);

                if (!loader.getTableCounts().isEmpty()) {
                    LOG.info("Table Counts:\n" + loader.getTableCounts());
                }
            }
        }
        if (LOG.isDebugEnabled())
            LOG.debug(String.format("Finished loading the %s database",
                                    this.getBenchmarkName().toUpperCase()));
    }

    public final void clearDatabase() {
        try {
            Loader loader = this.makeLoaderImpl();
            if (loader != null) {
                Connection conn = listDataSource.get(0).getConnection();
                conn.setAutoCommit(false);
                loader.unload(conn);
                conn.commit();
            }
        } catch (SQLException ex) {
            throw new RuntimeException(String.format("Unexpected error when trying to delete the %s database", this.benchmarkName), ex);
        }
    }

    // --------------------------------------------------------------------------
    // UTILITY METHODS
    // --------------------------------------------------------------------------

    /**
     * Return the unique identifier for this benchmark
     */
    public final String getBenchmarkName() {
        return (this.benchmarkName);
    }
    /**
     * Return the database's catalog
     */
    public final Catalog getCatalog() {
        return (this.catalog);
    }
    /**
     * Get the catalog object for the given table name
     */
    public Table getTableCatalog(String tableName) {
        Table catalog_tbl = this.catalog.getTable(tableName.toUpperCase());
        assert (catalog_tbl != null) : "Invalid table name '" + tableName + "'";
        return (catalog_tbl);
    }

    @Override
    public final String toString() {
        return benchmarkName.toUpperCase();
    }


    /**
     * Initialize a TransactionType handle for the get procedure name and id
     * This should only be invoked a start-up time
     */
    public final TransactionType initTransactionType(String procName, int id) {
        if (id == TransactionType.INVALID_ID) {
            LOG.error(String.format("Procedure %s.%s cannot use the reserved id '%d' for %s",
                    this.benchmarkName, procName, id,
                    TransactionType.INVALID.getClass().getSimpleName()));
            return null;
        }

        Map<String, Class<? extends Procedure>> txnClasses = Stream
            .of(Delivery.class, NewOrder.class, OrderStatus.class, Payment.class, StockLevel.class)
            .collect(Collectors.toMap(Class::getCanonicalName, clazz -> clazz));

        Package pkg = this.getProcedurePackageImpl();
        assert (pkg != null) : "Null Procedure package for " + this.benchmarkName;
        String fullName = pkg.getName() + "." + procName;

        Class<? extends Procedure> procClass = txnClasses.get(fullName);
        assert (procClass != null) : "Unexpected Procedure name " + this.benchmarkName + "." + procName;
        // TODO -- just use factory pattern instead of reflection..
        return new TransactionType(procClass, id);
    }

    public final WorkloadConfiguration getWorkloadConfiguration() {
        return (this.workConf);
    }

    /**
     * Return a mapping from TransactionTypes to Procedure invocations
     */
    public Map<TransactionType, Procedure> getProcedures() {
        Map<TransactionType, Procedure> proc_xref = new HashMap<>();
        TransactionTypes txns = this.workConf.getTransTypes();

        if (txns != null) {
            for (TransactionType txn : txns) {
                Procedure proc = txn.getInstance();
                proc.initialize();
                proc_xref.put(txn, proc);
            } // FOR
        }
        if (proc_xref.isEmpty()) {
            LOG.warn("No procedures defined for " + this);
        }
        return (proc_xref);
    }

    protected ArrayList<Worker> createTerminals() {

      // The array 'terminals' contains a terminal associated to a {warehouse, district}.
      Worker[] terminals = new Worker[workConf.getTerminals()];

      int numWarehouses = workConf.getNumWarehouses();
      if (numWarehouses <= 0) {
        numWarehouses = 1;
      }
      int numTerminals = workConf.getTerminals();
      assert (numTerminals >= numWarehouses) :
        String.format("Insufficient number of terminals '%d' [numWarehouses=%d]",
                      numTerminals, numWarehouses);

      // TODO: This is currently broken: fix it!
      int warehouseOffset = Integer.getInteger("warehouseOffset", 1);
      assert warehouseOffset == 1;

      // We distribute terminals evenly across the warehouses
      // Eg. if there are 10 terminals across 7 warehouses, they
      // are distributed as
      // 1, 1, 2, 1, 2, 1, 2
      final double terminalsPerWarehouse = (double) numTerminals
          / numWarehouses;
      int workerId = 0;
      assert terminalsPerWarehouse >= 1;
      int k = 0;
      for (int w = workConf.getStartWarehouseIdForShard() - 1;
           w < numWarehouses + workConf.getStartWarehouseIdForShard() - 1;
           w++) {
        // Compute the number of terminals in *this* warehouse
        int lowerTerminalId = (int) (w * terminalsPerWarehouse);
        int upperTerminalId = (int) ((w + 1) * terminalsPerWarehouse);
        // protect against double rounding errors
        int w_id = w + 1;
        if (w_id == numWarehouses)
          upperTerminalId = numTerminals;
        int numWarehouseTerminals = upperTerminalId - lowerTerminalId;

        if (BenchmarkModule.LOG.isDebugEnabled())
          BenchmarkModule.LOG.debug(String.format("w_id %d = %d terminals [lower=%d / upper%d]",
                                  w_id, numWarehouseTerminals, lowerTerminalId, upperTerminalId));

        final double districtsPerTerminal =
          TPCCConfig.configDistPerWhse / (double) numWarehouseTerminals;
        assert districtsPerTerminal >= 1 :
          String.format("Too many terminals [districtsPerTerminal=%.2f, numWarehouseTerminals=%d]",
                        districtsPerTerminal, numWarehouseTerminals);
        for (int terminalId = 0; terminalId < numWarehouseTerminals; terminalId++) {
          int lowerDistrictId = (int) (terminalId * districtsPerTerminal);
          int upperDistrictId = (int) ((terminalId + 1) * districtsPerTerminal);
          if (terminalId + 1 == numWarehouseTerminals) {
            upperDistrictId = TPCCConfig.configDistPerWhse;
          }
          lowerDistrictId += 1;

          Worker terminal = new Worker(this, workerId++,
                                               w_id, lowerDistrictId, upperDistrictId);
          terminals[k++] = terminal;
        }
      }
      assert terminals[terminals.length - 1] != null;

      ArrayList<Worker> ret = new ArrayList<>();
      Collections.addAll(ret, terminals);
      return ret;
    }

    /**
       * Hack to support postgres-specific timestamps
       * @param time - millis since epoch
       * @return Timestamp
       */
      public Timestamp getTimestamp(long time) {
        Timestamp timestamp;

        // HACK: NoisePage doesn't support JDBC timestamps.
        // We have to use the postgres-specific type
        if (this.workConf.getDBType() == DatabaseType.NOISEPAGE) {
          timestamp = new org.postgresql.util.PGTimestamp(time);
        } else {
          timestamp = new Timestamp(time);
        }
        return (timestamp);
      }

    public void enableForeignKeys() throws Exception {
        Loader loader = new Loader(this);
        loader.EnableForeignKeyConstraints(makeConnection());
      }

    // This function creates SQL procedures that the execution would need. Currently we have procedures to update the
      // Stock table, and a procedure to get stock levels of items recently ordered.
      public void createSqlProcedures() throws Exception {
        try {
          Connection conn = makeConnection();
          Statement st = conn.createStatement();

          StringBuilder argsSb = new StringBuilder();
          StringBuilder updateStatements = new StringBuilder();

          argsSb.append("wid int");
          for (int i = 1; i <= 15; ++i) {
            argsSb.append(String.format(", i%d int, q%d int, y%d int, r%d int", i, i, i, i));
            updateStatements.append(String.format(
              "UPDATE STOCK SET S_QUANTITY = q%d, S_YTD = y%d, S_ORDER_CNT = S_ORDER_CNT + 1, " +
              "S_REMOTE_CNT = r%d WHERE S_W_ID = wid AND S_I_ID = i%d;",
              i, i, i, i));
            String updateStmt =
              String.format("CREATE PROCEDURE updatestock%d (%s) AS '%s' LANGUAGE SQL;",
                            i, argsSb.toString(), updateStatements.toString());

            st.execute(String.format("DROP PROCEDURE IF EXISTS updatestock%d", i));
            st.execute(updateStmt);
          }

          StockLevel.InitializeGetStockCountProc(conn);
        } catch (SQLException se) {
          BenchmarkModule.LOG.error(se.getMessage());
          throw se;
        }
      }

    public void test() throws Exception {
        Worker worker = new Worker(this, 1 /* worker_id */, 1, 1, 1);
        worker.test(makeConnection());
      }
}
