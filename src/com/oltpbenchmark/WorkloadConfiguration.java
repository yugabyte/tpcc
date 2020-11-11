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

import com.oltpbenchmark.api.TransactionTypes;
import com.oltpbenchmark.types.DatabaseType;
import com.oltpbenchmark.util.StringUtil;
import com.oltpbenchmark.util.ThreadUtil;
import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.commons.configuration.XMLConfiguration;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class WorkloadConfiguration {

  private DatabaseType db_type;
  private String benchmarkName;

  public String getBenchmarkName() {
    return benchmarkName;
  }

  public void setBenchmarkName(String benchmarkName) {
    this.benchmarkName = benchmarkName;
  }

  private List<String> nodes;
  private String db_name;
  private String db_username;
  private String db_password;
  private String db_driver;
  private int numWarehouses = -1;
  private int startWarehouseIdForShard = -1;
  private int totalWarehousesAcrossShards = -1;
  private double selectivity = -1.0;
  private int terminals;
  private int numDBConnections = -1;
  private int port = 5433;
  private int loaderThreads = ThreadUtil.availableProcessors();
  private int numTxnTypes;
  private TraceReader traceReader = null;
  private boolean useKeyingTime = true;
  private boolean useThinkTime = true;
  private boolean enableForeignKeysAfterLoad = true;
  private boolean shouldEnableForeignKeys = true;
  private boolean createSQLProcedures = true;
  private int batchSize = 128;
  private int hikariConnectionTimeout = 60000;
  private boolean needsExecution = false;
  private boolean useStoredProcedures = true;

  public TraceReader getTraceReader() {
    return traceReader;
  }

  public void setTraceReader(TraceReader traceReader) {
    this.traceReader = traceReader;
  }

  private XMLConfiguration xmlConfig = null;

  private final List<Phase> works = new ArrayList<Phase>();
  private WorkloadState workloadState;

  public WorkloadState getWorkloadState() {
    return workloadState;
  }

  /**
   * Initiate a new benchmark and workload state
   */
  public WorkloadState initializeState(BenchmarkState benchmarkState) {
    assert (workloadState == null);
    workloadState = new WorkloadState(benchmarkState, works, terminals, traceReader);
    return workloadState;
  }

  private int numberOfPhases = 0;
  private TransactionTypes transTypes = null;
  private int isolationMode = Connection.TRANSACTION_SERIALIZABLE;
  private boolean recordAbortMessages = false;
  private String dataDir = null;

  public void addWork(int time, int warmup, int rate, List<String> weights, boolean rateLimited,
                      boolean disabled, boolean serial, boolean timed, int active_terminals,
                      Phase.Arrival arrival) {
    works.add(new Phase(benchmarkName, numberOfPhases, time, warmup, rate, weights, rateLimited,
                        disabled, serial, timed, active_terminals, arrival));
    numberOfPhases++;
  }

  public void setDBType(DatabaseType dbType) {
    db_type = dbType;
  }

  public DatabaseType getDBType() {
    return db_type;
  }

  public void setNodes(List<String> nodes) {
    this.nodes = nodes;
  }

  public List<String> getNodes() {
    return nodes;
  }

  public void setDBName(String dbname) {
    this.db_name = dbname;
  }

  public void setLoaderThreads(int loaderThreads) {
    this.loaderThreads = loaderThreads;
  }

  /**
   * The number of loader threads that the framework is allowed to use.
   *
   * @return
   */
  public int getLoaderThreads() {
    return this.loaderThreads;
  }

  public int getNumTxnTypes() {
    return numTxnTypes;
  }

  public void setNumTxnTypes(int numTxnTypes) {
    this.numTxnTypes = numTxnTypes;
  }

  public String getDBName() {
    return db_name;
  }

  public void setDBUsername(String username) {
    this.db_username = username;
  }

  public String getDBUsername() {
    return db_username;
  }

  public void setDBPassword(String password) {
    this.db_password = password;
  }

  public String getDBPassword() {
    return this.db_password;
  }

  public void setSelectivity(double selectivity) {
    this.selectivity = selectivity;
  }

  public double getSelectivity() {
    return this.selectivity;
  }

  public void setDBDriver(String driver) {
    this.db_driver = driver;
  }

  public String getDBDriver() {
    return this.db_driver;
  }

  public void setRecordAbortMessages(boolean recordAbortMessages) {
    this.recordAbortMessages = recordAbortMessages;
  }

  /**
   * Whether each worker should record the transaction's UserAbort messages
   * This primarily useful for debugging a benchmark
   */
  public boolean getRecordAbortMessages() {
    return (this.recordAbortMessages);
  }

  public void setNumWarehouses(int warehouses) {
    this.numWarehouses = warehouses;
  }
  public int getNumWarehouses() {
    return this.numWarehouses;
  }

  /**
   * Return the number of phases specified in the config file
   *
   * @return
   */
  public int getNumberOfPhases() {
    return this.numberOfPhases;
  }

  /**
   * Set the directory in which we can find the data files (for example, CSV
   * files) for loading the database.
   */
  public void setDataDir(String dir) {
    this.dataDir = dir;
  }

  /**
   * Return the directory in which we can find the data files (for example, CSV
   * files) for loading the database.
   */
  public String getDataDir() {
    return this.dataDir;
  }

  /**
   * A utility method that init the phaseIterator and dialectMap
   */
  public void init() {
    try {
      Class.forName(this.db_driver);
    } catch (ClassNotFoundException ex) {
      throw new RuntimeException("Failed to initialize JDBC driver '" + this.db_driver + "'", ex);
    }
  }

  public void setTerminals(int terminals) {
    this.terminals = terminals;
  }

  public int getTerminals() {
    return terminals;
  }

  public void setNumDBConnections(int numDBConnections) {
    this.numDBConnections = numDBConnections;
  }

  public void setStartWarehouseIdForShard(int startWarehoue) {
    this.startWarehouseIdForShard = startWarehoue;
  }
  public int getStartWarehouseIdForShard() {
    return this.startWarehouseIdForShard;
  }

  public void setTotalWarehousesAcrossShards(int totalWarehousesAcrossExecutions) {
    this.totalWarehousesAcrossShards = totalWarehousesAcrossExecutions;
  }
  public int getTotalWarehousesAcrossShards() {
    return this.totalWarehousesAcrossShards;
  }

  public int getNumDBConnections() {
    return this.numDBConnections;
  }

  public void setHikariConnectionTimeout(int hikariConnectionTimeout) {
    this.hikariConnectionTimeout = hikariConnectionTimeout;
  }

  public int getHikariConnectionTimeout() {
    return this.hikariConnectionTimeout;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public int getPort() {
    return port;
  }

  public TransactionTypes getTransTypes() {
    return transTypes;
  }

  public void setTransTypes(TransactionTypes transTypes) {
    this.transTypes = transTypes;
  }

  public List<Phase> getAllPhases() {
    return works;
  }

  public void setXmlConfig(XMLConfiguration xmlConfig) {
    this.xmlConfig = xmlConfig;
  }

  public XMLConfiguration getXmlConfig() {
    return xmlConfig;
  }

  public int getIsolationMode() {
    return isolationMode;
  }

  public String getIsolationString() {
    if (this.isolationMode == Connection.TRANSACTION_SERIALIZABLE)
      return "TRANSACTION_SERIALIZABLE";
    else if (this.isolationMode == Connection.TRANSACTION_READ_COMMITTED)
      return "TRANSACTION_READ_COMMITTED";
    else if (this.isolationMode == Connection.TRANSACTION_REPEATABLE_READ)
      return "TRANSACTION_REPEATABLE_READ";
    else if (this.isolationMode == Connection.TRANSACTION_READ_UNCOMMITTED)
      return "TRANSACTION_READ_UNCOMMITTED";
    else
      return "TRANSACTION_SERIALIZABLE [DEFAULT]";
  }

  public void setIsolationMode(String mode) {
    if (mode.equals("TRANSACTION_SERIALIZABLE"))
      this.isolationMode = Connection.TRANSACTION_SERIALIZABLE;
    else if (mode.equals("TRANSACTION_READ_COMMITTED"))
      this.isolationMode = Connection.TRANSACTION_READ_COMMITTED;
    else if (mode.equals("TRANSACTION_REPEATABLE_READ"))
      this.isolationMode = Connection.TRANSACTION_REPEATABLE_READ;
    else if (mode.equals("TRANSACTION_READ_UNCOMMITTED"))
      this.isolationMode = Connection.TRANSACTION_READ_UNCOMMITTED;
    else if (!mode.isEmpty())
      System.out.println("Indefined isolation mode, set to default [TRANSACTION_SERIALIZABLE]");
  }

  public boolean getUseKeyingTime() {
    return useKeyingTime;
  }

  public void setUseKeyingTime(boolean useKeyingTime) {
    this.useKeyingTime = useKeyingTime;
  }

  public boolean getUseThinkTime() {
    return useThinkTime;
  }

  public void setUseThinkTime(boolean useThinkTime) {
    this.useThinkTime = useThinkTime;
  }

  public boolean getEnableForeignKeysAfterLoad() {
    return enableForeignKeysAfterLoad;
  }

  public void setEnableForeignKeysAfterLoad(boolean enableForeignKeysAfterLoad) {
    this.enableForeignKeysAfterLoad = enableForeignKeysAfterLoad;
  }

  public boolean getShouldEnableForeignKeys() {
    return this.shouldEnableForeignKeys;
  }
  public void setShouldEnableForeignKeys(boolean shouldEnableForeignKeys) {
    this.shouldEnableForeignKeys = shouldEnableForeignKeys;
  }

  public boolean getCreateSQLProcedures() {
    return createSQLProcedures;
  }
  public void setCreateSQLProcedures(boolean createSQLProcedures) {
    this.createSQLProcedures = createSQLProcedures;
  }

  public int getBatchSize() {
    return batchSize;
  }

  public void setBatchSize(int batchSize) {
    this.batchSize = batchSize;
  }

  public void setNeedsExecution(boolean needsExecution) { this.needsExecution = needsExecution; }
  public boolean getNeedsExecution() { return this.needsExecution; }

  public void setUseStoredProcedures(boolean useStoredProcedures) {
    this.useStoredProcedures = useStoredProcedures;
  }
  public boolean getUseStoredProcedures() { return this.useStoredProcedures; }

  @Override
  public String toString() {
    Class<?> confClass = this.getClass();
    Map<String, Object> m = new ListOrderedMap<String, Object>();
    for (Field f : confClass.getDeclaredFields()) {
      Object obj = null;
      try {
        obj = f.get(this);
      } catch (IllegalAccessException ex) {
        throw new RuntimeException(ex);
      }
      m.put(f.getName().toUpperCase(), obj);
    } // FOR
    return StringUtil.formatMaps(m);
  }
}
