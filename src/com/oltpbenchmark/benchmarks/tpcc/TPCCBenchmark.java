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


package com.oltpbenchmark.benchmarks.tpcc;

import java.sql.Connection;
import java.sql.Statement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.oltpbenchmark.benchmarks.tpcc.procedures.StockLevel;
import org.apache.log4j.Logger;

import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.tpcc.procedures.NewOrder;
import com.oltpbenchmark.types.DatabaseType;

public class TPCCBenchmark extends BenchmarkModule {
  private static final Logger LOG = Logger.getLogger(TPCCBenchmark.class);

  public TPCCBenchmark(WorkloadConfiguration workConf) {
    super("tpcc", workConf);
  }

  @Override
  protected Package getProcedurePackageImpl() {
    return (NewOrder.class.getPackage());
  }

  @Override
  protected List<Worker<? extends BenchmarkModule>> makeWorkersImpl() {

    ArrayList<Worker<? extends BenchmarkModule>> workers = new ArrayList<>();
    try {
      List<TPCCWorker> terminals = createTerminals();
      workers.addAll(terminals);
    } catch (Exception e) {
      e.printStackTrace();
    }

    return workers;
  }

  @Override
  protected Loader<TPCCBenchmark> makeLoaderImpl() {
    return new TPCCLoader(this);
  }

  protected ArrayList<TPCCWorker> createTerminals() {

    // The array 'terminals' contains a terminal associated to a {warehouse, district}.
    TPCCWorker[] terminals = new TPCCWorker[workConf.getTerminals()];

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

      if (LOG.isDebugEnabled())
        LOG.debug(String.format("w_id %d = %d terminals [lower=%d / upper%d]",
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

        TPCCWorker terminal = new TPCCWorker(this, workerId++,
                                             w_id, lowerDistrictId, upperDistrictId);
        terminals[k++] = terminal;
      }
    }
    assert terminals[terminals.length - 1] != null;

    ArrayList<TPCCWorker> ret = new ArrayList<>();
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
        timestamp = new java.sql.Timestamp(time);
      }
      return (timestamp);
    }

    public void enableForeignKeys() throws Exception {
      TPCCLoader loader = new TPCCLoader(this);
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
        LOG.error(se.getMessage());
        throw se;
      }
    }

    public void test() throws Exception {
      TPCCWorker worker = new TPCCWorker(this, 1 /* worker_id */, 1, 1, 1);
      worker.test(makeConnection());
    }
}
