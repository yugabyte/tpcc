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

package com.oltpbenchmark.benchmarks.tpcc.procedures;

import java.sql.*;
import java.util.Random;

import com.oltpbenchmark.api.InstrumentedSQLStmt;
import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.jdbc.InstrumentedPreparedStatement;

import org.HdrHistogram.ConcurrentHistogram;
import org.HdrHistogram.Histogram;
import org.apache.log4j.Logger;

import com.oltpbenchmark.benchmarks.tpcc.TPCCConstants;
import com.oltpbenchmark.benchmarks.tpcc.TPCCUtil;

public class StockLevel extends Procedure {

  private static final Logger LOG = Logger.getLogger(StockLevel.class);

  public static InstrumentedSQLStmt stockGetDistOrderIdSQL = new InstrumentedSQLStmt(
      "SELECT D_NEXT_O_ID " +
      "  FROM " + TPCCConstants.TABLENAME_DISTRICT +
      " WHERE D_W_ID = ? " +
      "   AND D_ID = ?");

  private static final String GET_STOCK_COUNTS_PROC_NAME = "getstockcounts";

  public static InstrumentedSQLStmt stockGetCountStockSQL = new InstrumentedSQLStmt(
      String.format("{ ? = call %s( ?, ?, ?, ? ) }", GET_STOCK_COUNTS_PROC_NAME));

  public static void InitializeGetStockCountProc(Connection conn) throws SQLException {
    Statement stmt = conn.createStatement();
    stmt.execute(String.format(
        "CREATE OR REPLACE" +
        " FUNCTION %s(warehouse INT, district INT, min_o_id INT, max_o_id INT, max_quantity INT)" +
        " RETURNS integer AS $$" +
        " DECLARE" +
        " item_ids integer[];" +
        " result integer;" +
        " BEGIN" +
        " SELECT ARRAY(SELECT DISTINCT(OL_I_ID)" +
        " FROM ORDER_LINE" +
        " WHERE OL_W_ID = $1 and OL_D_ID = $2 and OL_O_ID >= $3 and OL_O_ID < $4) INTO item_ids;" +
        " SELECT COUNT(S_I_ID) INTO result" +
        " FROM STOCK WHERE S_W_ID = $1" +
        " AND S_I_ID = ANY(item_ids) AND S_QUANTITY < $5;" +
        " RETURN result;" +
        " END; $$ LANGUAGE plpgsql", GET_STOCK_COUNTS_PROC_NAME));
  }

  // Stock Level Txn
  private InstrumentedPreparedStatement stockGetDistOrderId = null;

  private static Histogram stockGetCountStockFuncLatency = new ConcurrentHistogram(InstrumentedSQLStmt.numSigDigits);
  private InstrumentedPreparedStatement stockGetCountStockFunc = null;

  public static void printLatencyStats() {
    LOG.info("StockLevel : ");
    LOG.info("latency GetDistOrderId " + stockGetDistOrderIdSQL.getStats());
    LOG.info("latency GetCountStock " + stockGetCountStockSQL.getStats());
  }

  public ResultSet run(Connection conn, Random gen,
                  int w_id, int numWarehouses,
                  int terminalDistrictLowerID, int terminalDistrictUpperID,
                  Worker w) throws SQLException {
    boolean trace = LOG.isTraceEnabled();
    stockGetDistOrderId = this.getPreparedStatement(conn, stockGetDistOrderIdSQL);
    stockGetCountStockFunc = new InstrumentedPreparedStatement(conn.prepareCall(stockGetCountStockSQL.getSqlStmt().getSQL()),
                                                               stockGetCountStockFuncLatency);


    int threshold = TPCCUtil.randomNumber(10, 20, gen);
    int d_id = TPCCUtil.randomNumber(terminalDistrictLowerID,terminalDistrictUpperID, gen);

    int stock_count = 0;
    stockGetDistOrderId.setInt(1, w_id);
    stockGetDistOrderId.setInt(2, d_id);
    if (trace)
      LOG.trace(String.format("stockGetDistOrderId BEGIN [W_ID=%d, D_ID=%d]", w_id, d_id));
    ResultSet rs = stockGetDistOrderId.executeQuery();
    if (trace) LOG.trace("stockGetDistOrderId END");

    if (!rs.next()) {
      throw new RuntimeException("D_W_ID="+ w_id +" D_ID="+ d_id+" not found!");
    }
    int o_id = rs.getInt("D_NEXT_O_ID");
    rs.close();

    stockGetCountStockFunc.setInt(1, w_id);
    stockGetCountStockFunc.setInt(2, d_id);
    stockGetCountStockFunc.setInt(3, o_id - 20);
    stockGetCountStockFunc.setInt(4, o_id);
    stockGetCountStockFunc.setInt(5, threshold);
    if (trace)
      LOG.trace(String.format("stockGetCountStock BEGIN [W_ID=%d, D_ID=%d, O_ID=%d]",
                w_id, d_id, o_id));
    rs = stockGetCountStockFunc.executeQuery();
    if (trace) LOG.trace("stockGetCountStock END");

    if (!rs.next()) {
      String msg = String.format("Failed to get StockLevel result for COUNT query " +
                                 "[W_ID=%d, D_ID=%d, O_ID=%d]", w_id, d_id, o_id);
      if (trace) LOG.warn(msg);
      throw new RuntimeException(msg);
    }
    stock_count = rs.getInt("result");
    if (trace) LOG.trace("stockGetCountStock RESULT=" + stock_count);

    rs.close();

    if (trace) {
      String terminalMessage = "\n+-------------------------- STOCK-LEVEL --------------------------+" +
              "\n Warehouse: " +
              w_id +
              "\n District:  " +
              d_id +
              "\n\n Stock Level Threshold: " +
              threshold +
              "\n Low Stock Count:       " +
              stock_count +
              "\n+-----------------------------------------------------------------+\n\n";
      LOG.trace(terminalMessage);
    }
    return null;
  }
}
