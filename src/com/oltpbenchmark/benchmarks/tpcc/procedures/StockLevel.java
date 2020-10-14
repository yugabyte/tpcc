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

import java.lang.Math;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.log4j.Logger;

import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.tpcc.TPCCConstants;
import com.oltpbenchmark.benchmarks.tpcc.TPCCUtil;
import com.oltpbenchmark.benchmarks.tpcc.TPCCWorker;

public class StockLevel extends TPCCProcedure {

  private static final Logger LOG = Logger.getLogger(StockLevel.class);

  public SQLStmt stockGetDistOrderIdSQL = new SQLStmt(
      "SELECT D_NEXT_O_ID " +
      "  FROM " + TPCCConstants.TABLENAME_DISTRICT +
      " WHERE D_W_ID = ? " +
      "   AND D_ID = ?");

  public SQLStmt stockGetCountUsingJoinSQL = new SQLStmt(
      "SELECT COUNT(DISTINCT (S_I_ID)) AS STOCK_COUNT " +
      " FROM " + TPCCConstants.TABLENAME_ORDERLINE + ", " + TPCCConstants.TABLENAME_STOCK +
      " WHERE OL_W_ID = ?" +
      " AND OL_D_ID = ?" +
      " AND OL_O_ID < ?" +
      " AND OL_O_ID >= ?" +
      " AND S_W_ID = ?" +
      " AND S_I_ID = OL_I_ID" +
      " AND S_QUANTITY < ?");

  public SQLStmt stockGetItemsSQL = new SQLStmt(
      "SELECT DISTINCT(OL_I_ID) FROM " + TPCCConstants.TABLENAME_ORDERLINE +
      " WHERE OL_W_ID = ?" +
      " AND OL_D_ID = ?" +
      " AND OL_O_ID < ?" +
      " AND OL_O_ID >= ?");

  public SQLStmt[] stockGetCountSQL;

  // Stock Level Txn
  private PreparedStatement stockGetDistOrderId = null;
  private PreparedStatement stockGetCountUsingJoin = null;
  private PreparedStatement stockGetItems = null;
  private PreparedStatement stockGetCount = null;

  public StockLevel() {
    // Because we can have as much as 300 (20 orders and a max of 15 items in an order) items in a
    // single query, we can't have unique statements prepared for all the possible counts. Instead,
    // we divide a request into a sequence of requests with each request handling (2 ^ i) items.
    stockGetCountSQL = new SQLStmt[9];
    StringBuilder sb = new StringBuilder();
    sb.append(
      "SELECT COUNT(DISTINCT (S_I_ID)) AS STOCK_COUNT FROM " + TPCCConstants.TABLENAME_STOCK +
      " WHERE S_W_ID = ? AND S_I_ID in (?");
    for (int i = 0; i < 9; ++i) {
      for (int j = 0; i > 0 && j < (1 << (i - 1)); ++j)
        sb.append(",?");
      stockGetCountSQL[i] = new SQLStmt(sb.toString() + ") AND S_QUANTITY < ?");
    }
  }

  public ResultSet run(Connection conn, Random gen,
                        int w_id, int numWarehouses,
                        int terminalDistrictLowerID, int terminalDistrictUpperID,
                        TPCCWorker w) throws SQLException {
    boolean trace = LOG.isTraceEnabled();
    stockGetDistOrderId = this.getPreparedStatement(conn, stockGetDistOrderIdSQL);
    stockGetCountUsingJoin = this.getPreparedStatement(conn, stockGetCountUsingJoinSQL);
    stockGetItems = this.getPreparedStatement(conn, stockGetItemsSQL);

    int threshold = TPCCUtil.randomNumber(10, 20, gen);
    int d_id = TPCCUtil.randomNumber(terminalDistrictLowerID,terminalDistrictUpperID, gen);

    int o_id = 0;
    // XXX int i_id = 0;

    stockGetDistOrderId.setInt(1, w_id);
    stockGetDistOrderId.setInt(2, d_id);
    if (trace)
      LOG.trace(String.format("stockGetDistOrderId BEGIN [W_ID=%d, D_ID=%d]", w_id, d_id));
    ResultSet rs = stockGetDistOrderId.executeQuery();
    if (trace) LOG.trace("stockGetDistOrderId END");

    if (!rs.next()) {
      throw new RuntimeException("D_W_ID="+ w_id +" D_ID="+ d_id+" not found!");
    }
    o_id = rs.getInt("D_NEXT_O_ID");
    rs.close();

    List<Integer> itemIds = getItems(w_id, d_id, o_id);
    int stock_count = getStockCount(w_id, itemIds, threshold, conn);
    conn.commit();

    if (trace) {
      StringBuilder terminalMessage = new StringBuilder();
      terminalMessage.append(
          "\n+-------------------------- STOCK-LEVEL --------------------------+");
      terminalMessage.append("\n Warehouse: ");
      terminalMessage.append(w_id);
      terminalMessage.append("\n District:  ");
      terminalMessage.append(d_id);
      terminalMessage.append("\n\n Stock Level Threshold: ");
      terminalMessage.append(threshold);
      terminalMessage.append("\n Low Stock Count:       ");
      terminalMessage.append(stock_count);
      terminalMessage.append(
          "\n+-----------------------------------------------------------------+\n\n");
      LOG.trace(terminalMessage.toString());
    }
    return null;
  }

  private int getStockCountUsingJoin(int whId, int dId, int oId,
                                     int threshold) throws SQLException {
    stockGetCountUsingJoin.setInt(1, whId);
    stockGetCountUsingJoin.setInt(2, dId);
    stockGetCountUsingJoin.setInt(3, oId);
    stockGetCountUsingJoin.setInt(4, oId - 20);
    stockGetCountUsingJoin.setInt(5, whId);
    stockGetCountUsingJoin.setInt(6, threshold);
    ResultSet rs = stockGetCountUsingJoin.executeQuery();

    if (!rs.next()) {
      String msg = String.format("Failed to get StockLevel result for COUNT query " +
                                 "[W_ID=%d, D_ID=%d, O_ID=%d]", whId, dId, oId);
      throw new RuntimeException(msg);
    }
    int count = rs.getInt("STOCK_COUNT");
    rs.close();
    return count;
  }

  private List<Integer> getItems(int whId, int dId, int oId) throws SQLException {
    int k = 1;
    stockGetItems.setInt(k++, whId);
    stockGetItems.setInt(k++, dId);
    stockGetItems.setInt(k++, oId);
    stockGetItems.setInt(k++, oId - 20);
    ResultSet rs = stockGetItems.executeQuery();

    List<Integer> out = new ArrayList<Integer>();
    while (rs.next()) {
      int item = rs.getInt("OL_I_ID");
      out.add(item);
    }
    rs.close();
    return out;
  }

  private int getStockCount(int whId, List<Integer> itemIds, int threshold,
                            Connection conn) throws SQLException {
    int stock_count = 0;
    int len = itemIds.size();
    int bit = 0;
    int itemIdx = 0;

    // If number of items is 10 (1010 in binary), then we send 2 requests, one with 2 items and one
    // with with 8 items.
    while (len > 0) {
      if (len % 2 == 0) {
        len = (len >> 1);
        ++bit;
        continue;
      }
      int count = (1 << bit);
      stockGetCount = this.getPreparedStatement(conn, stockGetCountSQL[bit]);

      int k = 1;
      stockGetCount.setInt(k++, whId);
      for (int j = 1; j <= count; ++j) {
        stockGetCount.setInt(k++, itemIds.get(itemIdx++));
      }
      stockGetCount.setInt(k++, threshold);
      ResultSet rs = stockGetCount.executeQuery();

      if (!rs.next()) {
        String msg = String.format("Failed to get StockLevel result for COUNT query " +
                                   "[W_ID=%d]", whId);
        throw new RuntimeException(msg);
      }
      stock_count += rs.getInt("STOCK_COUNT");
      rs.close();

      len = (len >> 1);
      ++bit;
    }
    return stock_count;
  }

  public void test(Connection conn, TPCCWorker w) throws Exception {
    stockGetDistOrderId = this.getPreparedStatement(conn, stockGetDistOrderIdSQL);
    stockGetCountUsingJoin = this.getPreparedStatement(conn, stockGetCountUsingJoinSQL);
    stockGetItems = this.getPreparedStatement(conn, stockGetItemsSQL);


    stockGetDistOrderId.setInt(1, 1);
    stockGetDistOrderId.setInt(2, 1);
    ResultSet rs = stockGetDistOrderId.executeQuery();
    if (!rs.next()) {
      throw new RuntimeException("Get district order id failed");
    }
    int oId = rs.getInt("D_NEXT_O_ID");
    rs.close();

    int count1 = getStockCountUsingJoin(1, 1, oId, 15);

    List<Integer> items = getItems(1, 1, oId);
    int count2 = getStockCount(1, items, 15, conn);

    if (count1 != count2) {
        throw new Exception(
            String.format("Values not expected for STOCK COUNT %d %d", count1, count2));
    }
  }
}
