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

import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;
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
import com.oltpbenchmark.benchmarks.tpcc.TPCCConfig;

public class NewOrder extends Procedure {

  private static final Logger LOG = Logger.getLogger(NewOrder.class);

  public static final InstrumentedSQLStmt stmtGetCustSQL = new InstrumentedSQLStmt(
      "SELECT C_DISCOUNT, C_LAST, C_CREDIT" +
      "  FROM " + TPCCConstants.TABLENAME_CUSTOMER +
      " WHERE C_W_ID = ? " +
      "   AND C_D_ID = ? " +
      "   AND C_ID = ? FOR KEY SHARE");

  public static final InstrumentedSQLStmt stmtGetWhseSQL = new InstrumentedSQLStmt(
      "SELECT W_TAX " +
      "  FROM " + TPCCConstants.TABLENAME_WAREHOUSE +
      " WHERE W_ID = ?");

  public static final InstrumentedSQLStmt  stmtInsertNewOrderSQL = new InstrumentedSQLStmt(
      "INSERT INTO " + TPCCConstants.TABLENAME_NEWORDER +
      " (NO_O_ID, NO_D_ID, NO_W_ID) " +
      " VALUES ( ?, ?, ?)");

  public static final InstrumentedSQLStmt  stmtUpdateDistSQL = new InstrumentedSQLStmt(
      "UPDATE " + TPCCConstants.TABLENAME_DISTRICT +
      " SET D_NEXT_O_ID = D_NEXT_O_ID + 1 " +
      " WHERE D_W_ID = ? " +
      "   AND D_ID = ?" +
      "   RETURNING D_NEXT_O_ID, D_TAX");

  public static final InstrumentedSQLStmt  stmtGetItemSQL = new InstrumentedSQLStmt(
      "SELECT I_PRICE, I_NAME , I_DATA " +
      "  FROM " + TPCCConstants.TABLENAME_ITEM +
      " WHERE I_ID = ?");

  public static final InstrumentedSQLStmt  stmtGetStockSQL = new InstrumentedSQLStmt(
      "SELECT S_QUANTITY, S_DATA, S_DIST_01, S_DIST_02, S_DIST_03, S_DIST_04, S_DIST_05, " +
      "       S_DIST_06, S_DIST_07, S_DIST_08, S_DIST_09, S_DIST_10" +
      "  FROM " + TPCCConstants.TABLENAME_STOCK +
      " WHERE S_I_ID = ? " +
      "   AND S_W_ID = ?");

  public static final InstrumentedSQLStmt  stmtInsertOOrderSQL = new InstrumentedSQLStmt(
      "INSERT INTO " + TPCCConstants.TABLENAME_OPENORDER +
      " (O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_OL_CNT, O_ALL_LOCAL)" +
      " VALUES (?, ?, ?, ?, ?, ?, ?)");

  public static final InstrumentedSQLStmt  stmtUpdateStockSQL = new InstrumentedSQLStmt(
      "UPDATE " + TPCCConstants.TABLENAME_STOCK +
      "   SET S_QUANTITY = ? , " +
      "       S_YTD = S_YTD + ?, " +
      "       S_ORDER_CNT = S_ORDER_CNT + 1, " +
      "       S_REMOTE_CNT = S_REMOTE_CNT + ? " +
      " WHERE S_I_ID = ? " +
      "   AND S_W_ID = ?");

  public static final InstrumentedSQLStmt  stmtInsertOrderLineSQL = new InstrumentedSQLStmt(
      "INSERT INTO " + TPCCConstants.TABLENAME_ORDERLINE +
      " (OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, " +
      "OL_AMOUNT, OL_DIST_INFO)  VALUES (?,?,?,?,?,?,?,?,?)");

  // We will have multiple statements for selecting from the ITEM table or STOCK table as also for
  // inserting into the ORDER_LINE tables based on the O_OL_CNT that is part of the NewOrder
  // transaction. These statements are created dynamically.
  public final InstrumentedSQLStmt[] stmtGetItemSQLArr;
  public final InstrumentedSQLStmt[] stmtGetStockSQLArr;
  public final InstrumentedSQLStmt[] stmtUpdateStockProcedureSQL;
  public final InstrumentedSQLStmt[] stmtInsertOrderLineSQLArr;

  public static final Histogram latencyUpdateStockProcedure = new ConcurrentHistogram(InstrumentedSQLStmt.numSigDigits);


  // NewOrder Txn
  private InstrumentedPreparedStatement stmtGetCust = null;
  private InstrumentedPreparedStatement stmtGetWhse = null;
  private InstrumentedPreparedStatement stmtInsertNewOrder = null;
  private InstrumentedPreparedStatement stmtUpdateDist = null;
  private InstrumentedPreparedStatement stmtInsertOOrder = null;
  private InstrumentedPreparedStatement stmtGetItem = null;
  private InstrumentedPreparedStatement stmtGetStock = null;
  private InstrumentedPreparedStatement stmtUpdateStock = null;
  private InstrumentedPreparedStatement stmtUpdateStockProcedure = null;
  private InstrumentedPreparedStatement stmtInsertOrderLine = null;

  public static void printLatencyStats() {
    LOG.info("NewOrder : ");
    LOG.info("latency GetCust " + stmtGetCustSQL.getStats());
    LOG.info("latency GetWhse " + stmtGetWhseSQL.getStats());
    LOG.info("latency InsertNewOrder " + stmtInsertNewOrderSQL.getStats());
    LOG.info("latency UpdateDist " + stmtUpdateDistSQL.getStats());
    LOG.info("latency InsertOOrder " + stmtInsertOOrderSQL.getStats());
    LOG.info("latency GetItem " + stmtGetItemSQL.getStats());
    LOG.info("latency GetStock " + stmtGetStockSQL.getStats());
    LOG.info("latency UpdateStock " + stmtUpdateStockSQL.getStats());
    LOG.info("latency UpdateStockProcedure " + InstrumentedSQLStmt.getOperationLatencyString(latencyUpdateStockProcedure));
    LOG.info("latency InsertOrderLine " + stmtInsertOrderLineSQL.getStats());
  }

  public NewOrder() {
    stmtGetItemSQLArr = new InstrumentedSQLStmt[15];
    stmtGetStockSQLArr = new InstrumentedSQLStmt[15];
    stmtUpdateStockProcedureSQL = new InstrumentedSQLStmt[15];
    stmtInsertOrderLineSQLArr = new InstrumentedSQLStmt[11];

    // We create 15 statements for selecting rows from the `ITEM` table with varying number of ITEM
    // ids.  Each string looks like:
    // SELECT I_ID, I_PRICE, I_NAME , I_DATA
    // FROM ITEM
    // WHERE I_ID IN (?, ? ..);
    StringBuilder sb = new StringBuilder();
    sb.append(String.format("SELECT I_ID, I_PRICE, I_NAME , I_DATA FROM %s WHERE I_ID IN (",
                            TPCCConstants.TABLENAME_ITEM));
    for (int i = 1; i <= 15; ++i) {
      if (i == 1) {
        sb.append("?");
      } else {
        sb.append(",?");
      }
      stmtGetItemSQLArr[i - 1] = new InstrumentedSQLStmt(stmtGetItemSQL.getHistogram(), sb.toString() + ")");
    }

    // We create 15 statements for selecting rows from the `STOCK` table with varying number of
    // ITEM ids and a fixed WAREHOUSE id. Each string looks like:
    // SELECT I_I, I_NAME , I_DATA
    // FROM STOCK
    // WHERE S_W_ID = ? AND S_I_ID IN (?, ? ..);
    sb = new StringBuilder();
    sb.append(
      String.format("SELECT S_W_ID, S_I_ID, S_QUANTITY, S_DATA, S_YTD, S_REMOTE_CNT, S_DIST_01, " +
                    "S_DIST_02, S_DIST_03, S_DIST_04, S_DIST_05, S_DIST_06, S_DIST_07, S_DIST_08, " +
                    "S_DIST_09, S_DIST_10 FROM %s WHERE S_W_ID = ? AND S_I_ID IN (",
                    TPCCConstants.TABLENAME_STOCK));
    for (int i = 1; i <= 15; ++i) {
      if (i == 1) {
        sb.append("?");
      } else {
        sb.append(",?");
      }
      stmtGetStockSQLArr[i - 1] = new InstrumentedSQLStmt(stmtGetStockSQL.getHistogram(), sb.toString() + ") FOR KEY SHARE");
    }

    // We create 15 statements to update the rows in `STOCK` table. Each string looks like:
    // CALL updatestock[0-9]*(?, ? ...)
    sb = new StringBuilder();
    sb.append("?");
    for (int i = 1; i <= 15; ++i) {
      sb.append(", ?, ?, ?, ?");
      stmtUpdateStockProcedureSQL[i - 1] = new InstrumentedSQLStmt(latencyUpdateStockProcedure, String.format("CALL updatestock%d(%s)",
                                                                      i, sb.toString()));
    }
    // We create 11 statements that insert into `ORDERLINE`. Each string looks like:
    // INSERT INTO ORDERLINE
    // (OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT,
    //  OL_DIST_INFO)
    // VALUES (?,?,?,?,?,?,?,?,?), (?,?,?,?,?,?,?,?,?) ..
    sb = new StringBuilder();
    sb.append(String.format("INSERT INTO %s (OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, OL_I_ID, " +
                            "OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO) VALUES",
                            TPCCConstants.TABLENAME_ORDERLINE));
    for (int i = 1; i <= 15; ++i) {
      if (i == 1) {
        sb.append("(?,?,?,?,?,?,?,?,?)");
      } else {
        sb.append(", (?,?,?,?,?,?,?,?,?)");
      }
      if (i >= 5) {
        stmtInsertOrderLineSQLArr[i - 5] = new InstrumentedSQLStmt(stmtInsertOrderLineSQL.getHistogram(), sb.toString());
      }
    }
  }

  public void run(Connection conn, Random gen,
                  int terminalWarehouseID, int numWarehouses,
                  int terminalDistrictLowerID, int terminalDistrictUpperID,
                  Worker w) throws SQLException {
    int districtID = TPCCUtil.randomNumber(terminalDistrictLowerID,terminalDistrictUpperID, gen);
    int customerID = TPCCUtil.getCustomerID(gen);
    int numItems = TPCCUtil.randomNumber(5, 15, gen);
    int[] itemIDs = new int[numItems];
    int[] supplierWarehouseIDs = new int[numItems];
    int[] orderQuantities = new int[numItems];
    int allLocal = 1;
    for (int i = 0; i < numItems; i++) {
      itemIDs[i] = TPCCUtil.getItemID(gen);
      if (TPCCUtil.randomNumber(1, 100, gen) > 1) {
        supplierWarehouseIDs[i] = terminalWarehouseID;
      } else {
        do {
          supplierWarehouseIDs[i] = TPCCUtil.getRandomWarehouseId(w, terminalWarehouseID, numWarehouses, gen);
        } while (supplierWarehouseIDs[i] == terminalWarehouseID
            && numWarehouses > 1);
        allLocal = 0;
      }
      orderQuantities[i] = TPCCUtil.randomNumber(1, 10, gen);
    }

    // we need to cause 1% of the new orders to be rolled back.
    // TODO -- in this case, lets make sure our retry/failure bookkeeping is smart enough to distinguish between this
    // vs. unexpected failures.
    if (TPCCUtil.randomNumber(1, 100, gen) == 1)
      itemIDs[numItems - 1] = TPCCConfig.INVALID_ITEM_ID;

    // Initializing all prepared statements.
    stmtGetCust=this.getPreparedStatement(conn, stmtGetCustSQL);
    stmtGetWhse=this.getPreparedStatement(conn, stmtGetWhseSQL);
    stmtInsertNewOrder=this.getPreparedStatement(conn, stmtInsertNewOrderSQL);
    stmtUpdateDist =this.getPreparedStatement(conn, stmtUpdateDistSQL);
    stmtUpdateStock =this.getPreparedStatement(conn, stmtUpdateStockSQL);
    stmtInsertOOrder =this.getPreparedStatement(conn, stmtInsertOOrderSQL);

    newOrderTransaction(terminalWarehouseID, districtID,
                        customerID, numItems, allLocal, itemIDs,
                        supplierWarehouseIDs, orderQuantities, conn, w);
  }

  private void newOrderTransaction(int w_id, int d_id, int c_id,
                                   int o_ol_cnt, int o_all_local, int[] itemIDs,
                                   int[] supplierWarehouseIDs, int[] orderQuantities,
                                   Connection conn, Worker w) throws SQLException {
    float c_discount, w_tax, d_tax, i_price;
    int d_next_o_id, o_id, s_quantity;
    String c_last, c_credit, i_name, i_data, s_data, ol_dist_info;
    float[] itemPrices = new float[o_ol_cnt];
    float[] orderLineAmounts = new float[o_ol_cnt];
    String[] itemNames = new String[o_ol_cnt];
    int[] stockQuantities = new int[o_ol_cnt];
    char[] brandGeneric = new char[o_ol_cnt];
    int ol_supply_w_id, ol_i_id, ol_quantity;
    int s_remote_cnt_increment;
    float ol_amount, total_amount;

    try {
      stmtGetCust.setInt(1, w_id);
      stmtGetCust.setInt(2, d_id);
      stmtGetCust.setInt(3, c_id);
      ResultSet rs = stmtGetCust.executeQuery();
      if (!rs.next())
        throw new RuntimeException("C_D_ID=" + d_id
            + " C_ID=" + c_id + " not found!");
      c_discount = rs.getFloat("C_DISCOUNT");
      c_last = rs.getString("C_LAST");
      c_credit = rs.getString("C_CREDIT");
      rs.close();

      stmtGetWhse.setInt(1, w_id);
      rs = stmtGetWhse.executeQuery();
      if (!rs.next())
        throw new RuntimeException("W_ID=" + w_id + " not found!");
      w_tax = rs.getFloat("W_TAX");
      rs.close();

      //woonhak, need to change order because of foreign key constraints
      //update next_order_id first, but it might doesn't matter
      stmtUpdateDist.setInt(1, w_id);
      stmtUpdateDist.setInt(2, d_id);
      rs = stmtUpdateDist.executeQuery();
      if (!rs.next())
        throw new RuntimeException(
            "Error!! Cannot update next_order_id on district for D_ID="
                + d_id + " D_W_ID=" + w_id);
      d_next_o_id = rs.getInt("D_NEXT_O_ID");
      d_tax = rs.getFloat("D_TAX");
      rs.close();

      o_id = d_next_o_id - 1;

      // woonhak, need to change order, because of foreign key constraints
      //[[insert ooder first
      stmtInsertOOrder.setInt(1, o_id);
      stmtInsertOOrder.setInt(2, d_id);
      stmtInsertOOrder.setInt(3, w_id);
      stmtInsertOOrder.setInt(4, c_id);
      stmtInsertOOrder.setTimestamp(5, w.getBenchmarkModule().getTimestamp(
          System.currentTimeMillis()));
      stmtInsertOOrder.setInt(6, o_ol_cnt);
      stmtInsertOOrder.setInt(7, o_all_local);
      stmtInsertOOrder.executeUpdate();
      //insert ooder first]]
      //TODO: add error checking

      stmtInsertNewOrder.setInt(1, o_id);
      stmtInsertNewOrder.setInt(2, d_id);
      stmtInsertNewOrder.setInt(3, w_id);
      stmtInsertNewOrder.executeUpdate();
      //TODO: add error checking

      float[] i_price_arr = new float[o_ol_cnt];
      String[] i_name_arr = new String[o_ol_cnt];
      String[] i_data_arr = new String[o_ol_cnt];

      int[] s_qty_arr = new int[o_ol_cnt];
      String[] s_data_arr = new String[o_ol_cnt];
      String[] ol_dist_info_arr = new String[o_ol_cnt];
      int[] ytd_arr = new int[o_ol_cnt];
      int[] remote_cnt_arr = new int[o_ol_cnt];

      getItemsAndStock(o_ol_cnt, w_id, d_id,
                       itemIDs, supplierWarehouseIDs, orderQuantities,
                       i_price_arr, i_name_arr, i_data_arr,
                       s_qty_arr, s_data_arr, ol_dist_info_arr,
                       ytd_arr, remote_cnt_arr, conn);

      if (w.getBenchmarkModule().getWorkloadConfiguration().getUseStoredProcedures()) {
        updateStockUsingProcedures(o_ol_cnt, w_id, itemIDs, supplierWarehouseIDs,
                                   orderQuantities, s_qty_arr, ytd_arr, remote_cnt_arr, conn);
      } else {
        updateStock(o_ol_cnt, w_id, itemIDs, supplierWarehouseIDs,
                    orderQuantities, s_qty_arr, ytd_arr, remote_cnt_arr);
      }

      total_amount = insertOrderLines(o_id, w_id, d_id, o_ol_cnt, itemIDs,
                       supplierWarehouseIDs, orderQuantities,
                       i_price_arr, i_data_arr, s_data_arr,
                       ol_dist_info_arr, orderLineAmounts,
                       brandGeneric, conn);
      total_amount *= (1 + w_tax + d_tax) * (1 - c_discount);
    } catch(UserAbortException userEx) {
        LOG.debug("Caught an expected error in New Order");
        throw userEx;
    } finally {
        if (stmtInsertOrderLine != null)
            stmtInsertOrderLine.clearBatch();
        if (stmtUpdateStock != null)
            stmtUpdateStock.clearBatch();
    }
  }

  // This function queries the ITEM and the STOCK table to get the information pertaining to the
  // items that are part of this order. The state is saved in the corresponding arrays.
  void getItemsAndStock(int o_ol_cnt, int w_id, int d_id,
                        int[] itemIDs, int[] supplierWarehouseIDs, int[] orderQuantities,
                        float[] i_price_arr, String[] i_name_arr, String[] i_data_arr,
                        int[] s_qty_arr, String[] s_data_arr, String[] ol_dist_info_arr,
                        int[] ytd_arr, int[] remote_cnt_arr,
                        Connection conn) throws  SQLException {
    Map<Integer, HashSet<Integer>> input = new HashMap<>();
    for (int i = 0; i < o_ol_cnt; ++i) {
      int itemId = itemIDs[i];
      int supplierWh = supplierWarehouseIDs[i];
      if (!input.containsKey(supplierWh)) {
        input.put(supplierWh, new HashSet<>());
      }
      input.get(supplierWh).add(itemId);
    }

    for (Map.Entry<Integer, HashSet<Integer>> entry : input.entrySet()) {
      stmtGetItem = this.getPreparedStatement(conn, stmtGetItemSQLArr[entry.getValue().size() - 1]);
      int k = 1;
      for (int itemId : entry.getValue()) {
        stmtGetItem.setInt(k++,  itemId);
      }
      ResultSet rs1 = stmtGetItem.executeQuery();

      stmtGetStock =
        this.getPreparedStatement(conn, stmtGetStockSQLArr[entry.getValue().size() - 1]);
      k = 1;
      stmtGetStock.setInt(k++, entry.getKey() /* supplier WH */);
      for (int itemId: entry.getValue()) {
        stmtGetStock.setInt(k++, itemId);
      }
      ResultSet rs2 = stmtGetStock.executeQuery();

      for (int expected: entry.getValue()) {
        if (!rs1.next()) {
          throw new UserAbortException("EXPECTED new order rollback: I_ID=" +
                                       TPCCConfig.INVALID_ITEM_ID + "not found");
        }
        if (!rs2.next()) {
          throw new UserAbortException("EXPECTED new order rollback: I_ID=" +
                                       TPCCConfig.INVALID_ITEM_ID + "not found");
        }

        int itemId = rs1.getInt("I_ID");
        assert (itemId == expected);
        itemId = rs2.getInt("S_I_ID");
        assert (itemId == expected);

        float price = rs1.getFloat("I_PRICE");
        String name = rs1.getString("I_NAME");
        String data = rs1.getString("I_DATA");

        int s_quantity = rs2.getInt("S_QUANTITY");
        String s_data = rs2.getString("S_DATA");
        String ol_dist_info = getDistInfo(rs2, d_id);
        int supplierWh = rs2.getInt("S_W_ID");

        int ytd = rs2.getInt("S_YTD");
        int remote_cnt = rs2.getInt("S_REMOTE_CNT");

        storeInfo(itemIDs, supplierWarehouseIDs, orderQuantities,
                  itemId, w_id, supplierWh,
                  price, name, data, s_quantity, s_data, ol_dist_info,
                  ytd, remote_cnt,
                  i_price_arr, i_name_arr, i_data_arr,
                  s_qty_arr, s_data_arr, ol_dist_info_arr,
                  ytd_arr, remote_cnt_arr);
      }
      rs1.close();
      rs2.close();
    }
  }

  // Returns the district information based on the district id from a row in the STOCK table.
  String getDistInfo(ResultSet rs, int d_id) throws SQLException {
    switch (d_id) {
      case 1:
        return rs.getString("S_DIST_01");
      case 2:
        return rs.getString("S_DIST_02");
      case 3:
        return rs.getString("S_DIST_03");
      case 4:
        return rs.getString("S_DIST_04");
      case 5:
        return rs.getString("S_DIST_05");
      case 6:
        return rs.getString("S_DIST_06");
      case 7:
        return rs.getString("S_DIST_07");
      case 8:
        return rs.getString("S_DIST_08");
      case 9:
        return rs.getString("S_DIST_09");
      case 10:
        return rs.getString("S_DIST_10");
    }
    return "";
  }

  // Stores the different states in the various arrays.
  void storeInfo(int[] itemIDs, int[] supplierWhs, int[] orderQuantities,
                 int itemId, int w_id, int supplierWh,
                 float price, String name, String i_data,
                 int qty, String s_data, String dist_info,
                 int ytd, int remote_cnt,
                 float[] i_price_arr, String[] i_name_arr, String[] i_data_arr,
                 int[] qty_arr, String[] data_arr, String[] dist_info_arr,
                 int[] ytd_arr, int[] remote_cnt_arr) {
    for (int i = 0; i < itemIDs.length; ++i) {
      if (itemId == itemIDs[i] && supplierWh == supplierWhs[i]) {
        i_price_arr[i] = price;
        i_name_arr[i] = name;
        i_data_arr[i] = i_data;

        qty_arr[i] = qty;
        data_arr[i] = s_data;
        dist_info_arr[i]= dist_info;
        ytd_arr[i] = ytd;
        remote_cnt_arr[i] = remote_cnt;

        // Note that the same item could be present in the itemID multiple times. So adjust the new
        // quantity for the next time accordingly.
        if (qty - orderQuantities[i] >= 10) {
          qty -= orderQuantities[i];
        } else {
          qty += (91 - orderQuantities[i]);
        }
        ytd = ytd + orderQuantities[i];

        int s_remote_cnt_increment;
        if (supplierWh == w_id) {
          s_remote_cnt_increment = 0;
        } else {
          s_remote_cnt_increment = 1;
        }
        remote_cnt = remote_cnt + s_remote_cnt_increment;
      }
    }
  }

  void updateStockUsingProcedures(int o_ol_cnt, int w_id,
                                  int[] itemIDs, int[] supplierWarehouseIDs,
                                  int[] orderQuantities, int[] s_qty_arr,
                                  int[] ytd_arr, int[] remote_cnt_arr,
                                  Connection conn) throws  SQLException {
    Map<Integer, List<Integer>> input = new HashMap<>();
    for (int i = 0; i < o_ol_cnt; ++i) {
      int itemId = itemIDs[i];
      int supplierWh = supplierWarehouseIDs[i];
      if (!input.containsKey(supplierWh)) {
        input.put(supplierWh, new ArrayList<>());
      }
      input.get(supplierWh).add(itemId);
    }

    for (Map.Entry<Integer, List<Integer>> entry : input.entrySet()) {
      int whId = entry.getKey();
      int numEntries = entry.getValue().size();
      stmtUpdateStockProcedure = this.getPreparedStatement(conn, stmtUpdateStockProcedureSQL[numEntries - 1]);

      int i = 1;
      stmtUpdateStockProcedure.setInt(i++, whId);
      for (int itemId : entry.getValue()) {
        int index = getIndex(itemId, itemIDs);
        int s_quantity = s_qty_arr[index];
        int ol_quantity = orderQuantities[index];

        if (s_quantity - ol_quantity >= 10) {
          s_quantity -= ol_quantity;
        } else {
          s_quantity += -ol_quantity + 91;
        }

        int s_remote_cnt_increment;
        if (whId == w_id) {
          s_remote_cnt_increment = 0;
        } else {
          s_remote_cnt_increment = 1;
        }

        stmtUpdateStockProcedure.setInt(i++, itemId);
        stmtUpdateStockProcedure.setInt(i++, s_quantity);
        stmtUpdateStockProcedure.setInt(i++, ytd_arr[index] + ol_quantity);
        stmtUpdateStockProcedure.setInt(i++, remote_cnt_arr[index] + s_remote_cnt_increment);
      }
      stmtUpdateStockProcedure.execute();
    }
  }

  // Updates the STOCK table with the new values for the quantity, ytd, remote_cnt and
  // operation_count.
  void updateStock(int o_ol_cnt, int w_id,
                   int[] itemIDs, int[] supplierWarehouseIDs,
                   int[] orderQuantities, int[] s_qty_arr,
                   int[] ytd_arr, int[] remote_cnt_arr) throws SQLException {
    for (int i = 0; i < o_ol_cnt; ++i) {
      int itemId = itemIDs[i];
      int whId = supplierWarehouseIDs[i];

      int index = getIndex(itemId, itemIDs);
      int s_quantity = s_qty_arr[index];
      int ol_quantity = orderQuantities[index];

      if (s_quantity - ol_quantity >= 10) {
        s_quantity -= ol_quantity;
      } else {
        s_quantity += -ol_quantity + 91;
      }

      int s_remote_cnt_increment;
      if (whId == w_id) {
        s_remote_cnt_increment = 0;
      } else {
        s_remote_cnt_increment = 1;
      }

      int k = 0;
      stmtUpdateStock.setInt(++k, s_quantity);
      stmtUpdateStock.setInt(++k, ytd_arr[index] + ol_quantity);
      stmtUpdateStock.setInt(++k, remote_cnt_arr[index] + s_remote_cnt_increment);
      stmtUpdateStock.setInt(++k, whId);
      stmtUpdateStock.setInt(++k, itemId);
      stmtUpdateStock.addBatch();
    }
    stmtUpdateStock.executeBatch();
  }

  // Returns the index of the item in the array.
  int getIndex(int itemId, int[] itemIDs) {
    int idx = -1;
    for (int i = 0; i < itemIDs.length; ++i) {
      int v = itemIDs[i];
      if (v == itemId) {
        idx = i;
      }
    }
    return idx;
  }

  // Inserts the order lines for the order.
  int insertOrderLines(int o_id, int w_id, int d_id,
                       int o_ol_cnt, int[] itemIDs,
                       int[] supplierWarehouseIDs, int[] orderQuantities,
                       float[] i_price_arr, String[] i_data_arr,
                       String[] s_data_arr, String[] ol_dist_info_arr,
                       float[] orderLineAmounts, char[] brandGeneric,
                       Connection conn) throws SQLException {
    int total_amount = 0;
    int k = 1;
    stmtInsertOrderLine = this.getPreparedStatement(conn, stmtInsertOrderLineSQLArr[o_ol_cnt - 5]);
    for (int ol_number = 1; ol_number <= o_ol_cnt; ol_number++) {
      int ol_supply_w_id = supplierWarehouseIDs[ol_number - 1];
      int ol_i_id = itemIDs[ol_number - 1];
      int ol_quantity = orderQuantities[ol_number - 1];

      float i_price = i_price_arr[ol_number - 1];
      String i_data = i_data_arr[ol_number - 1];

      String s_data = s_data_arr[ol_number - 1];
      String ol_dist_info = ol_dist_info_arr[ol_number - 1];

      float ol_amount = ol_quantity * i_price;
      orderLineAmounts[ol_number - 1] = ol_amount;
      total_amount += ol_amount;

      if (i_data.contains("ORIGINAL") && s_data.contains("ORIGINAL")) {
        brandGeneric[ol_number - 1] = 'B';
      } else {
        brandGeneric[ol_number - 1] = 'G';
      }

      stmtInsertOrderLine.setInt(k++, o_id);
      stmtInsertOrderLine.setInt(k++, d_id);
      stmtInsertOrderLine.setInt(k++, w_id);
      stmtInsertOrderLine.setInt(k++, ol_number);
      stmtInsertOrderLine.setInt(k++, ol_i_id);
      stmtInsertOrderLine.setInt(k++, ol_supply_w_id);
      stmtInsertOrderLine.setInt(k++, ol_quantity);
      stmtInsertOrderLine.setDouble(k++, ol_amount);
      stmtInsertOrderLine.setString(k++, ol_dist_info);
    }
    stmtInsertOrderLine.execute();
    return total_amount;
  }

  public void test(Connection conn, Worker w) throws Exception {
    //initializing all prepared statements
    stmtGetCust=this.getPreparedStatement(conn, stmtGetCustSQL);
    stmtGetWhse=this.getPreparedStatement(conn, stmtGetWhseSQL);
    stmtInsertNewOrder=this.getPreparedStatement(conn, stmtInsertNewOrderSQL);
    stmtUpdateDist =this.getPreparedStatement(conn, stmtUpdateDistSQL);
    stmtInsertOOrder =this.getPreparedStatement(conn, stmtInsertOOrderSQL);
    stmtGetItem =this.getPreparedStatement(conn, stmtGetItemSQL);
    stmtGetStock =this.getPreparedStatement(conn, stmtGetStockSQL);
    stmtUpdateStock =this.getPreparedStatement(conn, stmtUpdateStockSQL);
    stmtInsertOrderLine =this.getPreparedStatement(conn, stmtInsertOrderLineSQL);

    int count = 10;
    int wId = 1;
    int dId = 1;
    int cId = 1;

    int[] itemIDs = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    int[] supplierWhIds = {1, 2, 1, 1, 1, 1, 1, 1, 1, 1};
    int[] initialQty = {20, 20, 20, 20, 20, 11, 11, 11, 11, 11};
    InitializeStockValues(conn, count, itemIDs, supplierWhIds, initialQty);
    int nextOid = GetNextOid(conn, wId, dId);

    // TPC-C spec v5.11.0 section 4.2 describes the NewOrder transaction.
    // 1. wId is the Warehouse Id used for the transaction.
    // 2. dId is the District Id used for the transaction.
    // 3. cId is the customer Id used for the transaction.
    // 4. nextOid is the Order Id used for the next transaction. This is used in the OORDER table as
    //    well as the ORDER_LINE table.
    // 5. itemIDs are the items being worked on by this transaction.
    // 6. supplierWHIds are the corresponding Warehouse IDs. The combination of itemId and
    //    supplierWhId is used to figure out the STOCK table row to be modified. We also need to
    //    ensure that the ORDER_LINE table is populated with the same number of rows with the
    //    corresponding {itemId, supplierWhId} combination.
    // 7. orderQts is the quantity of each item that needs to be part of the order. This quantity
    //    needs to be reflected in every row in ORDER_LINE.
    // 8. qtyArr is the final expected value for `S_QUANTITY` in the STOCK table for the
    //    corresponding items. This value is calculated using the specification in section 2.4.2.2.
    // 9. ytdArr is the final expected value for `S_YTD` in the STOCK table for the corresponding
    //    items.
    // 10. orderCntArr is the final expected value for `S_ORDER_CNT` in the STOCK table for the
    //     corresponding items.
    // 11. remoteCntArr is the final expected value for `S_REMOTE_CNT` in the STOCK table for the
    //     corresponding items.
    AssertNewOrderTransaction(conn, w, count, wId, dId, cId, nextOid,
                              itemIDs, supplierWhIds,
                              new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10} /* orderQts */,
                              new int[]{19, 18, 17, 16, 15, 96, 95, 94, 93, 92} /* qtyArr */,
                              new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10} /* ytdArr */,
                              new int[]{1, 1, 1, 1, 1, 1, 1, 1, 1, 1} /* orderCntArr */,
                              new int[]{0, 1, 0, 0, 0, 0, 0, 0, 0, 0} /* remoteCntArr */);

    AssertNewOrderTransaction(conn, w, count, wId, dId, cId, nextOid + 1,
                              itemIDs, supplierWhIds,
                              new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10} /* orderQts */,
                              new int[]{18, 16, 14, 12, 10, 90, 88, 86, 84, 82} /* qtyArr */,
                              new int[]{2, 4, 6, 8, 10, 12, 14, 16, 18, 20} /* ytdArr */,
                              new int[]{2, 2, 2, 2, 2, 2, 2, 2, 2, 2} /* orderCntArr */,
                              new int[]{0, 2, 0, 0, 0, 0, 0, 0, 0, 0} /* remoteCntArr */);

    // Test same item being present twice in the list of items.
    AssertNewOrderTransaction(conn, w, count, wId, dId, cId, nextOid + 2,
                              new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 1} /* itemIDs */,
                              supplierWhIds,
                              new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10} /* orderQts */,
                              new int[]{98, 14, 11, 99, 96, 84, 81, 78, 75, 98} /* qtyArr */,
                              new int[]{13, 6, 9, 12, 15, 18, 21, 24, 27, 13} /* ytdArr */,
                              new int[]{4, 3, 3, 3, 3, 3, 3, 3, 3, 4} /* orderCntArr */,
                              new int[]{0, 3, 0, 0, 0, 0, 0, 0, 0, 0} /* remoteCntArr */);

  }

  void AssertNewOrderTransaction(Connection conn, Worker w,
                                 int count, int wId, int dId, int cId, int nextOid,
                                 int[] itemIDs, int[] supplierWhIds, int[] orderQts,
                                 int[] qtyArr, int[] ytdArr,
                                 int[] orderCntArr, int[] remoteCntArr) throws Exception {
    try{
      newOrderTransaction(wId, dId, cId, count, 0 /* o_all_local */, itemIDs, supplierWhIds,
                          orderQts, conn, w);
    } catch (Exception e) {
      LOG.error("Execution of the new order transaction failed" + e);
      throw e;
    }

    AssertDistValues(conn, wId, dId, nextOid + 1);
    LOG.info("Done asserting District");
    AssertStockValues(conn, count, itemIDs, supplierWhIds, qtyArr, ytdArr, orderCntArr,
                      remoteCntArr);
    LOG.info("Done asserting STOCK");
    AssertOOrderValues(conn, nextOid, wId, dId);
    LOG.info("Done asserting OORDER");
    AssertOrderLineValues(conn, count, nextOid, wId, dId, itemIDs, supplierWhIds, orderQts);
    LOG.info("Done asserting ORDERLINE");
  }

  // Initializes the stock quantity with the specified values and sets the S_YTD, S_ORDER_CNT and
  // S_REMOTE_CNT to 0.
  void InitializeStockValues(Connection conn,int count,
                             int[] itemIDs, int[] supplierWhIds, int [] qtyArr) throws Exception {
    try {
      Statement stmt = conn.createStatement();
      for (int i = 0; i < count; ++i) {
        stmt.execute(
          String.format("UPDATE STOCK SET S_QUANTITY = %d, S_YTD = 0, " +
                        "S_ORDER_CNT = 0, S_REMOTE_CNT = 0 WHERE S_W_ID = %d AND S_I_ID = %d",
                        qtyArr[i], supplierWhIds[i], itemIDs[i]));
      }
    } catch (Exception e) {
      LOG.error("Initializing rows in STOCK table failed " + e);
      throw e;
    }
  }

  // Returns the next OID specified in the DISTRICT table.
  int GetNextOid(Connection conn, int wId, int dId) throws Exception {
    try {
      Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery(
          String.format("SELECT * FROM DISTRICT WHERE D_W_ID = %d AND D_ID = %d", wId, dId));
      if (!rs.next()) {
        throw new Exception("Reading rows from DISTRICT table failed");
      }
      return rs.getInt("D_NEXT_O_ID");
    } catch (Exception e) {
      LOG.error("Reading rows from DISTRICT table failed " + e);
      throw e;
    }
  }

  // Ensures that the DISTRICT table is updated with the proper OID.
  void AssertDistValues(Connection conn, int wId, int dId, int nextOId) throws Exception {
    try {
      Statement stmt = conn.createStatement();

      ResultSet rs = stmt.executeQuery(
          String.format("SELECT * FROM DISTRICT WHERE D_W_ID = %d AND D_ID = %d", wId, dId));
      if (!rs.next()) {
        throw new Exception("Reading row from DISTRICT table failed");
      }
      if (rs.getInt("D_NEXT_O_ID") != nextOId) {
          throw new Exception(String.format("Values not expected for DISTRICT (%d) (%d)",
                                            rs.getInt("D_NEXT_O_ID"), nextOId));
      }
    } catch (Exception e) {
      LOG.error("Reading rows from DISTRICT table failed " + e);
      throw e;
    }
  }

  // Ensures that the STOCK table is updated with the proper values of quantity, ytd, order_cnt and
  // remote_cnt values.
  void AssertStockValues(Connection conn, int count,
                         int[] itemIDs, int[] supplierWhIds,
                         int[] qtyArr, int[] ytdArr,
                         int[] orderCntArr, int[] remoteCntArr) throws Exception {
    try {
      Statement stmt = conn.createStatement();
      for (int i = 0; i < count; ++i) {
        int itemId = itemIDs[i];
        int supplierWhId = supplierWhIds[i];

        ResultSet rs = stmt.executeQuery(
            String.format("SELECT * FROM STOCK WHERE S_W_ID = %d AND S_I_ID = %d",
                          supplierWhId, itemId));
        if (!rs.next()) {
          throw new Exception("Reading row from STOCK table failed");
        }

        if (rs.getInt("S_QUANTITY") != qtyArr[i] ||
            rs.getInt("S_YTD") != ytdArr[i] ||
            rs.getInt("S_ORDER_CNT") != orderCntArr[i] ||
            rs.getInt("S_REMOTE_CNT") != remoteCntArr[i]) {
          throw new Exception(String.format(
              "Values not expected for STOCK (%d %d %d %d) (%d %d %d %d)",
              rs.getInt("S_QUANTITY"), rs.getInt("S_YTD"), rs.getInt("S_ORDER_CNT"),
              rs.getInt("S_REMOTE_CNT"), qtyArr[i], ytdArr[i], orderCntArr[i], remoteCntArr[i]));
        }
      }
    } catch (Exception e) {
      LOG.error("Reading rows from STOCK table failed " + e);
      throw e;
    }
  }

  // Ensures that the OORDER table is populated with the correct row corresponding to the new
  // order_id.
  void AssertOOrderValues(Connection conn, int oId, int wId, int dId) throws Exception {
    try {
      Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery(
          String.format("SELECT * FROM OORDER WHERE O_ID = %d AND O_W_ID = %d AND O_D_ID = %d",
                        oId, wId, dId));
      if (!rs.next()) {
        throw new Exception("Reading row from OORDER table failed");
      }
      if (rs.getInt("O_ID") != oId ||
          rs.getInt("O_W_ID") != wId ||
          rs.getInt("O_D_ID") != dId) {
        throw new Exception(String.format("Values not expected for OORDER (%d %d %d) (%d %d %d)",
                                          rs.getInt("O_ID"), rs.getInt("O_W_ID"),
                                          rs.getInt("O_D_ID"), oId, wId, dId));
      }
    } catch (Exception e) {
      LOG.error("Reading rows from OORDER table failed " + e);
      throw e;
    }
  }

  // Ensures that the ORDER_LINE table is populated with the correct rows corresponding to the new
  // order_id.
  void AssertOrderLineValues(Connection conn, int count,
                             int oId, int wId, int dId,
                             int[] itemIDs, int[] supplierWhIds,
                             int[] qtyArr) throws Exception {
    try {
      Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery(String.format(
          "SELECT * FROM ORDER_LINE WHERE OL_O_ID = %d AND OL_W_ID = %d AND OL_D_ID = %d",
          oId, wId, dId));
      for (int i = 0; i < count; ++i) {
        if (!rs.next()) {
          throw new Exception("Reading row from ORDER_LINE table failed");
        }
        if (rs.getInt("OL_O_ID") != oId ||
            rs.getInt("OL_W_ID") != wId ||
            rs.getInt("OL_D_ID") != dId ||
            rs.getInt("OL_NUMBER") != (i + 1) ||
            rs.getInt("OL_I_ID") != itemIDs[i] ||
            rs.getInt("OL_SUPPLY_W_ID") != supplierWhIds[i] ||
            rs.getInt("OL_QUANTITY") != qtyArr[i]) {
        throw new Exception(String.format(
            "Values not expected for ORDERLINE (%d %d %d %d %d %d %d) (%d %d %d %d %d %d %d)",
            rs.getInt("OL_O_ID"), rs.getInt("OL_W_ID"), rs.getInt("OL_D_ID"),
            rs.getInt("OL_NUMBER"), rs.getInt("OL_I_ID"), rs.getInt("OL_SUPPLY_W_ID"),
            rs.getInt("OL_QUANTITY"), oId, wId, dId, i, itemIDs[i],  supplierWhIds[i], qtyArr[i]));
        }
      }
    } catch (Exception e) {
      LOG.error("Reading rows from ORDER_LINE table failed " + e);
      throw e;
    }
  }
}
