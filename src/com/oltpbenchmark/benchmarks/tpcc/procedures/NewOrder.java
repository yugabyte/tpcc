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
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import org.apache.log4j.Logger;

import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.tpcc.TPCCConstants;
import com.oltpbenchmark.benchmarks.tpcc.TPCCUtil;
import com.oltpbenchmark.benchmarks.tpcc.TPCCWorker;
import com.oltpbenchmark.benchmarks.tpcc.TPCCConfig;

public class NewOrder extends TPCCProcedure {

  private static final Logger LOG = Logger.getLogger(NewOrder.class);

  public final SQLStmt stmtGetCustSQL = new SQLStmt(
      "SELECT C_DISCOUNT, C_LAST, C_CREDIT" +
      "  FROM " + TPCCConstants.TABLENAME_CUSTOMER +
      " WHERE C_W_ID = ? " +
      "   AND C_D_ID = ? " +
      "   AND C_ID = ?");

  public final SQLStmt stmtGetWhseSQL = new SQLStmt(
      "SELECT W_TAX " +
      "  FROM " + TPCCConstants.TABLENAME_WAREHOUSE +
      " WHERE W_ID = ?");

  public final SQLStmt stmtGetDistSQL = new SQLStmt(
      "SELECT D_NEXT_O_ID, D_TAX " +
      "  FROM " + TPCCConstants.TABLENAME_DISTRICT +
      " WHERE D_W_ID = ? AND D_ID = ? FOR UPDATE");

  public final SQLStmt  stmtInsertNewOrderSQL = new SQLStmt(
      "INSERT INTO " + TPCCConstants.TABLENAME_NEWORDER +
      " (NO_O_ID, NO_D_ID, NO_W_ID) " +
      " VALUES ( ?, ?, ?)");

  public final SQLStmt  stmtUpdateDistSQL = new SQLStmt(
      "UPDATE " + TPCCConstants.TABLENAME_DISTRICT +
      " SET D_NEXT_O_ID = D_NEXT_O_ID + 1 " +
      " WHERE D_W_ID = ? " +
      "   AND D_ID = ?");

  public final SQLStmt  stmtInsertOOrderSQL = new SQLStmt(
      "INSERT INTO " + TPCCConstants.TABLENAME_OPENORDER +
      " (O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_OL_CNT, O_ALL_LOCAL)" +
      " VALUES (?, ?, ?, ?, ?, ?, ?)");

  public final SQLStmt  stmtGetItemSQL = new SQLStmt(
      "SELECT I_PRICE, I_NAME , I_DATA " +
      "  FROM " + TPCCConstants.TABLENAME_ITEM +
      " WHERE I_ID = ?");

  public final SQLStmt  stmtGetStockSQL = new SQLStmt(
      "SELECT S_QUANTITY, S_DATA, S_DIST_01, S_DIST_02, S_DIST_03, S_DIST_04, S_DIST_05, " +
      "       S_DIST_06, S_DIST_07, S_DIST_08, S_DIST_09, S_DIST_10" +
      "  FROM " + TPCCConstants.TABLENAME_STOCK +
      " WHERE S_I_ID = ? " +
      "   AND S_W_ID = ? FOR UPDATE");

  public final SQLStmt  stmtUpdateStockSQL = new SQLStmt(
      "UPDATE " + TPCCConstants.TABLENAME_STOCK +
      "   SET S_QUANTITY = ? , " +
      "       S_YTD = S_YTD + ?, " +
      "       S_ORDER_CNT = S_ORDER_CNT + 1, " +
      "       S_REMOTE_CNT = S_REMOTE_CNT + ? " +
      " WHERE S_I_ID = ? " +
      "   AND S_W_ID = ?");

  public final SQLStmt  stmtInsertOrderLineSQL = new SQLStmt(
      "INSERT INTO " + TPCCConstants.TABLENAME_ORDERLINE +
      " (OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, " +
      "OL_AMOUNT, OL_DIST_INFO)  VALUES (?,?,?,?,?,?,?,?,?)");

  // NewOrder Txn
  private PreparedStatement stmtGetCust = null;
  private PreparedStatement stmtGetWhse = null;
  private PreparedStatement stmtGetDist = null;
  private PreparedStatement stmtInsertNewOrder = null;
  private PreparedStatement stmtUpdateDist = null;
  private PreparedStatement stmtInsertOOrder = null;
  private PreparedStatement stmtGetItem = null;
  private PreparedStatement stmtGetStock = null;
  private PreparedStatement stmtUpdateStock = null;
  private PreparedStatement stmtInsertOrderLine = null;

  public ResultSet run(Connection conn, Random gen,
    int terminalWarehouseID, int numWarehouses,
    int terminalDistrictLowerID, int terminalDistrictUpperID,
      TPCCWorker w) throws SQLException {

    //initializing all prepared statements
    stmtGetCust=this.getPreparedStatement(conn, stmtGetCustSQL);
    stmtGetWhse=this.getPreparedStatement(conn, stmtGetWhseSQL);
    stmtGetDist=this.getPreparedStatement(conn, stmtGetDistSQL);
    stmtInsertNewOrder=this.getPreparedStatement(conn, stmtInsertNewOrderSQL);
    stmtUpdateDist =this.getPreparedStatement(conn, stmtUpdateDistSQL);
    stmtInsertOOrder =this.getPreparedStatement(conn, stmtInsertOOrderSQL);
    stmtGetItem =this.getPreparedStatement(conn, stmtGetItemSQL);
    stmtGetStock =this.getPreparedStatement(conn, stmtGetStockSQL);
    stmtUpdateStock =this.getPreparedStatement(conn, stmtUpdateStockSQL);
    stmtInsertOrderLine =this.getPreparedStatement(conn, stmtInsertOrderLineSQL);

    int districtID = TPCCUtil.randomNumber(terminalDistrictLowerID,terminalDistrictUpperID, gen);
    int customerID = TPCCUtil.getCustomerID(gen);

    int numItems = (int) TPCCUtil.randomNumber(5, 15, gen);
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
          supplierWarehouseIDs[i] = TPCCUtil.randomNumber(1,
              numWarehouses, gen);
        } while (supplierWarehouseIDs[i] == terminalWarehouseID
            && numWarehouses > 1);
        allLocal = 0;
      }
      orderQuantities[i] = TPCCUtil.randomNumber(1, 10, gen);
    }

    // we need to cause 1% of the new orders to be rolled back.
    if (TPCCUtil.randomNumber(1, 100, gen) == 1)
      itemIDs[numItems - 1] = TPCCConfig.INVALID_ITEM_ID;


    newOrderTransaction(terminalWarehouseID, districtID,
            customerID, numItems, allLocal, itemIDs,
            supplierWarehouseIDs, orderQuantities, conn, w);
    return null;
  }

  private void newOrderTransaction(int w_id, int d_id, int c_id,
                                   int o_ol_cnt, int o_all_local, int[] itemIDs,
                                   int[] supplierWarehouseIDs, int[] orderQuantities,
                                   Connection conn, TPCCWorker w) throws SQLException {
    float c_discount, w_tax, d_tax = 0, i_price;
    int d_next_o_id, o_id = -1, s_quantity;
    String c_last = null, c_credit = null, i_name, i_data, s_data;
    String s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05;
    String s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10, ol_dist_info = null;
    float[] itemPrices = new float[o_ol_cnt];
    float[] orderLineAmounts = new float[o_ol_cnt];
    String[] itemNames = new String[o_ol_cnt];
    int[] stockQuantities = new int[o_ol_cnt];
    char[] brandGeneric = new char[o_ol_cnt];
    int ol_supply_w_id, ol_i_id, ol_quantity;
    int s_remote_cnt_increment;
    float ol_amount, total_amount = 0;

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
      rs = null;

      stmtGetWhse.setInt(1, w_id);
      rs = stmtGetWhse.executeQuery();
      if (!rs.next())
        throw new RuntimeException("W_ID=" + w_id + " not found!");
      w_tax = rs.getFloat("W_TAX");
      rs.close();
      rs = null;

      stmtGetDist.setInt(1, w_id);
      stmtGetDist.setInt(2, d_id);
      rs = stmtGetDist.executeQuery();
      if (!rs.next()) {
        throw new RuntimeException("D_ID=" + d_id + " D_W_ID=" + w_id
            + " not found!");
      }
      d_next_o_id = rs.getInt("D_NEXT_O_ID");
      d_tax = rs.getFloat("D_TAX");
      rs.close();
      rs = null;

      //woonhak, need to change order because of foreign key constraints
      //update next_order_id first, but it might doesn't matter
      stmtUpdateDist.setInt(1, w_id);
      stmtUpdateDist.setInt(2, d_id);
      int result = stmtUpdateDist.executeUpdate();
      if (result == 0)
        throw new RuntimeException(
            "Error!! Cannot update next_order_id on district for D_ID="
                + d_id + " D_W_ID=" + w_id);

      o_id = d_next_o_id;

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
      /*TODO: add error checking */

      stmtInsertNewOrder.setInt(1, o_id);
      stmtInsertNewOrder.setInt(2, d_id);
      stmtInsertNewOrder.setInt(3, w_id);
      stmtInsertNewOrder.executeUpdate();
      /*TODO: add error checking */


      /* woonhak, [[change order
      stmtInsertOOrder.setInt(1, o_id);
      stmtInsertOOrder.setInt(2, d_id);
      stmtInsertOOrder.setInt(3, w_id);
      stmtInsertOOrder.setInt(4, c_id);
      stmtInsertOOrder.setTimestamp(5,
          new Timestamp(System.currentTimeMillis()));
      stmtInsertOOrder.setInt(6, o_ol_cnt);
      stmtInsertOOrder.setInt(7, o_all_local);
      stmtInsertOOrder.executeUpdate();
      change order]]*/

      Set<Integer> itemSet = new HashSet<Integer>();
      for (int ol_number = 1; ol_number <= o_ol_cnt; ol_number++) {
        ol_supply_w_id = supplierWarehouseIDs[ol_number - 1];
        ol_i_id = itemIDs[ol_number - 1];

        // If the item is present multiple times in the list, flush the outstanding updates/inserts
        // so that this update can use the correct values.
        if (itemSet.contains(ol_i_id)) {
          stmtInsertOrderLine.executeBatch();
          stmtUpdateStock.executeBatch();
        }
        itemSet.add(ol_i_id);
        ol_quantity = orderQuantities[ol_number - 1];
        stmtGetItem.setInt(1, ol_i_id);
        rs = stmtGetItem.executeQuery();
        if (!rs.next()) {
          // This is (hopefully) an expected error: this is an
          // expected new order rollback
          assert ol_number == o_ol_cnt;
          assert ol_i_id == TPCCConfig.INVALID_ITEM_ID;
          rs.close();
          throw new UserAbortException(
              "EXPECTED new order rollback: I_ID=" + ol_i_id
                  + " not found!");
        }

        i_price = rs.getFloat("I_PRICE");
        i_name = rs.getString("I_NAME");
        i_data = rs.getString("I_DATA");
        rs.close();
        rs = null;

        itemPrices[ol_number - 1] = i_price;
        itemNames[ol_number - 1] = i_name;


        stmtGetStock.setInt(1, ol_i_id);
        stmtGetStock.setInt(2, ol_supply_w_id);
        rs = stmtGetStock.executeQuery();
        if (!rs.next())
          throw new RuntimeException("I_ID=" + ol_i_id
              + " not found!");
        s_quantity = rs.getInt("S_QUANTITY");
        s_data = rs.getString("S_DATA");
        s_dist_01 = rs.getString("S_DIST_01");
        s_dist_02 = rs.getString("S_DIST_02");
        s_dist_03 = rs.getString("S_DIST_03");
        s_dist_04 = rs.getString("S_DIST_04");
        s_dist_05 = rs.getString("S_DIST_05");
        s_dist_06 = rs.getString("S_DIST_06");
        s_dist_07 = rs.getString("S_DIST_07");
        s_dist_08 = rs.getString("S_DIST_08");
        s_dist_09 = rs.getString("S_DIST_09");
        s_dist_10 = rs.getString("S_DIST_10");
        rs.close();
        rs = null;

        stockQuantities[ol_number - 1] = s_quantity;

        if (s_quantity - ol_quantity >= 10) {
          s_quantity -= ol_quantity;
        } else {
          s_quantity += -ol_quantity + 91;
        }

        if (ol_supply_w_id == w_id) {
          s_remote_cnt_increment = 0;
        } else {
          s_remote_cnt_increment = 1;
        }

        stmtUpdateStock.setInt(1, s_quantity);
        stmtUpdateStock.setInt(2, ol_quantity);
        stmtUpdateStock.setInt(3, s_remote_cnt_increment);
        stmtUpdateStock.setInt(4, ol_i_id);
        stmtUpdateStock.setInt(5, ol_supply_w_id);
        stmtUpdateStock.addBatch();

        ol_amount = ol_quantity * i_price;
        orderLineAmounts[ol_number - 1] = ol_amount;
        total_amount += ol_amount;

        if (i_data.indexOf("ORIGINAL") != -1
            && s_data.indexOf("ORIGINAL") != -1) {
          brandGeneric[ol_number - 1] = 'B';
        } else {
          brandGeneric[ol_number - 1] = 'G';
        }

        switch ((int) d_id) {
          case 1:
            ol_dist_info = s_dist_01;
            break;
          case 2:
            ol_dist_info = s_dist_02;
            break;
          case 3:
            ol_dist_info = s_dist_03;
            break;
          case 4:
            ol_dist_info = s_dist_04;
            break;
          case 5:
            ol_dist_info = s_dist_05;
            break;
          case 6:
            ol_dist_info = s_dist_06;
            break;
          case 7:
            ol_dist_info = s_dist_07;
            break;
          case 8:
            ol_dist_info = s_dist_08;
            break;
          case 9:
            ol_dist_info = s_dist_09;
            break;
          case 10:
            ol_dist_info = s_dist_10;
            break;
        }
        stmtInsertOrderLine.setInt(1, o_id);
        stmtInsertOrderLine.setInt(2, d_id);
        stmtInsertOrderLine.setInt(3, w_id);
        stmtInsertOrderLine.setInt(4, ol_number);
        stmtInsertOrderLine.setInt(5, ol_i_id);
        stmtInsertOrderLine.setInt(6, ol_supply_w_id);
        stmtInsertOrderLine.setInt(7, ol_quantity);
        stmtInsertOrderLine.setDouble(8, ol_amount);
        stmtInsertOrderLine.setString(9, ol_dist_info);
        stmtInsertOrderLine.addBatch();
      } // end-for

      stmtInsertOrderLine.executeBatch();
      stmtUpdateStock.executeBatch();
      total_amount *= (1 + w_tax + d_tax) * (1 - c_discount);
    } catch(UserAbortException userEx)
    {
      LOG.debug("Caught an expected error in New Order");
      throw userEx;
    } finally {
      if (stmtInsertOrderLine != null)
        stmtInsertOrderLine.clearBatch();
      if (stmtUpdateStock != null)
        stmtUpdateStock.clearBatch();
    }
  }

  public void test(Connection conn, TPCCWorker w) throws Exception {
    //initializing all prepared statements
    stmtGetCust=this.getPreparedStatement(conn, stmtGetCustSQL);
    stmtGetWhse=this.getPreparedStatement(conn, stmtGetWhseSQL);
    stmtGetDist=this.getPreparedStatement(conn, stmtGetDistSQL);
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

    int[] orderQts =   { 1,  2,  3,  4,  5,  6,  7,  8,  9, 10};
    int[] qtyArr = {19, 18, 17, 16, 15, 96, 95, 94, 93, 92};
    int[] orderCntArr = {1, 1, 1, 1, 1, 1, 1, 1, 1, 1};
    int[] remoteCntArr = {0, 1, 0, 0, 0, 0, 0, 0, 0, 0};

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

  void AssertNewOrderTransaction(Connection conn, TPCCWorker w,
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
