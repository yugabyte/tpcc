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

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import com.oltpbenchmark.benchmarks.tpcc.*;
import com.oltpbenchmark.benchmarks.tpcc.pojo.*;
import com.oltpbenchmark.schema.SchemaManager;
import com.oltpbenchmark.schema.SchemaManagerFactory;
import com.oltpbenchmark.schema.Table;

import org.apache.log4j.Logger;

import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.util.Histogram;

/**
 * @author pavlo
 */
public class Loader {
    private static final Logger LOG = Logger.getLogger(Loader.class);
    private static final int FIRST_UNPROCESSED_O_ID = 2101;

    protected final BenchmarkModule benchmark;
    protected final WorkloadConfiguration workConf;
    protected final int numWarehouses;
    private final Histogram<String> tableSizes = new Histogram<>();

    /**
     * A LoaderThread is responsible for loading some portion of a
     * benchmark's database.
     * Note that each LoaderThread has its own database Connection handle.
     */
    public abstract class LoaderThread implements Runnable {
        public LoaderThread() {}

        @Override
        public final void run() {
            try {
                Connection conn = Loader.this.benchmark.makeConnection();
                conn.setAutoCommit(false);

                this.load(conn);
                conn.commit();
                conn.close();
            } catch (SQLException ex) {
                SQLException next_ex = ex.getNextException();
                String msg = String.format("Unexpected error when loading %s database",
                                           Loader.this.benchmark.getBenchmarkName().toUpperCase());
                LOG.error(msg, next_ex);
                throw new RuntimeException(ex);
            }
        }

        /**
         * This is the method that each LoaderThread has to implement
         */
        public abstract void load(Connection conn);
    }

    public Loader(BenchmarkModule benchmark) {
        this.benchmark = benchmark;
        this.workConf = benchmark.getWorkloadConfiguration();
        this.numWarehouses = workConf.getNumWarehouses();
    }

    /**
     * Create a list of loader threads to load TPCC data. One thread will load items, and then every other thread will
     * load all the data for a given warehouse.
     *
     * @return The list of LoaderThreads to run in order to load all TPCC data.
     */
    public List<LoaderThread> createLoaderThreads() {
      List<LoaderThread> threads = new ArrayList<>();
      final CountDownLatch warehouseLatch =  new CountDownLatch(numWarehouses);

      // ITEM
      // This will be invoked first and executed in a single thread.
      threads.add(new LoaderThread() {
        @Override
        public void load(Connection conn) {
          if (!workConf.getEnableForeignKeysAfterLoad() && workConf.getShouldEnableForeignKeys()) {
            EnableForeignKeyConstraints(conn);
          }
          if (workConf.getStartWarehouseIdForShard() == 1) {
            loadItems(conn);
          }
        }
      });

      // WAREHOUSES
      // We use a separate thread per warehouse. Each thread will load
      // all of the tables that depend on that warehouse. They all have
      // to wait until the ITEM table is loaded first though.
      for (int w = workConf.getStartWarehouseIdForShard();
           w < workConf.getStartWarehouseIdForShard() + numWarehouses;
           w++) {
        final int w_id = w;
        LoaderThread t = new LoaderThread() {
          @Override
          public void load(Connection conn) {
            if (LOG.isDebugEnabled()) LOG.debug("Starting to load WAREHOUSE " + w_id);

            // WAREHOUSE
            loadWarehouse(conn, w_id);

            // STOCK
            loadStock(conn, w_id);

            // DISTRICT
            loadDistricts(conn, w_id);

            // CUSTOMER
            loadCustomers(conn, w_id);

            // ORDERS
            loadOrders(conn, w_id);

            warehouseLatch.countDown();
          }
        };
        threads.add(t);
      } // FOR

      threads.add(new LoaderThread() {
          @Override
          public void load(Connection conn) {
              try {
                  warehouseLatch.await();
              } catch (InterruptedException ex) {
                  ex.printStackTrace();
                  throw new RuntimeException(ex);
              }
              if (workConf.getEnableForeignKeysAfterLoad() &&
                  workConf.getShouldEnableForeignKeys()) {
                EnableForeignKeyConstraints(conn);
              }
          }
      });
      return (threads);
    }

    public Histogram<String> getTableCounts() {
        return (this.tableSizes);
    }

    private PreparedStatement getInsertStatement(Connection conn, String tableName) throws SQLException {
      return conn.prepareStatement(Table.getInsertDml(tableName));
    }

    protected void transRollback(Connection conn) {
      try {
        conn.rollback();
      } catch (SQLException se) {
        LOG.debug(se.getMessage());
      }
    }

    private void performInsertsWithRetries(Connection conn, PreparedStatement... stmts) throws Exception {
      int attempts = workConf.getMaxLoaderRetries() + 1;
      SQLException failure = null;
      for (int i = 0; i < attempts; ++i) {
        try {
           for (PreparedStatement stmt: stmts) {
              stmt.executeBatch();
              stmt.clearBatch();
           }
            conn.commit();
            return;
        } catch (SQLException ex) {
          LOG.warn("Fail to load batch with error: " + ex.getMessage());
          failure = ex;
          transRollback(conn);
        }
      }
      if (failure != null) {
        LOG.error("Failed to load data for TPCC", failure);
        throw failure;
      }
    }

    public void EnableForeignKeyConstraints(Connection conn) {
      try {
        SchemaManager schemaManager = SchemaManagerFactory.getSchemaManager(workConf, conn);
        schemaManager.enableForeignKeyConstraints();
      } catch (SQLException se) {
        LOG.error("Could not create foreign keys" + se.getMessage());
        transRollback(conn);
      }
    }

    protected void loadItems(Connection conn) {
      try {
        PreparedStatement itemPrepStmt = getInsertStatement(conn, TPCCConstants.TABLENAME_ITEM);
        Item item = new Item();
        int batchSize = 0;
        for (int i = 1; i <= TPCCConfig.configItemCount; i++) {
          item.i_id = i;
          item.i_name = TPCCUtil.randomStr(TPCCUtil.randomNumber(14, 24, benchmark.rng()));
          item.i_price = TPCCUtil.randomNumber(100, 10000, benchmark.rng()) / 100.0;
          // i_data
          int randPct = TPCCUtil.randomNumber(1, 100, benchmark.rng());
          int len = TPCCUtil.randomNumber(26, 50, benchmark.rng());
          if (randPct > 10) {
            // 90% of time i_data isa random string of length [26 .. 50]
            item.i_data = TPCCUtil.randomStr(len);
          } else {
            // 10% of time i_data has "ORIGINAL" crammed somewhere in
            // middle
            int startORIGINAL = TPCCUtil.randomNumber(2, (len - 8), benchmark.rng());
            item.i_data = TPCCUtil.randomStr(startORIGINAL - 1) + "ORIGINAL" +
                          TPCCUtil.randomStr(len - startORIGINAL - 9);
          }

          item.i_im_id = TPCCUtil.randomNumber(1, 10000, benchmark.rng());

          int idx = 0;
          itemPrepStmt.setLong(++idx, item.i_id);
          itemPrepStmt.setString(++idx, item.i_name);
          itemPrepStmt.setDouble(++idx, item.i_price);
          itemPrepStmt.setString(++idx, item.i_data);
          itemPrepStmt.setLong(++idx, item.i_im_id);
          itemPrepStmt.addBatch();
          batchSize++;

          if (batchSize == workConf.getBatchSize()) {
            performInsertsWithRetries(conn, itemPrepStmt);
            batchSize = 0;
          }
        }
        if (batchSize > 0)
            performInsertsWithRetries(conn, itemPrepStmt);

      } catch (Exception ex) {
        LOG.error("Failed to load data for TPC-C", ex);
        transRollback(conn);
      }

    } // end loadItem()

    protected void loadWarehouse(Connection conn, int w_id) {
      try {
        PreparedStatement whsePrepStmt = getInsertStatement(conn, TPCCConstants.TABLENAME_WAREHOUSE);
        Warehouse warehouse = new Warehouse();

        warehouse.w_id = w_id;
        warehouse.w_ytd = 300000;

        // random within [0.0000 .. 0.2000]
        warehouse.w_tax = TPCCUtil.randomNumber(0, 2000, benchmark.rng()) / 10000.0;
        warehouse.w_name = TPCCUtil.randomStr(TPCCUtil.randomNumber(6, 10, benchmark.rng()));
        warehouse.w_street_1 = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, benchmark.rng()));
        warehouse.w_street_2 = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, benchmark.rng()));
        warehouse.w_city = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, benchmark.rng()));
        warehouse.w_state = TPCCUtil.randomStr(2).toUpperCase();
        warehouse.w_zip = TPCCUtil.randomNStr(4) + "11111";

        int idx = 0;
        whsePrepStmt.setLong(++idx, warehouse.w_id);
        whsePrepStmt.setDouble(++idx, warehouse.w_ytd);
        whsePrepStmt.setDouble(++idx, warehouse.w_tax);
        whsePrepStmt.setString(++idx, warehouse.w_name);
        whsePrepStmt.setString(++idx, warehouse.w_street_1);
        whsePrepStmt.setString(++idx, warehouse.w_street_2);
        whsePrepStmt.setString(++idx, warehouse.w_city);
        whsePrepStmt.setString(++idx, warehouse.w_state);
        whsePrepStmt.setString(++idx, warehouse.w_zip);
        whsePrepStmt.execute();

        performInsertsWithRetries(conn, whsePrepStmt);
      } catch (Exception ex) {
        LOG.error("Failed to load data for TPC-C", ex);
        transRollback(conn);
      }
    } // end loadWhse()

    private void loadStock(Connection conn, int w_id) {
      int k = 0;
      try {
        PreparedStatement stckPrepStmt = getInsertStatement(conn, TPCCConstants.TABLENAME_STOCK);
        Stock stock = new Stock();
        for (int i = 1; i <= TPCCConfig.configItemCount; i++) {
          stock.s_i_id = i;
          stock.s_w_id = w_id;
          stock.s_quantity = TPCCUtil.randomNumber(10, 100, benchmark.rng());
          stock.s_ytd = 0;
          stock.s_order_cnt = 0;
          stock.s_remote_cnt = 0;

          // s_data
          int randPct = TPCCUtil.randomNumber(1, 100, benchmark.rng());
          int len = TPCCUtil.randomNumber(26, 50, benchmark.rng());
          if (randPct > 10) {
            // 90% of time i_data isa random string of length [26 ..
            // 50]
            stock.s_data = TPCCUtil.randomStr(len);
          } else {
            // 10% of time i_data has "ORIGINAL" crammed somewhere
            // in middle
            int startORIGINAL = TPCCUtil
                .randomNumber(2, (len - 8), benchmark.rng());
            stock.s_data = TPCCUtil.randomStr(startORIGINAL - 1)
                + "ORIGINAL"
                + TPCCUtil.randomStr(len - startORIGINAL - 9);
          }

          k++;
          int idx = 0;
          stckPrepStmt.setLong(++idx, stock.s_w_id);
          stckPrepStmt.setLong(++idx, stock.s_i_id);
          stckPrepStmt.setLong(++idx, stock.s_quantity);
          stckPrepStmt.setDouble(++idx, stock.s_ytd);
          stckPrepStmt.setLong(++idx, stock.s_order_cnt);
          stckPrepStmt.setLong(++idx, stock.s_remote_cnt);
          stckPrepStmt.setString(++idx, stock.s_data);
          stckPrepStmt.setString(++idx, TPCCUtil.randomStr(24));
          stckPrepStmt.setString(++idx, TPCCUtil.randomStr(24));
          stckPrepStmt.setString(++idx, TPCCUtil.randomStr(24));
          stckPrepStmt.setString(++idx, TPCCUtil.randomStr(24));
          stckPrepStmt.setString(++idx, TPCCUtil.randomStr(24));
          stckPrepStmt.setString(++idx, TPCCUtil.randomStr(24));
          stckPrepStmt.setString(++idx, TPCCUtil.randomStr(24));
          stckPrepStmt.setString(++idx, TPCCUtil.randomStr(24));
          stckPrepStmt.setString(++idx, TPCCUtil.randomStr(24));
          stckPrepStmt.setString(++idx, TPCCUtil.randomStr(24));
          stckPrepStmt.addBatch();
          if ((k % workConf.getBatchSize()) == 0) {
            performInsertsWithRetries(conn, stckPrepStmt);
          }
        } // end for [i]
        performInsertsWithRetries(conn, stckPrepStmt);
      } catch (Exception ex) {
        LOG.error("Failed to load data for TPC-C", ex);
        transRollback(conn);
      }
    } // end loadStock()

    private void loadDistricts(Connection conn, int w_id) {
      try {
        PreparedStatement distPrepStmt = getInsertStatement(conn, TPCCConstants.TABLENAME_DISTRICT);
        District district = new District();

        for (int d = 1; d <= TPCCConfig.configDistPerWhse; d++) {
          district.d_id = d;
          district.d_w_id = w_id;
          district.d_ytd = 30000;

          // random within [0.0000 .. 0.2000]
          district.d_tax = (float) ((TPCCUtil.randomNumber(0, 2000, benchmark.rng())) / 10000.0);

          district.d_next_o_id = TPCCConfig.configCustPerDist + 1;
          district.d_name = TPCCUtil.randomStr(TPCCUtil.randomNumber(6, 10, benchmark.rng()));
          district.d_street_1 = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, benchmark.rng()));
          district.d_street_2 = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, benchmark.rng()));
          district.d_city = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, benchmark.rng()));
          district.d_state = TPCCUtil.randomStr(2).toUpperCase();
          district.d_zip = TPCCUtil.randomNStr(4) + "11111";

          int idx = 0;
          distPrepStmt.setLong(++idx, district.d_w_id);
          distPrepStmt.setLong(++idx, district.d_id);
          distPrepStmt.setDouble(++idx, district.d_ytd);
          distPrepStmt.setDouble(++idx, district.d_tax);
          distPrepStmt.setLong(++idx, district.d_next_o_id);
          distPrepStmt.setString(++idx, district.d_name);
          distPrepStmt.setString(++idx, district.d_street_1);
          distPrepStmt.setString(++idx, district.d_street_2);
          distPrepStmt.setString(++idx, district.d_city);
          distPrepStmt.setString(++idx, district.d_state);
          distPrepStmt.setString(++idx, district.d_zip);
          distPrepStmt.addBatch();
        } // end for [d]
        performInsertsWithRetries(conn, distPrepStmt);
      } catch (Exception e) {
        LOG.error("Failed to load data for TPC-C", e);
        transRollback(conn);
      }
    } // end loadDist()

    private void loadCustomers(Connection conn, int w_id) {
      int k = 0;
      Customer customer = new Customer();
      History history = new History();

      try {
        PreparedStatement custPrepStmt = getInsertStatement(conn, TPCCConstants.TABLENAME_CUSTOMER);
        PreparedStatement histPrepStmt = getInsertStatement(conn, TPCCConstants.TABLENAME_HISTORY);

        for (int d = 1; d <= TPCCConfig.configDistPerWhse; d++) {
          for (int c = 1; c <= TPCCConfig.configCustPerDist; c++) {
            Timestamp sysdate = this.benchmark.getTimestamp(System.currentTimeMillis());

            customer.c_id = c;
            customer.c_d_id = d;
            customer.c_w_id = w_id;

            // discount is random between [0.0000 ... 0.5000]
            customer.c_discount = (float) (TPCCUtil.randomNumber(0, 5000, benchmark.rng()) / 10000.0);

            if (TPCCUtil.randomNumber(1, 100, benchmark.rng()) <= 10) {
              customer.c_credit = "BC"; // 10% Bad Credit
            } else {
              customer.c_credit = "GC"; // 90% Good Credit
            }
            if (c <= 1000) {
              customer.c_last = TPCCUtil.getLastName(c - 1);
            } else {
              customer.c_last = TPCCUtil.getNonUniformRandomLastNameForLoad(benchmark.rng());
            }
            customer.c_first = TPCCUtil.randomStr(TPCCUtil.randomNumber(8, 16, benchmark.rng()));
            customer.c_credit_lim = (float)50000.0;

            customer.c_balance = (float)-10.0;
            customer.c_ytd_payment = (float)10.0;
            customer.c_payment_cnt = 1;
            customer.c_delivery_cnt = 0;

            customer.c_street_1 = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, benchmark.rng()));
            customer.c_street_2 = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, benchmark.rng()));
            customer.c_city = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, benchmark.rng()));
            customer.c_state = TPCCUtil.randomStr(2).toUpperCase();
            // TPC-C 4.3.2.7: 4 random digits + "11111"
            customer.c_zip = TPCCUtil.randomNStr(4) + "11111";
            customer.c_phone = TPCCUtil.randomNStr(16);
            customer.c_since = sysdate;
            customer.c_middle = "OE";
            customer.c_data = TPCCUtil.randomStr(TPCCUtil.randomNumber(300, 500, benchmark.rng()));

            history.h_c_id = c;
            history.h_c_d_id = d;
            history.h_c_w_id = w_id;
            history.h_d_id = d;
            history.h_w_id = w_id;
            history.h_date = sysdate;
            history.h_amount = (float)10.0;
            history.h_data = TPCCUtil.randomStr(TPCCUtil
                .randomNumber(10, 24, benchmark.rng()));

            k = k + 2;
            int idx = 0;
            custPrepStmt.setLong(++idx, customer.c_w_id);
            custPrepStmt.setLong(++idx, customer.c_d_id);
            custPrepStmt.setLong(++idx, customer.c_id);
            custPrepStmt.setDouble(++idx, customer.c_discount);
            custPrepStmt.setString(++idx, customer.c_credit);
            custPrepStmt.setString(++idx, customer.c_last);
            custPrepStmt.setString(++idx, customer.c_first);
            custPrepStmt.setDouble(++idx, customer.c_credit_lim);
            custPrepStmt.setDouble(++idx, customer.c_balance);
            custPrepStmt.setDouble(++idx, customer.c_ytd_payment);
            custPrepStmt.setLong(++idx, customer.c_payment_cnt);
            custPrepStmt.setLong(++idx, customer.c_delivery_cnt);
            custPrepStmt.setString(++idx, customer.c_street_1);
            custPrepStmt.setString(++idx, customer.c_street_2);
            custPrepStmt.setString(++idx, customer.c_city);
            custPrepStmt.setString(++idx, customer.c_state);
            custPrepStmt.setString(++idx, customer.c_zip);
            custPrepStmt.setString(++idx, customer.c_phone);
            custPrepStmt.setTimestamp(++idx, customer.c_since);
            custPrepStmt.setString(++idx, customer.c_middle);
            custPrepStmt.setString(++idx, customer.c_data);
            custPrepStmt.addBatch();

            idx = 0;
            histPrepStmt.setInt(++idx, history.h_c_id);
            histPrepStmt.setInt(++idx, history.h_c_d_id);
            histPrepStmt.setInt(++idx, history.h_c_w_id);
            histPrepStmt.setInt(++idx, history.h_d_id);
            histPrepStmt.setInt(++idx, history.h_w_id);
            histPrepStmt.setTimestamp(++idx, history.h_date);
            histPrepStmt.setDouble(++idx, history.h_amount);
            histPrepStmt.setString(++idx, history.h_data);
            histPrepStmt.addBatch();

            if ((k % workConf.getBatchSize()) == 0) {
              performInsertsWithRetries(conn, custPrepStmt, histPrepStmt);
            }
          } // end for [c]
        } // end for [d]
        performInsertsWithRetries(conn, custPrepStmt, histPrepStmt);

      } catch (Exception e) {
        LOG.error("Failed to load data for TPC-C", e);
        transRollback(conn);
      }
    } // end loadCust()

    private void loadOrders(Connection conn, int w_id) {
      int k = 0;
      int t = 0;
      int newOrderBatch = 0;
      try {
        PreparedStatement ordrPrepStmt = getInsertStatement(conn, TPCCConstants.TABLENAME_OPENORDER);
        PreparedStatement nworPrepStmt = getInsertStatement(conn, TPCCConstants.TABLENAME_NEWORDER);
        PreparedStatement orlnPrepStmt = getInsertStatement(conn, TPCCConstants.TABLENAME_ORDERLINE);

        Oorder oorder = new Oorder();
        NewOrder new_order = new NewOrder();
        OrderLine order_line = new OrderLine();

        for (int d = 1; d <= TPCCConfig.configDistPerWhse; d++) {
          // TPC-C 4.3.3.1: o_c_id must be a permutation of [1, 3000]
          int[] c_ids = new int[TPCCConfig.configCustPerDist];
          for (int i = 0; i < TPCCConfig.configCustPerDist; ++i) {
            c_ids[i] = i + 1;
          }
          // Collections.shuffle exists, but there is no
          // Arrays.shuffle
          for (int i = 0; i < c_ids.length - 1; ++i) {
            int remaining = c_ids.length - i - 1;
            int swapIndex = benchmark.rng().nextInt(remaining) + i + 1;
            assert i < swapIndex;
            int temp = c_ids[swapIndex];
            c_ids[swapIndex] = c_ids[i];
            c_ids[i] = temp;
          }

          for (int c = 1; c <= TPCCConfig.configCustPerDist; c++) {
            oorder.o_id = c;
            oorder.o_w_id = w_id;
            oorder.o_d_id = d;
            oorder.o_c_id = c_ids[c - 1];
            // o_carrier_id is set *only* for orders with ids < 2101
            // [4.3.3.1]
            if (oorder.o_id < FIRST_UNPROCESSED_O_ID) {
              oorder.o_carrier_id = TPCCUtil.randomNumber(1, 10, benchmark.rng());
            } else {
              oorder.o_carrier_id = null;
            }
            oorder.o_ol_cnt = TPCCUtil.randomNumber(5, 15, benchmark.rng());
            oorder.o_all_local = 1;
            oorder.o_entry_d = this.benchmark.getTimestamp(System.currentTimeMillis());

            k++;
            int idx = 0;
            ordrPrepStmt.setInt(++idx, oorder.o_w_id);
            ordrPrepStmt.setInt(++idx, oorder.o_d_id);
            ordrPrepStmt.setInt(++idx, oorder.o_id);
            ordrPrepStmt.setInt(++idx, oorder.o_c_id);
            if (oorder.o_carrier_id != null) {
                ordrPrepStmt.setInt(++idx, oorder.o_carrier_id);
            } else {
                ordrPrepStmt.setNull(++idx, Types.INTEGER);
            }
            ordrPrepStmt.setInt(++idx, oorder.o_ol_cnt);
            ordrPrepStmt.setInt(++idx, oorder.o_all_local);
            ordrPrepStmt.setTimestamp(++idx, oorder.o_entry_d);
            ordrPrepStmt.addBatch();

            // 900 rows in the NEW-ORDER table corresponding to the last
            // 900 rows in the ORDER table for that district (i.e.,
            // with NO_O_ID between 2,101 and 3,000)
            if (c >= FIRST_UNPROCESSED_O_ID) {
              new_order.no_w_id = w_id;
              new_order.no_d_id = d;
              new_order.no_o_id = c;

              k++;
              idx = 0;
              nworPrepStmt.setInt(++idx, new_order.no_w_id);
              nworPrepStmt.setInt(++idx, new_order.no_d_id);
              nworPrepStmt.setInt(++idx, new_order.no_o_id);
              nworPrepStmt.addBatch();
              newOrderBatch++;
            } // end new order

            for (int l = 1; l <= oorder.o_ol_cnt; l++) {
              order_line.ol_w_id = w_id;
              order_line.ol_d_id = d;
              order_line.ol_o_id = c;
              order_line.ol_number = l; // ol_number
              order_line.ol_i_id = TPCCUtil.randomNumber(1,
                      TPCCConfig.configItemCount, benchmark.rng());
              if (order_line.ol_o_id < FIRST_UNPROCESSED_O_ID) {
                order_line.ol_delivery_d = oorder.o_entry_d;
                order_line.ol_amount = 0;
              } else {
                order_line.ol_delivery_d = null;
                // random within [0.01 .. 9,999.99]
                order_line.ol_amount =
                  (float) (TPCCUtil.randomNumber(1, 999999, benchmark.rng()) / 100.0);
              }
              order_line.ol_supply_w_id = order_line.ol_w_id;
              order_line.ol_quantity = 5;
              order_line.ol_dist_info = TPCCUtil.randomStr(24);

              k++;
              idx = 0;
              orlnPrepStmt.setInt(++idx, order_line.ol_w_id);
              orlnPrepStmt.setInt(++idx, order_line.ol_d_id);
              orlnPrepStmt.setInt(++idx, order_line.ol_o_id);
              orlnPrepStmt.setInt(++idx, order_line.ol_number);
              orlnPrepStmt.setLong(++idx, order_line.ol_i_id);
              if (order_line.ol_delivery_d != null) {
                  orlnPrepStmt.setTimestamp(++idx, order_line.ol_delivery_d);
              } else {
                  orlnPrepStmt.setNull(++idx, 0);
              }
              orlnPrepStmt.setDouble(++idx, order_line.ol_amount);
              orlnPrepStmt.setLong(++idx, order_line.ol_supply_w_id);
              orlnPrepStmt.setDouble(++idx, order_line.ol_quantity);
              orlnPrepStmt.setString(++idx, order_line.ol_dist_info);
              orlnPrepStmt.addBatch();

              if ((k % workConf.getBatchSize()) == 0) {
                if (newOrderBatch > 0) {
                  performInsertsWithRetries(conn, ordrPrepStmt, nworPrepStmt, orlnPrepStmt);
                  newOrderBatch = 0;
                } else {
                  performInsertsWithRetries(conn, ordrPrepStmt, orlnPrepStmt);
                }
              }
            } // end for [l]
          } // end for [c]
        } // end for [d]

        if (LOG.isDebugEnabled())
          LOG.debug("  Writing final records " + k + " of " + t);
        performInsertsWithRetries(conn, ordrPrepStmt, nworPrepStmt, orlnPrepStmt);
      } catch (Exception e) {
        LOG.error("Failed to load data for TPC-C", e);
        transRollback(conn);
      }
    } // end loadOrder()

    public void unload(Connection conn) throws SQLException {
      conn.setAutoCommit(false);
      conn.setTransactionIsolation(workConf.getIsolationMode());
      Statement st = conn.createStatement();

      st.execute("DROP TABLE IF EXISTS NEW_ORDER CASCADE");
      st.execute("DROP TABLE IF EXISTS ORDER_LINE CASCADE");
      st.execute("DROP TABLE IF EXISTS OORDER CASCADE");
      st.execute("DROP TABLE IF EXISTS HISTORY CASCADE");
      st.execute("DROP TABLE IF EXISTS STOCK CASCADE");
      st.execute("DROP TABLE IF EXISTS ITEM CASCADE");
      st.execute("DROP TABLE IF EXISTS CUSTOMER CASCADE");
      st.execute("DROP TABLE IF EXISTS DISTRICT CASCADE");
      st.execute("DROP TABLE IF EXISTS WAREHOUSE CASCADE");
    }
}
