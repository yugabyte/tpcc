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

/*
 * jTPCCTerminal - Terminal emulator code for jTPCC (transactions)
 *
 * Copyright (C) 2003, Raul Barbosa
 * Copyright (C) 2004-2006, Denis Lussier
 *
 */

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Random;

import org.apache.log4j.Logger;

import com.oltpbenchmark.api.Procedure.UserAbortException;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.tpcc.procedures.TPCCProcedure;
import com.oltpbenchmark.types.TransactionStatus;

public class TPCCWorker extends Worker<TPCCBenchmark> {

  private static final Logger LOG = Logger.getLogger(TPCCWorker.class);

  // This is the warehouse that is a constant for this terminal.
  private final int terminalWarehouseID;
  // This is the district ID used for StockLevel transactions.
  private final int terminalDistrictID;
  // private boolean debugMessages;
  private final Random gen = new Random();

  public TPCCWorker(TPCCBenchmark benchmarkModule, int id,
                    int terminalWarehouseID, int terminalDistrictLowerID,
                    int terminalDistrictUpperID) {
    super(benchmarkModule, id);

    this.terminalWarehouseID = terminalWarehouseID;
    assert terminalDistrictLowerID >= 1;
    assert terminalDistrictUpperID <= TPCCConfig.configDistPerWhse;
    assert terminalDistrictLowerID <= terminalDistrictUpperID;
    this.terminalDistrictID =
      TPCCUtil.randomNumber(terminalDistrictLowerID,terminalDistrictUpperID, gen);
  }

  /**
   * Executes a single TPCC transaction of type transactionType.
   */
  @Override
  protected TransactionStatus executeWork(Connection conn, TransactionType nextTransaction)
                                          throws UserAbortException, SQLException {
    try {
      TPCCProcedure proc = (TPCCProcedure) this.getProcedure(nextTransaction.getProcedureClass());

      // The districts should be chosen randomly from [1,10] for the following transactions:
      // 1. NewOrder    TPC-C 2.4.1.2
      // 2. Payment     TPC-C 2.5.1.2
      // 3. OrderStatus TPC-C 2.6.1.2
      // The 'StockLevel' transaction has a fixed districtId for the whole terminal.
      int lowDistrictId = terminalDistrictID;
      int highDistrictId = terminalDistrictID;
      if (nextTransaction.getName().equals("NewOrder") ||
          nextTransaction.getName().equals("Payment") ||
          nextTransaction.getName().equals("OrderStatus")) {
        lowDistrictId = 1;
        highDistrictId = TPCCConfig.configDistPerWhse;
      }
      proc.run(conn, gen, terminalWarehouseID, wrkld.getTotalWarehousesAcrossShards(),
              lowDistrictId, highDistrictId, this);
    } catch (ClassCastException ex){
      //fail gracefully
      LOG.error("We have been invoked with an INVALID transactionType?!");
      throw new RuntimeException("Bad transaction type = "+ nextTransaction);
    }
    if (!conn.getAutoCommit()) {
      conn.commit();
    }
    return (TransactionStatus.SUCCESS);
  }

  public void test(Connection conn) throws Exception {
    TPCCProcedure proc = (TPCCProcedure) this.getProcedure(
        this.transactionTypes.getType("NewOrder").getProcedureClass());
    proc.test(conn, this);
  }
}
