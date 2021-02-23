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

import java.sql.Connection;
import java.sql.Statement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;


import com.oltpbenchmark.benchmarks.tpcc.procedures.StockLevel;
import com.oltpbenchmark.benchmarks.tpcc.TPCCConfig;
import com.oltpbenchmark.benchmarks.tpcc.TPCCUtil;
import com.zaxxer.hikari.HikariDataSource;

import org.apache.log4j.Logger;

import com.oltpbenchmark.LatencyRecord;
import com.oltpbenchmark.Phase;
import com.oltpbenchmark.SubmittedProcedure;
import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.WorkloadState;
import com.oltpbenchmark.api.Procedure.UserAbortException;
import com.oltpbenchmark.types.DatabaseType;
import com.oltpbenchmark.types.State;
import com.oltpbenchmark.types.TransactionStatus;

public class Worker implements Runnable {
    private static final Logger LOG = Logger.getLogger(Worker.class);

    private static WorkloadState wrkldState;
    // This is the warehouse that is a constant for this terminal.
    protected final int terminalWarehouseID;
    // This is the district ID used for StockLevel transactions.
    protected final int terminalDistrictID;
    // private boolean debugMessages;
    protected final Random gen = new Random();
    private LatencyRecord latencies;
    private LatencyRecord wholeOperationLatencies;
    private LatencyRecord acqConnectionLatencies;
    private final Statement currStatement;

    // Interval requests used by the monitor
    private final AtomicInteger intervalRequests = new AtomicInteger(0);

    private final int id;
    private final BenchmarkModule benchmarkModule;
    protected final HikariDataSource dataSource;
    protected static WorkloadConfiguration wrkld;
    protected TransactionTypes transactionTypes;
    protected Map<Class<? extends Procedure>, Procedure> class_procedures = new HashMap<>();

    private boolean seenDone = false;

    int[] totalRetries;
    int[] totalFailures;
    int totalAttemptsPerTransaction = 1;

    public Worker(BenchmarkModule benchmarkModule, int id, int terminalWarehouseID, int terminalDistrictLowerID,
                  int terminalDistrictUpperID) {
        this.id = id;
        this.benchmarkModule = benchmarkModule;
        // TODO -- can these be made non-static?
        Worker.wrkld = this.benchmarkModule.getWorkloadConfiguration();
        Worker.wrkldState = Worker.wrkld.getWorkloadState();
        this.currStatement = null;
        totalRetries = new int[wrkld.getNumTxnTypes()];
        totalFailures = new int[wrkld.getNumTxnTypes()];
        InitializeProcedures();

        assert (this.transactionTypes != null) : "The TransactionTypes from the WorkloadConfiguration is null!";
        try {
            this.dataSource = this.benchmarkModule.getDataSource();
        } catch (Exception ex) {
            throw new RuntimeException("Failed to connect to database", ex);
        }
        totalAttemptsPerTransaction = wrkld.getMaxRetriesPerTransaction() + 1;
        this.terminalWarehouseID = terminalWarehouseID;

        assert terminalDistrictLowerID >= 1;
        assert terminalDistrictUpperID <= TPCCConfig.configDistPerWhse;
        assert terminalDistrictLowerID <= terminalDistrictUpperID;
        this.terminalDistrictID =
                TPCCUtil.randomNumber(terminalDistrictLowerID,terminalDistrictUpperID, gen);
    }

    public final void InitializeProcedures() {
        // Generate all the Procedures that we're going to need
        this.transactionTypes = Worker.wrkld.getTransTypes();
        for (Entry<TransactionType, Procedure> e : this.benchmarkModule.getProcedures().entrySet()) {
            Procedure proc = e.getValue();
            this.class_procedures.put(proc.getClass(), proc);
        }
    }

    /**
     * Get the BenchmarkModule managing this Worker
     */
    public final BenchmarkModule getBenchmarkModule() {
        return (this.benchmarkModule);
    }

    /**
     * Get the unique thread id for this worker
     */
    public final int getId() {
        return this.id;
    }

    public final int[] getTotalRetries() {
      return totalRetries;
    }

    public final int[] getTotalFailures() {
      return totalFailures;
    }

    @Override
    public String toString() {
        return String.format("%s<%03d>",
                             this.getClass().getSimpleName(), this.getId());
    }

    public final int getRequests() {
        return latencies.size();
    }

    public final int getAndResetIntervalRequests() {
        return intervalRequests.getAndSet(0);
    }

    public final Iterable<LatencyRecord.Sample> getLatencyRecords() {
        return latencies;
    }

    public final Iterable<LatencyRecord.Sample> getWholeOperationLatencyRecords() {
        return wholeOperationLatencies;
    }

    public final Iterable<LatencyRecord.Sample> getAcqConnectionLatencyRecords() {
        return acqConnectionLatencies;
    }

    @SuppressWarnings("unchecked")
    public final <P extends Procedure> P getProcedure(Class<P> procClass) {
        return (P) (this.class_procedures.get(procClass));
    }

    private long getKeyingTimeInMillis(TransactionType type) {
        // TPC-C 5.2.5.2: For keying times for each type of transaction.
        if (type.getName().equals("NewOrder")) {
          return 18000;
        }
        if (type.getName().equals("Payment")) {
          return 3000;
        }
        if (type.getName().equals("OrderStatus")) {
          return 2000;
        }
        if (type.getName().equals("Delivery")) {
          return 2000;
        }
        if (type.getName().equals("StockLevel")) {
          return 2000;
        }
        LOG.error("returning -1 " + type.getName());
        return -1;
    }

    private long getThinkTimeInMillis(TransactionType type) {
        // TPC-C 5.2.5.4: For think times for each type of transaction.
        long mean;
        switch (type.getName()) {
            case "NewOrder":
            case "Payment":
                mean = 12000;
                break;
            case "OrderStatus":
                mean = 10000;
                break;
            case "Delivery":
            case "StockLevel":
                mean = 5000;
                break;
            default:
                LOG.error("returning -1 " + type.getName());
                return -1;
        }

        float c = this.benchmarkModule.rng().nextFloat();
        long thinkTime = (long)(-1 * Math.log(c) * mean);
        if (thinkTime > 10 * mean) {
          thinkTime = 10 * mean;
        }
        return thinkTime;
    }

    /**
     * Stop executing the current statement.
     */
    synchronized public void cancelStatement() {
        try {
            if (this.currStatement != null)
                this.currStatement.cancel();
        } catch (SQLException e) {
            LOG.error("Failed to cancel statement: " + e.getMessage());
        }
    }

    public void test(Connection conn) throws Exception {
      Procedure proc = this.getProcedure(
          this.transactionTypes.getType("NewOrder").getProcedureClass());
      proc.test(conn, this);
    }

    static class TransactionExecutionState {
      private final long startOperation;
      private long endOperation;
      private final long startConnection;
      private long endConnection;
      private final TransactionType type;

      public TransactionExecutionState() {
        this.startOperation = 0;
        this.endOperation = 0;
        this.startConnection = 0;
        this.endConnection = 0;
        this.type = TransactionType.INVALID;
      }

      public TransactionExecutionState(long startOperation, long endOperation,
                                       long startConnection, long endConnection,
                                       TransactionType type) {
        this.startOperation = startOperation;
        this.endOperation = endOperation;
        this.startConnection = startConnection;
        this.endConnection = endConnection;
        this.type = type;
      }

      public long getStartOperation() {
        return startOperation;
      }

      public long getEndOperation() {
        return endOperation;
      }

      public long getStartConnection() {
        return startConnection;
      }

      public long getEndConnection() {
        return endConnection;
      }

      public TransactionType getTransactionType() {
        return type;
      }

      public void setEndOperation(long endOperation) {
        this.endOperation = endOperation;
      }

      public void setEndConnection(long endConnection) {
        this.endConnection = endConnection;
      }
    }

    @Override
    public final void run() {
        Thread t = Thread.currentThread();
        SubmittedProcedure pieceOfWork;
        t.setName(this.toString());

        // In case of reuse reset the measurements
        latencies = new LatencyRecord(wrkldState.getTestStartNs());
        wholeOperationLatencies = new LatencyRecord(wrkldState.getTestStartNs());
        acqConnectionLatencies = new LatencyRecord(wrkldState.getTestStartNs());

        // Invoke the initialize callback
        try {
            this.initialize();
        } catch (Throwable ex) {
            throw new RuntimeException("Unexpected error when initializing " + this, ex);
        }

        // wait for start
        wrkldState.blockForStart();
        State preState, postState;
        Phase phase;

        work: while (true) {

            // PART 1: Init and check if done

            preState = Worker.wrkldState.getGlobalState();

            // Do nothing
            if (preState == State.DONE && !seenDone) {
                // This is the first time we have observed that the
                // test is done. Notify the global test state, then
                // continue applying load
                seenDone = true;
                Worker.wrkldState.signalDone();
                break;
            }

            // PART 2: Wait for work

            // Sleep if there's nothing to do.
            Worker.wrkldState.stayAwake();
            phase = Worker.wrkldState.getCurrentPhase();
            if (phase == null)
                continue;


            // Grab some work and update the state, in case it changed while we
            // waited.

            pieceOfWork = wrkldState.fetchWork(this.id);
            preState = wrkldState.getGlobalState();

            long start = System.nanoTime();

            long keying_time_msecs = 0;
            long think_time_msecs = 0;
            if (Worker.wrkld.getUseKeyingTime()) {
                // Wait for the keying time which is a fixed amount for each type of transaction.
                keying_time_msecs = getKeyingTimeInMillis(transactionTypes.getType(pieceOfWork.getType()));
            }
            if (Worker.wrkld.getUseThinkTime()) {
                think_time_msecs = getThinkTimeInMillis(transactionTypes.getType(pieceOfWork.getType()));
            }

            phase = Worker.wrkldState.getCurrentPhase();
            if (phase == null)
                continue;

            switch (preState) {
                case DONE:
                case EXIT:
                case LATENCY_COMPLETE:
                    // Once a latency run is complete, we wait until the next
                    // phase or until DONE.
                    continue work;
                default:
                    // Do nothing
            }


            // PART 3: Execute work


            // TODO: Measuring latency when not rate limited is ... a little
            // weird because if you add more simultaneous clients, you will
            // increase latency (queue delay) but we do this anyway since it is
            // useful sometimes

            if (keying_time_msecs > 0) {
                try {
                    Thread.sleep(keying_time_msecs);
                } catch (InterruptedException e) {
                    LOG.error("Thread sleep interrupted");
                }
            }

            TransactionExecutionState executionState = new TransactionExecutionState();
            try {
                executionState = doWork(pieceOfWork);
            } catch (IndexOutOfBoundsException e) {
                if (phase.isThroughputRun()) {
                    LOG.error("Thread tried executing disabled phase!");
                    throw e;
                }
                if (phase.id == Worker.wrkldState.getCurrentPhase().id) {
                    switch (preState) {
                        case WARMUP:
                            // Don't quit yet: we haven't even begun!
                            phase.resetSerial();
                            break;
                        case COLD_QUERY:
                        case MEASURE:
                            // The serial phase is over. Finish the run early.
                            wrkldState.signalLatencyComplete();
                            LOG.info("[Serial] Serial execution of all" + " transactions complete.");
                            break;
                        default:
                            throw e;
                    }
                }
            }
           if (think_time_msecs > 0) {
                // Sleep for the think time duration.
                try {
                    Thread.sleep(think_time_msecs);
                } catch (InterruptedException e) {
                    LOG.error("Thread sleep interrupted");
                }
            }

            // In case of transaction failures, the end times will not be populated.
            if (executionState.getEndOperation() == 0 || executionState.getEndConnection() == 0) {
              if (this.wrkldState.getGlobalState() != State.DONE) {
                ++totalFailures[pieceOfWork.getType() - 1];
              }
              continue work;
            }

            // PART 4: Record results

            long end = System.nanoTime();
            postState = wrkldState.getGlobalState();

            switch (postState) {
                case MEASURE:
                    // Non-serial measurement. Only measure if the state both
                    // before and after was MEASURE, and the phase hasn't
                    // changed, otherwise we're recording results for a query
                    // that either started during the warmup phase or ended
                    // after the timer went off.
                    if (preState == State.MEASURE && executionState.getTransactionType() != null &&
                        Worker.wrkldState.getCurrentPhase().id == phase.id) {
                        latencies.addLatency(
                          executionState.getTransactionType().getId(),
                          start, end,
                          executionState.getStartOperation(), executionState.getEndOperation()
                        );
                        acqConnectionLatencies.addLatency(
                          1, start, end,
                          executionState.getStartConnection(),executionState.getEndOperation()
                        );

                        // The latency of the whole operation can be obtained by evaluating the
                        // time from the acquisition of the connection to the completion of the
                        // operation.
                        wholeOperationLatencies.addLatency(
                          executionState.getTransactionType().getId(),
                          start, end,
                          executionState.getStartConnection(), executionState.getEndOperation()
                        );

                        intervalRequests.incrementAndGet();
                    }
                    if (phase.isLatencyRun())
                        Worker.wrkldState.startColdQuery();
                    break;
                case COLD_QUERY:
                    // No recording for cold runs, but next time we will since
                    // it'll be a hot run.
                    if (preState == State.COLD_QUERY)
                        Worker.wrkldState.startHotQuery();
                    break;
                default:
                    // Do nothing
            }


            wrkldState.finishedWork();
        }

        tearDown(false);
    }

    /**
     * Called in a loop in the thread to exercise the system under test. Each
     * implementing worker should return the TransactionType handle that was
     * executed.
     */
    protected final TransactionExecutionState doWork(SubmittedProcedure pieceOfWork) {
        TransactionType next = null;
        long startOperation = 0;
        long endOperation = 0;
        long startConnection = 0;
        long endConnection = 0;

        TransactionStatus status = TransactionStatus.RETRY;
        final DatabaseType dbType = wrkld.getDBType();

        Connection conn;
        try {
            if (next == null) {
                next = transactionTypes.getType(pieceOfWork.getType());
            }
            startConnection = System.nanoTime();

            conn = dataSource.getConnection();
            if (next.getProcedureClass() != StockLevel.class) {
                // In accordance with 2.8.2.3 of the TPCC spec, StockLevel should execute each query in its own Snapshot
                // Isolation.
                conn.setAutoCommit(false);
            }
            conn.setTransactionIsolation(this.wrkld.getIsolationMode());

            endConnection = System.nanoTime();
            int attempt = 0;

            while (status == TransactionStatus.RETRY &&
                   this.wrkldState.getGlobalState() != State.DONE &&
                   ++attempt <= totalAttemptsPerTransaction) {

                if (attempt > 1) {
                    ++totalRetries[pieceOfWork.getType() - 1];
                }
                assert (next.isSupplemental() == false) : "Trying to select a supplemental transaction " + next;

                try {
                    status = TransactionStatus.UNKNOWN;

                    startOperation = System.nanoTime();
                    status = this.executeWork(conn, next);
                    endOperation = System.nanoTime();

                // User Abort Handling
                // These are not errors
                } catch (UserAbortException ex) {
                    if (LOG.isDebugEnabled())
                        LOG.trace(next + " Aborted", ex);
                    if (!conn.getAutoCommit()) {
                        conn.rollback();
                    }
                    status = TransactionStatus.USER_ABORTED;
                    break;

                // Database System Specific Exception Handling
                } catch (SQLException ex) {
                    // TODO: Handle acceptable error codes for every DBMS
                     if (LOG.isDebugEnabled())
                        LOG.warn(String.format("%s thrown when executing '%s' on '%s' " +
                                               "[Message='%s', ErrorCode='%d', SQLState='%s']",
                                               ex.getClass().getSimpleName(), next, this.toString(),
                                               ex.getMessage(), ex.getErrorCode(), ex.getSQLState()), ex);

		    if (!conn.getAutoCommit()) {
                conn.rollback();
		    }

                    if (ex.getSQLState() == null) {
                        continue;
                    // ------------------
                    // POSTGRES
                    // ------------------
                    } else if (ex.getErrorCode() == 0 && ex.getSQLState() != null && ex.getSQLState().equals("40001")) {
                        // Postgres serialization
                        status = TransactionStatus.RETRY;
                        continue;
                    } else if (ex.getErrorCode() == 0 && ex.getSQLState() != null && ex.getSQLState().equals("53200")) {
                        // Postgres OOM error
                        throw ex;
                    } else if (ex.getErrorCode() == 0 && ex.getSQLState() != null && ex.getSQLState().equals("XX000")) {
                        // Postgres no pinned buffers available
                        throw ex;
                    // ------------------
                    // UNKNOWN!
                    // ------------------
                    } else {
                        // UNKNOWN: In this case .. Retry as well!
                        LOG.warn("The DBMS rejected the transaction without an error code", ex);
                        continue;
                        // FIXME Disable this for now
                        // throw ex;
                    }
                // Assertion Error
                } catch (Error ex) {
                    LOG.error("Fatal error when invoking " + next, ex);
                    throw ex;
                 // Random Error
                } catch (Exception ex) {
                    LOG.error("Fatal error when invoking " + next, ex);
                    throw new RuntimeException(ex);

                } finally {
                     if (LOG.isDebugEnabled())
                        LOG.debug(String.format("%s %s Result: %s", this, next, status));

                    switch (status) {
                        case SUCCESS:
                            break;
                        case RETRY_DIFFERENT:
                            break;
                        case USER_ABORTED:
                            break;
                        case RETRY:
                            continue;
                        default:
                            assert (false) : String.format("Unexpected status '%s' for %s", status, next);
                    } // SWITCH
                }

            } // WHILE
            conn.close();
        } catch (SQLException ex) {
            String msg = String.format("Unexpected fatal, error in '%s' when executing '%s' [%s]",
                                       this, next, dbType);
            if (dbType == DatabaseType.NOISEPAGE) {
                msg += "\nBut we are not stopping because " + dbType + " cannot handle this correctly";
                LOG.warn(msg);
            } else {
                throw new RuntimeException(msg, ex);
            }
        }

        return new TransactionExecutionState(startOperation, endOperation,
                startConnection, endConnection,
                next);
    }

    /**
     * Optional callback that can be used to initialize the Worker right before
     * the benchmark execution begins
     */
    protected void initialize() {
        // The default is to do nothing
    }

    /**
     * Executes a single TPCC transaction of type transactionType.
     */
    protected TransactionStatus executeWork(Connection conn, TransactionType nextTransaction)
                                            throws UserAbortException, SQLException {
      try {
        Procedure proc = this.getProcedure(nextTransaction.getProcedureClass());

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
      conn.commit();
      return (TransactionStatus.SUCCESS);
    }

    /**
     * Called at the end of the test to do any clean up that may be required.
     */
    public void tearDown(boolean error) { }

    public void initializeState() {
        assert (Worker.wrkldState == null);
        Worker.wrkldState = Worker.wrkld.getWorkloadState();
    }
}
