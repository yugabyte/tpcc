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
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;


import com.oltpbenchmark.*;
import com.oltpbenchmark.benchmarks.tpcc.TPCCConfig;
import com.oltpbenchmark.benchmarks.tpcc.TPCCUtil;
import com.oltpbenchmark.benchmarks.tpcc.procedures.NewOrder;
import com.oltpbenchmark.benchmarks.tpcc.procedures.StockLevel;
import com.oltpbenchmark.util.Pair;
import com.zaxxer.hikari.HikariDataSource;

import org.apache.log4j.Logger;

import com.oltpbenchmark.api.Procedure.UserAbortException;
import com.oltpbenchmark.types.State;
import com.oltpbenchmark.types.TransactionStatus;

public class Worker implements Runnable {
    private static final Logger LOG = Logger.getLogger(Worker.class);

    private static WorkloadState wrkldState;
    // This is the warehouse that is a constant for this terminal.
    private final int terminalWarehouseID;
    // This is the district ID used for StockLevel transactions.
    private final int terminalDistrictID;
    // private boolean debugMessages;
    protected final Random gen = new Random();
    private TransactionLatencyRecord latencies;
    private TransactionLatencyRecord failureLatencies;
    private WorkerTaskLatencyRecord workerTaskLatencyRecord;

    private final Statement currStatement;

    // Interval requests used by the monitor
    private final AtomicInteger intervalRequests = new AtomicInteger(0);

    private final int id;
    private final BenchmarkModule benchmarkModule;
    protected final HikariDataSource dataSource;
    protected static WorkloadConfiguration wrkld;
    protected TransactionTypes transactionTypes;
    protected final Map<Class<? extends Procedure>, Procedure> class_procedures = new HashMap<>();

    private boolean seenDone = false;
    int[][] totalFailedTries;
    int totalAttemptsPerTransaction;

    public Worker(
            BenchmarkModule benchmarkModule, int id, int terminalWarehouseID, int terminalDistrictLowerID,
            int terminalDistrictUpperID) {
        this.id = id;
        this.benchmarkModule = benchmarkModule;
        // TODO -- can these be made non-static?
        Worker.wrkld = this.benchmarkModule.getWorkloadConfiguration();
        Worker.wrkldState = Worker.wrkld.getWorkloadState();
        this.currStatement = null;
        InitializeProcedures();

        assert (this.transactionTypes != null) : "The TransactionTypes from the WorkloadConfiguration is null!";
        try {
            this.dataSource = this.benchmarkModule.getDataSource();
        } catch (Exception ex) {
            throw new RuntimeException("Failed to connect to database", ex);
        }
        totalAttemptsPerTransaction = wrkld.getMaxRetriesPerTransaction() + 1;
        totalFailedTries = new int[wrkld.getNumTxnTypes()][totalAttemptsPerTransaction];
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

    public final int[][] getTotalFailedTries() {
      return totalFailedTries;
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

    public final Iterable<LatencyRecord.Sample> getFailureLatencyRecords() {
        return failureLatencies;
    }

    public final Iterable<LatencyRecord.Sample> getWorkerTaskLatencyRecords() {
        return workerTaskLatencyRecord;
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
        long beginNs, endExecuteTimeNs, endFetchedWorkNs, endThinkTimeNs, endKeyingTimeNs;
        Thread t = Thread.currentThread();
        SubmittedProcedure pieceOfWork;
        t.setName(this.toString());

        // In case of reuse reset the measurements
        latencies = new TransactionLatencyRecord(wrkldState.getTestStartNs());
        failureLatencies = new TransactionLatencyRecord(wrkldState.getTestStartNs());
        workerTaskLatencyRecord = new WorkerTaskLatencyRecord(wrkldState.getTestStartNs());
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

            beginNs = System.nanoTime();
            // Grab some work and update the state, in case it changed while we
            // waited.
            pieceOfWork = wrkldState.fetchWork(this.id);

            preState = wrkldState.getGlobalState();
            endFetchedWorkNs = System.nanoTime();
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
            endKeyingTimeNs = System.nanoTime();

            ArrayList<Pair<TransactionExecutionState, TransactionStatus>> executionStates = null;
            try {
                executionStates = doWork(pieceOfWork);
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
            endExecuteTimeNs = System.nanoTime();

            if (think_time_msecs > 0) {
                // Sleep for the think time duration.
                try {
                    Thread.sleep(think_time_msecs);
                } catch (InterruptedException e) {
                    LOG.error("Thread sleep interrupted");
                }
            }
            endThinkTimeNs = System.nanoTime();

            // PART 4: Record results
            postState = wrkldState.getGlobalState();

            switch (postState) {
                case MEASURE:
                    // Non-serial measurement. Only measure if the state after doWork
                    // completion was MEASURE. We're recording results for operations
                    // that completed in the MEASURE phase
                    if ((preState == State.MEASURE || preState == State.WARMUP) &&
                        Worker.wrkldState.getCurrentPhase().id == phase.id) {
                        int attempt = 0;
                        for (Pair<TransactionExecutionState, TransactionStatus>  executionState : executionStates) {
                            if (executionState.first.getTransactionType() != null) {
                                switch (executionState.second) {
                                    case SUCCESS:
                                    case USER_ABORTED:
                                        latencies.addLatency(
                                                executionState.first.getTransactionType().getId(),
                                                executionState.first.getStartConnection(),
                                                executionState.first.getEndConnection(),
                                                executionState.first.getStartOperation(),
                                                executionState.first.getEndOperation()
                                        );
                                        break;
                                    case RETRY:
                                    case UNKNOWN:
                                        ++totalFailedTries[pieceOfWork.getType() - 1][attempt];
                                        failureLatencies.addLatency(executionState.first.getTransactionType().getId(),
                                                executionState.first.getStartConnection(),
                                                executionState.first.getEndConnection(),
                                                executionState.first.getStartOperation(),
                                                executionState.first.getEndOperation());
                                        break;
                                }
                            }
                            attempt++;
                        }
                        workerTaskLatencyRecord.addLatency(
                                pieceOfWork.getType(),
                                beginNs, endFetchedWorkNs, endKeyingTimeNs, endExecuteTimeNs, endThinkTimeNs);
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
    }

    /**
     * Called in a loop in the thread to exercise the system under test. Each
     * implementing worker should return the TransactionType handle that was
     * executed.
     */
    protected final ArrayList<Pair<TransactionExecutionState, TransactionStatus>> doWork(SubmittedProcedure pieceOfWork) {
        TransactionType next = null;
        long startOperation = 0;
        long endOperation = 0;
        long startConnection;
        long endConnection;
        ArrayList<Pair<TransactionExecutionState, TransactionStatus>> listExecutionStates = new ArrayList<>();

        TransactionStatus status = TransactionStatus.RETRY;

        Connection conn;
        try {
            if (next == null) {
                next = transactionTypes.getType(pieceOfWork.getType());
            }
            startConnection = System.nanoTime();

            conn = dataSource.getConnection();
            try {
                if(wrkld.getDBType().equals("yugabyte"))
                    conn.createStatement().execute("SET yb_enable_expression_pushdown to on");
                if (next.getProcedureClass() != StockLevel.class) {
                    // In accordance with 2.8.2.3 of the TPCC spec, StockLevel should execute each query in its own Snapshot
                    // Isolation.
                    conn.setAutoCommit(false);
                }
            } catch (Throwable e) {

            }

            endConnection = System.nanoTime();
            int attempt = 0;

            while (status == TransactionStatus.RETRY &&
                   wrkldState.getGlobalState() != State.DONE &&
                   ++attempt <= totalAttemptsPerTransaction) {
                try {
                    status = TransactionStatus.UNKNOWN;
                    startOperation = System.nanoTime();
                    status = this.executeWork(conn, next);
                // User Abort Handling
                // These are not errors
                } catch (UserAbortException ex) {
                    // UserAbortException should represent an expected NewOrder failure and will be recorded as a
                    // success. No other procedure class should hit this branch.
                    assert(next.getProcedureClass() == NewOrder.class);
                    if (!conn.getAutoCommit()) {
                        conn.rollback();
                    }
                    status = TransactionStatus.USER_ABORTED;
                    // Operation is considered ended once we've successfully rolled back the expected failure in
                    // NewOrder
                    endOperation = System.nanoTime();
                    break;
                // Database System Specific Exception Handling
                } catch (SQLException ex) {
                    LOG.debug(String.format("%s thrown when executing '%s' on '%s' " +
                                           "[Message='%s', ErrorCode='%d', SQLState='%s']",
                                           ex.getClass().getSimpleName(), next, this.toString(),
                                           ex.getMessage(), ex.getErrorCode(), ex.getSQLState()), ex);
                    try {
                        if (!conn.getAutoCommit()) {
                            conn.rollback();
                        }
                    } catch (Throwable t) {
                        // ignore if we are not able to rollback the transaction
                    }

                    if (ex.getSQLState() != null) {
                        if (ex.getErrorCode() == 0 && ex.getSQLState() != null && ex.getSQLState().equals("40001")) {
                            // Postgres serialization
                            status = TransactionStatus.RETRY;
                        } else if (
                                ex.getErrorCode() == 0 &&
                                ex.getSQLState() != null &&
                                Arrays.asList("53200", "XX000").contains(ex.getSQLState())) {
                            // 53200 - Postgres OOM error
                            // XX000 - Postgres no pinned buffers available
                            status = TransactionStatus.RETRY;
                        } else {
                            // UNKNOWN: In this case .. Retry as well!
                            if(attempt == totalAttemptsPerTransaction) {
                                LOG.warn("The DBMS rejected the transaction without an error code:" +  ex.getStackTrace());
                            } else {
                                LOG.warn("The DBMS rejected the transaction without an error code:" +  ex.getMessage());
                            }
                            // FIXME Disable this for now
                            // throw ex;
                            status = TransactionStatus.RETRY;
                        }
                    }
                // Assertion Error
                } catch (Error ex) {
                    if (attempt == totalAttemptsPerTransaction) {
                        LOG.error("Fatal error when invoking :" + ex.getStackTrace());
                    } else {
                        LOG.error("Fatal error when invoking :" + ex.getMessage());
                    }
                    status = TransactionStatus.RETRY;
                 // Random Error
                } catch (Exception ex) {
                    if (attempt == totalAttemptsPerTransaction) {
                        LOG.error("Fatal error when invoking :" + ex.getStackTrace());
                    } else {
                        LOG.error("Fatal error when invoking :" + ex.getMessage());
                    }
                    status = TransactionStatus.RETRY;

                } finally {
                    LOG.debug(String.format("%s %s Result: %s", this, next, status));
                    endOperation = System.nanoTime();
                    // add the current attempts stats to a list
                    listExecutionStates.add(Pair.of(new TransactionExecutionState(startOperation, endOperation,
                            startConnection, endConnection,
                            next), status));
                    switch (status) {
                        case SUCCESS:
                        case USER_ABORTED:
                            break;
                        case RETRY:
                        case UNKNOWN:
                            assert (false) : String.format("Unexpected status '%s' for %s", status, next);
                    } // SWITCH
                }
                // Don't retry if it is not a NewOrder Transaction.
                if (next.getProcedureClass() != NewOrder.class) {
                    break;
                }
            } // WHILE
            conn.close();
        } catch (SQLException ex) {
            String msg = String.format("Unexpected fatal, error in '%s' when executing '%s'",
                                       this, next);
            throw new RuntimeException(msg, ex);
        }
        return listExecutionStates;
    }

    /**
     * Executes a single TPCC transaction of type transactionType.
     */
    protected TransactionStatus executeWork(Connection conn, TransactionType nextTransaction) throws SQLException {
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
      if (!conn.getAutoCommit()) {
        conn.commit();
      }
      return (TransactionStatus.SUCCESS);
    }

    public void initializeState() {
        assert (Worker.wrkldState == null);
        Worker.wrkldState = Worker.wrkld.getWorkloadState();
    }
}
