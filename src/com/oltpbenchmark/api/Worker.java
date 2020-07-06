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
import java.sql.Savepoint;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;


import com.zaxxer.hikari.HikariDataSource;

import org.apache.log4j.Logger;

import com.oltpbenchmark.LatencyRecord;
import com.oltpbenchmark.Phase;
import com.oltpbenchmark.SubmittedProcedure;
import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.WorkloadState;
import com.oltpbenchmark.api.Procedure.UserAbortException;
import com.oltpbenchmark.catalog.Catalog;
import com.oltpbenchmark.types.DatabaseType;
import com.oltpbenchmark.types.State;
import com.oltpbenchmark.types.TransactionStatus;

public abstract class Worker<T extends BenchmarkModule> implements Runnable {
    private static final Logger LOG = Logger.getLogger(Worker.class);

    private static WorkloadState wrkldState;
    private LatencyRecord latencies;
    private Statement currStatement;

    // Interval requests used by the monitor
    private AtomicInteger intervalRequests = new AtomicInteger(0);

    private final int id;
    private final T benchmarkModule;
    protected final HikariDataSource dataSource;
    protected static WorkloadConfiguration wrkld;
    protected TransactionTypes transactionTypes;
    protected Map<Class<? extends Procedure>, Procedure> class_procedures = new HashMap<Class<? extends Procedure>, Procedure>();

    private boolean seenDone = false;

    public Worker(T benchmarkModule, int id) {
        this.id = id;
        this.benchmarkModule = benchmarkModule;
        this.wrkld = this.benchmarkModule.getWorkloadConfiguration();
        this.wrkldState = this.wrkld.getWorkloadState();
        this.currStatement = null;
        InitializeProcedures();

        assert (this.transactionTypes != null) : "The TransactionTypes from the WorkloadConfiguration is null!";
        try {
            this.dataSource = this.benchmarkModule.getDataSource();
        } catch (Exception ex) {
            throw new RuntimeException("Failed to connect to database", ex);
        }
    }

    public final void InitializeProcedures() {
        // Generate all the Procedures that we're going to need
        this.transactionTypes = this.wrkld.getTransTypes();
        for (Entry<TransactionType, Procedure> e : this.benchmarkModule.getProcedures().entrySet()) {
            Procedure proc = e.getValue();
            this.class_procedures.put(proc.getClass(), proc);
        }
    }

    /**
     * Get the BenchmarkModule managing this Worker
     */
    public final T getBenchmarkModule() {
        return (this.benchmarkModule);
    }

    /**
     * Get the unique thread id for this worker
     */
    public final int getId() {
        return this.id;
    }

    @Override
    public String toString() {
        return String.format("%s<%03d>",
                             this.getClass().getSimpleName(), this.getId());
    }

    /**
     * Get the the total number of workers in this benchmark invocation
     */
    public final int getNumWorkers() {
        return (this.benchmarkModule.getWorkloadConfiguration().getTerminals());
    }

    public final WorkloadConfiguration getWorkloadConfiguration() {
        return (this.benchmarkModule.getWorkloadConfiguration());
    }

    public final Catalog getCatalog() {
        return (this.benchmarkModule.getCatalog());
    }

    public final Random rng() {
        return (this.benchmarkModule.rng());
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

    @SuppressWarnings("unchecked")
    public final <P extends Procedure> P getProcedure(Class<P> procClass) {
        return (P) (this.class_procedures.get(procClass));
    }

    synchronized public void setCurrStatement(Statement s) {
        this.currStatement = s;
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
        long mean = -1;;
        if (type.getName().equals("NewOrder")) {
          mean = 12000;
        } else if (type.getName().equals("Payment")) {
          mean = 12000;
        } else if (type.getName().equals("OrderStatus")) {
          mean = 10000;
        } else if (type.getName().equals("Delivery")) {
          mean = 5000;
        } else if (type.getName().equals("StockLevel")) {
          mean = 5000;
        } else {
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

    @Override
    public final void run() {
        Thread t = Thread.currentThread();
        SubmittedProcedure pieceOfWork;
        t.setName(this.toString());

        // In case of reuse reset the measurements
        latencies = new LatencyRecord(wrkldState.getTestStartNs());

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

        TransactionType invalidTT = TransactionType.INVALID;
        assert (invalidTT != null);

        work: while (true) {

            // PART 1: Init and check if done

            preState = wrkldState.getGlobalState();
            phase = this.wrkldState.getCurrentPhase();

            switch (preState) {
                case DONE:
                    if (!seenDone) {
                        // This is the first time we have observed that the
                        // test is done notify the global test state, then
                        // continue applying load
                        seenDone = true;
                        wrkldState.signalDone();
                        break work;
                    }
                    break;
                default:
                    // Do nothing
            }

            // PART 2: Wait for work

            // Sleep if there's nothing to do.
            wrkldState.stayAwake();
            phase = this.wrkldState.getCurrentPhase();
            if (phase == null)
                continue work;


            // Grab some work and update the state, in case it changed while we
            // waited.

            pieceOfWork = wrkldState.fetchWork(this.id);
            preState = wrkldState.getGlobalState();

            phase = this.wrkldState.getCurrentPhase();
            if (phase == null)
                continue work;

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

            long start = System.nanoTime();

            // TODO: Measuring latency when not rate limited is ... a little
            // weird because if you add more simultaneous clients, you will
            // increase latency (queue delay) but we do this anyway since it is
            // useful sometimes

            if (this.wrkld.getUseKeyingTime()) {
                // Wait for the keying time which is a fixed amount for each type of transaction.
                long keying_time_msecs = getKeyingTimeInMillis(transactionTypes.getType(pieceOfWork.getType()));
                try {
                    long sleep_start = System.nanoTime();
                    Thread.sleep(keying_time_msecs);
                    if (LOG.isDebugEnabled()) {
                        LOG.info(transactionTypes.getType(pieceOfWork.getType()).getName() +
                            " Keying time " + (System.nanoTime() - sleep_start) / 1000 / 1000 / 1000);
                    }
                } catch (InterruptedException e) {
                    LOG.error("Thread sleep interrupted");
                }
            }

            TransactionType type = invalidTT;
            try {
                type = doWork(preState == State.MEASURE, pieceOfWork);
            } catch (IndexOutOfBoundsException e) {
                if (phase.isThroughputRun()) {
                    LOG.error("Thread tried executing disabled phase!");
                    throw e;
                }
                if (phase.id == this.wrkldState.getCurrentPhase().id) {
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

            if (this.wrkld.getUseThinkTime()) {
                // Sleep for the think time duration.
                long think_time_msecs = getThinkTimeInMillis(transactionTypes.getType(pieceOfWork.getType()));
                try {
                    long sleep_start = System.nanoTime();
                    Thread.sleep(think_time_msecs);
                    if (LOG.isDebugEnabled()) {
                        LOG.info(transactionTypes.getType(pieceOfWork.getType()).getName() +
                            " Think time " + (System.nanoTime() - sleep_start) / 1000 / 1000 / 1000);
                    }
                } catch (InterruptedException e) {
                    LOG.error("Thread sleep interrupted");
                }
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
                    if (preState == State.MEASURE && type != null && this.wrkldState.getCurrentPhase().id == phase.id) {
                        latencies.addLatency(type.getId(), start, end, this.id, phase.id);
                        intervalRequests.incrementAndGet();
                    }
                    if (phase.isLatencyRun())
                        this.wrkldState.startColdQuery();
                    break;
                case COLD_QUERY:
                    // No recording for cold runs, but next time we will since
                    // it'll be a hot run.
                    if (preState == State.COLD_QUERY)
                        this.wrkldState.startHotQuery();
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
     *
     * @param llr
     */
    protected final TransactionType doWork(boolean measure, SubmittedProcedure pieceOfWork) {
        TransactionType next = null;
        TransactionStatus status = TransactionStatus.RETRY;
        Savepoint savepoint = null;
        final DatabaseType dbType = wrkld.getDBType();
        final boolean recordAbortMessages = wrkld.getRecordAbortMessages();


        Connection conn = null;
        try {
            conn = dataSource.getConnection();
            conn.setAutoCommit(false);
            conn.setTransactionIsolation(this.wrkld.getIsolationMode());

            while (status == TransactionStatus.RETRY && this.wrkldState.getGlobalState() != State.DONE) {
                if (next == null) {
                    next = transactionTypes.getType(pieceOfWork.getType());
                }
                assert (next.isSupplemental() == false) : "Trying to select a supplemental transaction " + next;

                try {
                    // For Postgres, we have to create a savepoint in order
                    // to rollback a user aborted transaction
                    // if (dbType == DatabaseType.POSTGRES) {
                    // savepoint = this.conn.setSavepoint();
                    // // if (LOG.isDebugEnabled())
                    // LOG.info("Created SavePoint: " + savepoint);
                    // }

                    status = TransactionStatus.UNKNOWN;
                    status = this.executeWork(conn, next);

                // User Abort Handling
                // These are not errors
                } catch (UserAbortException ex) {
                    if (LOG.isDebugEnabled())
                        LOG.trace(next + " Aborted", ex);
                    if (savepoint != null) {
                        conn.rollback(savepoint);
                    } else {
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

		    if (this.wrkld.getDBType().shouldUseTransactions()) {
			if (savepoint != null) {
			    conn.rollback(savepoint);
			} else {
			    conn.rollback();
			}
		    }

                    if (ex.getSQLState() == null) {
                        continue;
                    // ------------------
                    // POSTGRES
                    // ------------------
                    } else if (ex.getErrorCode() == 0 && ex.getSQLState() != null && ex.getSQLState().equals("40001")) {
                        // Postgres serialization
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

        return (next);
    }

    /**
     * Optional callback that can be used to initialize the Worker right before
     * the benchmark execution begins
     */
    protected void initialize() {
        // The default is to do nothing
    }

    /**
     * Invoke a single transaction for the given TransactionType
     *
     * @param txnType
     * @return TODO
     * @throws UserAbortException
     *             TODO
     * @throws SQLException
     *             TODO
     */
    protected abstract TransactionStatus executeWork(Connection conn, TransactionType txnType) throws UserAbortException, SQLException;

    /**
     * Called at the end of the test to do any clean up that may be required.
     *
     * @param error
     *            TODO
     */
    public void tearDown(boolean error) { }

    public void initializeState() {
        assert (this.wrkldState == null);
        this.wrkldState = this.wrkld.getWorkloadState();
    }
}
