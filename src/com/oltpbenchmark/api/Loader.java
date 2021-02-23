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
import java.sql.SQLException;
import java.util.List;

import org.apache.log4j.Logger;

import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.catalog.Table;
import com.oltpbenchmark.util.Histogram;

/**
 * @author pavlo
 */
public abstract class Loader<T extends BenchmarkModule> {
    private static final Logger LOG = Logger.getLogger(Loader.class);

    protected final T benchmark;
    protected final WorkloadConfiguration workConf;
    protected final int numWarehouses;
    private final Histogram<String> tableSizes = new Histogram<>();

    /**
     * A LoaderThread is responsible for loading some portion of a
     * benchmark's databsae.
     * Note that each LoaderThread has its own databsae Connection handle.
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

    public Loader(T benchmark) {
        this.benchmark = benchmark;
        this.workConf = benchmark.getWorkloadConfiguration();
        this.numWarehouses = workConf.getNumWarehouses();
    }

    /**
     * Each Loader will generate a list of Runnable objects that
     * will perform the loading operation for the benchmark.
     * The number of threads that will be launched at the same time
     * depends on the number of cores that are available. But they are
     * guaranteed to execute in the order specified in the list.
     * You will have to use your own protections if there are dependencies between
     * threads (i.e., if one table needs to be loaded before another).
     *
     * Each LoaderThread will be given a Connection handle to the DBMS when
     * it is invoked.
     *
     * If the benchmark does <b>not</b> support multi-threaded loading yet,
     * then this method should return null.
     *
     * @return The list of LoaderThreads the framework will launch.
     */
    public abstract List<LoaderThread> createLoaderThreads();

    public Histogram<String> getTableCounts() {
        return (this.tableSizes);
    }

    /**
     * Get the catalog object for the given table name
     */
    @Deprecated
    public Table getTableCatalog(String tableName) {
        Table catalog_tbl = this.benchmark.getCatalog().getTable(tableName.toUpperCase());
        assert (catalog_tbl != null) : "Invalid table name '" + tableName + "'";
        return (catalog_tbl);
    }

    /**
     * Method that can be overriden to specifically unload the tables of the
     * database.
     */
    public abstract void unload(Connection conn) throws SQLException;
}
