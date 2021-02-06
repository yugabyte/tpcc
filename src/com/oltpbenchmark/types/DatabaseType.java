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

package com.oltpbenchmark.types;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

/**
 * List of the database management systems that we support 
 * in the framework.
 * @author pavlo
 */
public enum DatabaseType {

    /**
     * Parameters:
     * (1) JDBC Driver String
     * (2) Should SQLUtil.getInsertSQL escape table/col names
     * (3) Should SQLUtil.getInsertSQL include col names
     * (4) Does this DBMS support "real" transactions?
     */
    DB2(true),
    MYSQL(true),
    MYROCKS(true),
    POSTGRES(true),
    ORACLE(true),
    SQLSERVER(true),
    SQLITE(true),
    AMAZONRDS(true),
    SQLAZURE(true),
    ASSCLOWN(true),
    HSQLDB(true),
    H2(true),
    MONETDB(true),
    NUODB(true),
    TIMESTEN(true),
    CASSANDRA(false),
    MEMSQL(false),
    NOISEPAGE(true),
    ;
    
    DatabaseType(boolean supportTxns) {
        this.supportTxns = supportTxns;
    }

    /**
     * If this flag is set to true, then the framework will invoke the JDBC transaction
     * api to do various things during execution. This should only be disabled
     * if you know that the DBMS will throw an error when these commands are executed.
     * For example, the Cassandra JDBC driver (as of 2018) throws a "Not Implemented" exception
     * when the framework tries to set the isolation level.
     */
    private final boolean supportTxns;
    
    // ---------------------------------------------------------------
    // ACCESSORS
    // ----------------------------------------------------------------

    /**
     * Returns true if the framework should use transactions when executing
     * any SQL queries on the target DBMS.
     */
    public boolean shouldUseTransactions() {
        return (this.supportTxns);
    }
    
    // ----------------------------------------------------------------
    // STATIC METHODS + MEMBERS
    // ----------------------------------------------------------------
    
    protected static final Map<Integer, DatabaseType> idx_lookup = new HashMap<>();
    protected static final Map<String, DatabaseType> name_lookup = new HashMap<>();
    static {
        for (DatabaseType vt : EnumSet.allOf(DatabaseType.class)) {
            DatabaseType.idx_lookup.put(vt.ordinal(), vt);
            DatabaseType.name_lookup.put(vt.name().toUpperCase(), vt);
        }
    }
    
    public static DatabaseType get(String name) {
        return DatabaseType.name_lookup.get(name.toUpperCase());
    }
}
