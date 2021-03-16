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

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import com.oltpbenchmark.jdbc.InstrumentedPreparedStatement;
import org.apache.log4j.Logger;

public abstract class Procedure {
    private static final Logger LOG = Logger.getLogger(Procedure.class);

    private final String procName;
    private Map<String, SQLStmt> name_stmt_xref;
    private final Map<SQLStmt, String> stmt_name_xref = new HashMap<>();
    
    /**
     * Constructor
     */
    protected Procedure() {
        this.procName = this.getClass().getSimpleName();
    }
    
    /**
     * Initialize all of the SQLStmt handles. This must be called separately from
     * the constructor, otherwise we can't get access to all of our SQLStmts.
     */
    protected final void initialize() {
        this.name_stmt_xref = Procedure.getStatments(this);
        for (Entry<String, SQLStmt> e : this.name_stmt_xref.entrySet()) {
            this.stmt_name_xref.put(e.getValue(), e.getKey());
        } // FOR
        if (LOG.isDebugEnabled())
            LOG.debug(String.format("Initialized %s with %d SQLStmts: %s",
                                    this, this.name_stmt_xref.size(), this.name_stmt_xref.keySet()));
    }

    /**
     * Return a PreparedStatement for the given SQLStmt handle
     * The underlying Procedure API will make sure that the proper SQL
     * for the target DBMS is used for this SQLStmt.
     * This will automatically call setObject for all the parameters you pass in.
     */
    public final InstrumentedPreparedStatement getPreparedStatement(Connection conn,
                                                                    InstrumentedSQLStmt stmt) throws SQLException {
        assert(this.name_stmt_xref != null) : "The Procedure " + this + " has not been initialized yet!";
        assert(this.stmt_name_xref.containsKey(stmt)) :
                "Unexpected SQLStmt handle in " + this.getClass().getSimpleName() + "\n" + this.name_stmt_xref;
        return new InstrumentedPreparedStatement(conn.prepareStatement(stmt.getSqlStmt().getSQL()), stmt);
    }
    
    protected static Map<String, SQLStmt> getStatments(Procedure proc) {
        Class<? extends Procedure> c = proc.getClass();
        Map<String, SQLStmt> stmts = new HashMap<>();
        for (Field f : c.getDeclaredFields()) {
            int modifiers = f.getModifiers();
            if (!Modifier.isTransient(modifiers) &&
                    Modifier.isPublic(modifiers) &&
                    !Modifier.isStatic(modifiers)) {
                try {
                    Object o = f.get(proc);
                    if (o instanceof SQLStmt) {
                        stmts.put(f.getName(), (SQLStmt)o);
                    }
                } catch (Exception ex) {
                    throw new RuntimeException("Failed to retrieve " + f + " from " + c.getSimpleName(), ex);
                }
            }
        } // FOR
        return (stmts);
    }
    
    @Override
    public String toString() {
        return (this.procName);
    }

    public abstract void run(Connection conn, Random gen,
                             int terminalWarehouseID, int numWarehouses,
                             int terminalDistrictLowerID, int terminalDistrictUpperID,
                             Worker w) throws SQLException;

    public void test(Connection conn, Worker w) throws Exception {}

    /**
     * Thrown from a Procedure to indicate to the Worker
     * that the procedure should be aborted and rolled back.
     */
    public static class UserAbortException extends RuntimeException {
        private static final long serialVersionUID = -1L;

        /**
         * Default Constructor
         */
        public UserAbortException(String msg, Throwable ex) {
            super(msg, ex);
        }
        
        /**
         * Constructs a new UserAbortException
         * with the specified detail message.
         */
        public UserAbortException(String msg) {
            this(msg, null);
        }
    } // END CLASS    
}
