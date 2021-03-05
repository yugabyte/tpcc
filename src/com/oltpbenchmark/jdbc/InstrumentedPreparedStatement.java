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

package com.oltpbenchmark.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;

import com.oltpbenchmark.api.InstrumentedSQLStmt;
import org.HdrHistogram.ConcurrentHistogram;
import org.HdrHistogram.Histogram;

import com.oltpbenchmark.api.SQLStmt;

public class InstrumentedPreparedStatement {

    private final PreparedStatement stmt;
    private final Histogram histogram;

    public InstrumentedPreparedStatement(PreparedStatement stmt, InstrumentedSQLStmt sqlStmt) {
        this.stmt = stmt;
        this.histogram = sqlStmt.getHistogram();
    }

    public InstrumentedPreparedStatement(PreparedStatement stmt, Histogram histogram) {
        this.stmt = stmt;
        this.histogram = histogram;
    }

    public ResultSet executeQuery() throws SQLException {
        long start = System.nanoTime();
        try {
            return this.stmt.executeQuery();
        } finally {
            long end = System.nanoTime();
            addLatency(start, end);
        }
    }

    public int executeUpdate() throws SQLException {
        long start = System.nanoTime();
        try {
            return this.stmt.executeUpdate();
        } finally {
            long end = System.nanoTime();
            addLatency(start, end);
        }
    }

    public boolean execute() throws SQLException {
        long start = System.nanoTime();
        try {
            return this.stmt.execute();
        } finally {
            long end = System.nanoTime();
            addLatency(start, end);
        }
    }

    public int[] executeBatch() throws SQLException {
        long start = System.nanoTime();
        try {
            return this.stmt.executeBatch();
        } finally {
            long end = System.nanoTime();
            addLatency(start, end);
        }
    }

    public void setInt(int parameterIndex, int x) throws SQLException {
        this.stmt.setInt(parameterIndex, x);
    }

    public void setDouble(int parameterIndex, double x) throws SQLException {
        this.stmt.setDouble(parameterIndex, x);
    }

    public void setString(int parameterIndex, String x) throws SQLException {
        this.stmt.setString(parameterIndex, x);
    }

    public void setTimestamp(int parameterIndex, java.sql.Timestamp x) throws SQLException {
        this.stmt.setTimestamp(parameterIndex, x);
    }

    public void addBatch() throws SQLException {
        this.stmt.addBatch();
    }

    public void clearBatch() throws SQLException {
        this.stmt.clearBatch();
    }

    private void addLatency(long startNs, long endNs) {
        histogram.recordValue((endNs - startNs) / 1000);
    }
}
