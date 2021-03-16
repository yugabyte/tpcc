package com.oltpbenchmark.jdbc;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.oltpbenchmark.api.InstrumentedSQLStmt;
import org.HdrHistogram.Histogram;

public class InstrumentedPreparedStatement {
    private final PreparedStatement stmt;
    private final Histogram histogram;

    private static boolean trackLatencies = true;

    public InstrumentedPreparedStatement(PreparedStatement stmt, Histogram histogram) {
        this.stmt = stmt;
        this.histogram = histogram;
    }

    public InstrumentedPreparedStatement(PreparedStatement stmt, InstrumentedSQLStmt sqlStmt) {
        this(stmt, sqlStmt.getHistogram());
    }

    public static void trackLatencyMetrics(boolean track) {
        trackLatencies = track;
    }

    public static boolean isTrackingLatencyMetrics() {
        return trackLatencies;
    }

    public ResultSet executeQuery() throws SQLException {
        if (!trackLatencies) {
            return this.stmt.executeQuery();
        }

        long start = System.nanoTime();
        try {
            return this.stmt.executeQuery();
        } finally {
            long end = System.nanoTime();
            addLatency(start, end);
        }
    }

    public int executeUpdate() throws SQLException {
        if (!trackLatencies) {
            return this.stmt.executeUpdate();
        }

        long start = System.nanoTime();
        try {
            return this.stmt.executeUpdate();
        } finally {
            long end = System.nanoTime();
            addLatency(start, end);
        }
    }

    public void execute() throws SQLException {
        if (!trackLatencies) {
            this.stmt.execute();
            return;
        }

        long start = System.nanoTime();
        try {
            this.stmt.execute();
        } finally {
            long end = System.nanoTime();
            addLatency(start, end);
        }
    }

    public void executeBatch() throws SQLException {
        if (!trackLatencies) {
            this.stmt.executeBatch();
            return;
        }

        long start = System.nanoTime();
        try {
            this.stmt.executeBatch();
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
