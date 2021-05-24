package com.oltpbenchmark.schema;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Set;

import org.apache.log4j.Logger;

public abstract class SchemaManager {
    private static final Logger LOG = Logger.getLogger(SchemaManager.class);

    protected final Connection db_connection;

    public SchemaManager(Connection db_connection) {
        this.db_connection = db_connection;
    }

    public abstract void create() throws SQLException;

    public abstract void enableForeignKeyConstraints() throws SQLException;
    
    protected abstract void createIndexes() throws SQLException;
    
    public static Set<String> getTableNames() {
        return TPCCTableSchemas.tables.keySet();
    }

    protected void execute(String sql) throws SQLException {
        LOG.info(sql);
        db_connection.createStatement().execute(sql);
    }
    
    public abstract void createSqlProcedures() throws Exception;

}