package com.oltpbenchmark.schema;

import java.util.ArrayList;
import java.util.List;

import com.sun.istack.NotNull;

public class TableSchema {
    private final String name;
    private final List<Column> columns;
    private final String primaryKey;
    private final String partitionKey;
    
    public TableSchema(String name, List<Column> columns, String primaryKey, String partitionKey) {
        this.name = name;
        this.columns = columns;
        this.primaryKey = primaryKey;
        this.partitionKey = partitionKey;
    }
    
    public String name() {
        return name;
    }

    public List<Column> getColumns() {
        return columns;
    }

    public String getPrimaryKey() {
        return primaryKey;
    }

    public String getPartitionKey() {
        return partitionKey;
    }
}

class TableSchemaBuilder {
    private final String name;
    private List<Column> columns = new ArrayList<Column>();
    private String primaryKey;
    private String partitionKey;
    
    TableSchemaBuilder(@NotNull String name) {
        if (name.isEmpty()) {
            throw new RuntimeException("Found malformed table schema: Given table name is empty");
        }
        this.name = name;
    }
    
    TableSchemaBuilder column(String name, String decl) {
        this.columns.add(new Column(name, decl));
        return this;
    }
    
    TableSchemaBuilder primaryKey(String primaryKeyDecl) {
        this.primaryKey = primaryKeyDecl;
        return this;
    }
    
    TableSchemaBuilder partitionKey(String partitionKey) {
        this.partitionKey = partitionKey;
        return this;
    }

    public TableSchema build() {
        if (columns.isEmpty()) {
            throw new RuntimeException("Found malformed table schema: No columns given for table " + name);
        }
        return new TableSchema(name, columns, primaryKey, partitionKey);
    }
}