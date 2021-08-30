package com.oltpbenchmark.schema;

public abstract class Table {
    protected final TableSchema schema;
    
    public Table (TableSchema schema) {
        this.schema = schema;
    }

    // Return name of the table.
    public String name() { 
        return schema.name();
    }

    public String getDropDdl() {
        return String.format("DROP TABLE IF EXISTS %s CASCADE", schema.name());
    }
    
    public static String getInsertDml(String tablename) {
        TableSchema schema = TPCCTableSchemas.getTableSchema(tablename);
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("INSERT INTO %s VALUES (", schema.name()));
        sb.append("?");
        for (int i = 1; i < schema.getColumns().size(); ++i) {
            sb.append(", ?");
        }
        sb.append(")");
        return sb.toString();
    }
    
    // Return the DDL statements to create this table.
    public abstract String getCreateDdl();
    
    protected StringBuilder addColDescForCreateDDL(StringBuilder sb) {
        for (Column c : schema.getColumns()) {
            sb.append("\n").append(c.getName()).append(' ').append(c.getDecl()).append(',');
        }
        // Remove trailing comma added after the last column in above loop.
        sb.setLength(sb.length() - 1);
        return sb;
    }
}
