package com.oltpbenchmark.schema.defaultschema;

import com.oltpbenchmark.schema.Table;
import com.oltpbenchmark.schema.TableSchema;

public class DefaultTable extends Table {
    
    private final String tablespace;
    
    public DefaultTable(TableSchema schema) {
        this(schema, null);
    }
    
    public DefaultTable(TableSchema schema, String tablespace) {
        super(schema);
        this.tablespace = tablespace;
    }

    @Override
    public String getCreateDdl() {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE ").append(schema.name()).append(" ( ");
        
        addColDescForCreateDDL(sb);
        
        if (schema.getPrimaryKey() != null) {
            sb.append(",\n PRIMARY KEY ").append(schema.getPrimaryKey());
        }
        sb.append("\n)");

        if (tablespace != null) {
            sb.append(" TABLESPACE ").append(tablespace);
        }
        return sb.toString();
    }
}
