package com.oltpbenchmark.schema;

/*
 * Helper class denoting a column in a table in the TPCC schema.
 */
public class Column {
    private final String name;
    private final String decl;
    
    public Column(String name, String decl) {
        this.name = name;
        this.decl = decl;
    }
    
    public String getName() {
        return name;
    }
    
    public String getDecl() {
        return decl;
    }
}
