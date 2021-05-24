package com.oltpbenchmark.schema.geopartitioned;

import com.oltpbenchmark.schema.Column;
import com.oltpbenchmark.schema.Table;
import com.oltpbenchmark.schema.TableSchema;
import com.oltpbenchmark.util.GeoPartitionPolicy;

public class PartitionedTable extends Table {
    private final GeoPartitionPolicy policy;

    PartitionedTable (TableSchema schema, GeoPartitionPolicy policy) {
        super(schema);
        this.policy = policy;
    }

    private int numPartitions() {
        return policy.getNumPartitions();
    }

    private String tablespaceForPartitionedTables() {
        return policy.getTablespaceForPartitionedTables();
    }

    private String tablespaceForPartition(int idx) {
        return policy.getTablespaceForPartition(idx);
    }

    @Override
    public String getCreateDdl() {
        StringBuilder sb = new StringBuilder();
        sb.append(createPartitionedTable());
        for (int i = 1; i <= numPartitions(); ++i) {
            sb.append(createPartitionTable(i));
        }
        return sb.toString();
    }

    private String createPartitionedTable() {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE ").append(schema.name()).append(" ( ");
 
        addColDescForCreateDDL(sb);
       
        // Append the partition key.
        sb.append(")\n PARTITION BY RANGE ").append(schema.getPartitionKey());
        // Append the tablespace name.
        sb.append("\n TABLESPACE ").append(tablespaceForPartitionedTables());

        // Append the SPLIT INTO syntax. Partitioned tables do not have any data backing them
        // and need to be stored in only one tablet.
        sb.append("\n SPLIT INTO 1 TABLETS;");

        return sb.toString();
    }

    private String createPartitionTable(int idx) {
        final int start = getStartWarehouseForPartition(idx);
        final int end = getEndWarehouseForPartition(idx);
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("CREATE TABLE %s%d PARTITION OF %s(", schema.name(), idx, schema.name()));

        for (Column c : schema.getColumns()) {
            // While creating partitions, skip the column declaration.
            sb.append("\n").append(c.getName()).append(',');
        }
        
        // Remove trailing comma added after the last column in above loop.
        sb.setLength(sb.length() - 1);

        if (schema.getPrimaryKey() != null) {
            sb.append(String.format(",\n PRIMARY KEY %s", schema.getPrimaryKey()));
        }
        sb.append(String.format(")\n FOR VALUES FROM (%d) TO (%d)", start, end));
        sb.append(String.format(" TABLESPACE %s;", tablespaceForPartition(idx - 1)));

        return sb.toString();
    }

    public int getStartWarehouseForPartition(int idx) {
        return (idx - 1) * policy.numWareHousesPerSplit() + 1;
    }

    public int getEndWarehouseForPartition(int idx) {
        return idx * policy.numWareHousesPerSplit() + 1;
    }
}
