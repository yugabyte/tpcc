package com.oltpbenchmark.schema.geopartitioned;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import com.oltpbenchmark.benchmarks.tpcc.TPCCConstants;
import com.oltpbenchmark.benchmarks.tpcc.procedures.StockLevel;
import com.oltpbenchmark.schema.SchemaManager;
import com.oltpbenchmark.schema.TPCCTableSchemas;
import com.oltpbenchmark.schema.Table;
import com.oltpbenchmark.schema.TableSchema;
import com.oltpbenchmark.schema.defaultschema.DefaultTable;
import com.oltpbenchmark.util.GeoPartitionPolicy;

public class GeoPartitionedSchemaManager extends SchemaManager {
    private final GeoPartitionPolicy geoPartitionPolicy;

    private final Map<String, Table> tables = new HashMap<String, Table>();

    public GeoPartitionedSchemaManager(GeoPartitionPolicy geoPartitioningPolicy, Connection conn) {
        super(conn);
        this.geoPartitionPolicy = geoPartitioningPolicy;
        for (TableSchema t : TPCCTableSchemas.tables.values()) {
            tables.put(t.name(), 
                       t.name().equals(TPCCConstants.TABLENAME_ITEM) ? new DefaultTable(t, geoPartitioningPolicy.getTablespaceForItemTable()) 
                                                                     : new PartitionedTable(t, geoPartitionPolicy));
        }
    }

    private int numPartitions() {
        return geoPartitionPolicy.getNumPartitions();
    }

    @Override
    public void create() throws SQLException {
        for (Table t : tables.values()) {
            execute(t.getDropDdl());
        }

        createTablespaces();

        for (Table t : tables.values()) {
            execute(t.getCreateDdl());
        }
        // TODO -- can we defer this until after load as well?
        createIndexes();

        if (!db_connection.getAutoCommit()) {
            db_connection.commit();
        }
    }

    @Override
    public void createIndexes() throws SQLException {
        for (int i = 1; i <= numPartitions(); ++i) {
            execute(String.format("CREATE INDEX idx_customer_name%d ON customer%d ((c_w_id,c_d_id) HASH,c_last,c_first) TABLESPACE %s",
                    i, i, geoPartitionPolicy.getTablespaceForPartition(i - 1)));
            execute(String.format("CREATE UNIQUE INDEX idx_order%d ON oorder%d ((o_w_id,o_d_id) HASH,o_c_id,o_id DESC) TABLESPACE %s",
                    i, i, geoPartitionPolicy.getTablespaceForPartition(i - 1)));

        }
    }

    private void createTablespaces() throws SQLException {
        for (final String tablespace : geoPartitionPolicy.getTablespaceToPlacementPolicy().keySet()) {
            execute(String.format("DROP TABLESPACE IF EXISTS %s", tablespace));
            // Get the placement policy associated with this tablespace.
            final String placementJson = geoPartitionPolicy.getTablespaceCreationJson(tablespace);
            execute(String.format("CREATE TABLESPACE %s WITH (replica_placement='%s')", tablespace, placementJson));
        }
    }

    @Override
    public void enableForeignKeyConstraints() throws SQLException {
        // Create foreign key relations among the partitions themselves, rather than between
        // the partitioned tables themselves.
        for (int idx = 1; idx <= numPartitions(); ++idx) {
            execute(String.format("ALTER TABLE DISTRICT%d DROP CONSTRAINT IF EXISTS D_FKEY_W%d", idx, idx));
            execute(String.format("ALTER TABLE DISTRICT%d ADD CONSTRAINT D_FKEY_W%d FOREIGN KEY (D_W_ID) " +
                    "REFERENCES WAREHOUSE%d(W_ID) NOT VALID", idx, idx, idx));

            execute(String.format("ALTER TABLE CUSTOMER%d DROP CONSTRAINT IF EXISTS C_FKEY_D%d", idx, idx));
            execute(String.format("ALTER TABLE CUSTOMER%d ADD CONSTRAINT C_FKEY_D%d FOREIGN KEY (C_W_ID, C_D_ID) " +
                    "REFERENCES DISTRICT%d (D_W_ID, D_ID) NOT VALID", idx, idx, idx));

            execute(String.format("ALTER TABLE STOCK%d DROP CONSTRAINT IF EXISTS S_FKEY_W%d", idx, idx));
            execute(String.format("ALTER TABLE STOCK%d DROP CONSTRAINT IF EXISTS S_FKEY_I%d", idx, idx));
            execute(String.format("ALTER TABLE STOCK%d ADD CONSTRAINT S_FKEY_W%d FOREIGN KEY(S_W_ID) " +
                    "REFERENCES WAREHOUSE%d(W_ID) NOT VALID", idx, idx, idx));
            execute(String.format("ALTER TABLE STOCK%d ADD CONSTRAINT S_FKEY_I%d FOREIGN KEY(S_I_ID)  " +
                    "REFERENCES ITEM(I_ID) NOT VALID", idx, idx));

            execute(String.format("ALTER TABLE OORDER%d DROP CONSTRAINT IF EXISTS O_FKEY_C%d", idx, idx));
            execute(String.format("ALTER TABLE OORDER%d ADD CONSTRAINT O_FKEY_C%d FOREIGN KEY (O_W_ID, O_D_ID, O_C_ID) " +
                    "REFERENCES CUSTOMER%d (C_W_ID, C_D_ID, C_ID) NOT VALID", idx, idx, idx));

            execute(String.format("ALTER TABLE NEW_ORDER%d DROP CONSTRAINT IF EXISTS NO_FKEY_O%d", idx, idx));
            execute(String.format("ALTER TABLE NEW_ORDER%d ADD CONSTRAINT NO_FKEY_O%d FOREIGN KEY " +
                    "(NO_W_ID, NO_D_ID, NO_O_ID) REFERENCES OORDER%d (O_W_ID, O_D_ID, O_ID) NOT VALID", idx, idx, idx));

            execute(String.format("ALTER TABLE HISTORY%d DROP CONSTRAINT IF EXISTS H_FKEY_C%d", idx, idx));
            execute(String.format("ALTER TABLE HISTORY%d DROP CONSTRAINT IF EXISTS H_FKEY_D%d", idx, idx));
            execute(String.format("ALTER TABLE HISTORY%d ADD CONSTRAINT H_FKEY_C%d FOREIGN KEY " +
                    "(H_C_W_ID, H_C_D_ID, H_C_ID) REFERENCES CUSTOMER%d (C_W_ID, C_D_ID, C_ID) NOT VALID", idx, idx, idx));
            execute(String.format("ALTER TABLE HISTORY%d ADD CONSTRAINT H_FKEY_D%d FOREIGN KEY " +
                    "(H_W_ID, H_D_ID) REFERENCES DISTRICT%d (D_W_ID, D_ID) NOT VALID", idx, idx, idx));

            execute(String.format("ALTER TABLE ORDER_LINE%d DROP CONSTRAINT IF EXISTS OL_FKEY_O%d", idx, idx));
            execute(String.format("ALTER TABLE ORDER_LINE%d DROP CONSTRAINT IF EXISTS OL_FKEY_S%d", idx, idx));
            execute(String.format("ALTER TABLE ORDER_LINE%d ADD CONSTRAINT OL_FKEY_O%d FOREIGN KEY " +
                    "(OL_W_ID, OL_D_ID, OL_O_ID) REFERENCES OORDER%d (O_W_ID, O_D_ID, O_ID) NOT VALID", idx, idx, idx));
            execute(String.format("ALTER TABLE ORDER_LINE%d ADD CONSTRAINT OL_FKEY_S%d FOREIGN KEY " +
                    "(OL_SUPPLY_W_ID, OL_I_ID) REFERENCES STOCK%d (S_W_ID, S_I_ID) NOT VALID", idx, idx, idx));
        }
    }
    
    @Override
    public void createSqlProcedures() throws Exception {
        try (Statement st = db_connection.createStatement()) {
            final int numPartitions = geoPartitionPolicy.getNumPartitions();

            // For each partition, build 15 procedures similar to createSQLProcedures
            // above.
            for (int partition = 1; partition < numPartitions; ++partition) {
                StringBuilder argsSb = new StringBuilder();
                StringBuilder updateStatements = new StringBuilder();

                argsSb.append("wid int");
                // Create functions that update the partition tables themselves.
                for (int i = 1; i <= 15; ++i) {
                    argsSb.append(String.format(", i%d int, q%d int, y%d int, r%d int", i, i, i, i));
                    updateStatements.append(String.format(
                            "UPDATE STOCK%d SET S_QUANTITY = q%d, S_YTD = y%d, S_ORDER_CNT = S_ORDER_CNT + 1, " +
                                    "S_REMOTE_CNT = r%d WHERE S_W_ID = wid AND S_I_ID = i%d;",
                                    partition, i, i, i, i));
                    String updateStmt =
                            String.format("CREATE PROCEDURE updatestock%d_%d (%s) AS '%s' LANGUAGE SQL;",
                                    i, partition, argsSb.toString(), updateStatements.toString());

                    st.execute(String.format("DROP PROCEDURE IF EXISTS updatestock%d_%d", i, partition));
                    st.execute(updateStmt);
                }
            }

            // Create a function that will look at the given warehouse-id,
            // calculate the partition to which the warehouse-id belongs
            // and call update_stock function for that partition.
            StringBuilder argsSb = new StringBuilder();
            argsSb.append("wid int");

            StringBuilder params = new StringBuilder();
            params.append("wid");
            for (int i = 1; i <= 15; ++i) {
                params.append(String.format(", i%d, q%d, y%d, r%d", i, i, i, i));
                StringBuilder partitionStatements = new StringBuilder();
                for (int partition = 1; partition < numPartitions; ++partition) {
                    final int start = geoPartitionPolicy.getStartWarehouseForPartition(partition);
                    final int end = geoPartitionPolicy.getEndWarehouseForPartition(partition);
                    partitionStatements.append(String.format(
                            " IF wid >= %d AND wid < %d THEN" +
                                    " CALL updatestock%d_%d (%s); " +
                                    " END IF;",
                                    start, end, i, partition, params.toString()));
                }

                argsSb.append(String.format(", i%d int, q%d int, y%d int, r%d int", i, i, i, i));
                st.execute(String.format("DROP PROCEDURE IF EXISTS updatestock%d", i));
                st.execute(String.format(
                        "CREATE PROCEDURE updatestock%d (%s) AS $$" +
                                " BEGIN " +
                                partitionStatements.toString() +
                                "END $$ LANGUAGE plpgsql"
                                ,i, argsSb.toString()
                        ));
            }

            StockLevel.InitializeGetStockCountProc(db_connection);
        }
    }
}
