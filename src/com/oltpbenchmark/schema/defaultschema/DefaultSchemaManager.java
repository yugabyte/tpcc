package com.oltpbenchmark.schema.defaultschema;

import com.oltpbenchmark.benchmarks.tpcc.procedures.StockLevel;
import com.oltpbenchmark.schema.SchemaManager;
import com.oltpbenchmark.schema.TPCCTableSchemas;
import com.oltpbenchmark.schema.Table;
import com.oltpbenchmark.schema.TableSchema;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

public class DefaultSchemaManager extends SchemaManager {
    private final Map<String, Table> tables = new HashMap<String, Table>();

    public DefaultSchemaManager(Connection db_connection) {
        super(db_connection);
        for (TableSchema t : TPCCTableSchemas.tables.values()) {
            tables.put(t.name(), new DefaultTable(t));
        }
    }

    @Override
    public void create() throws SQLException {
        for (Table t : tables.values()) {
            execute(t.getDropDdl());
            execute(t.getCreateDdl());
        }
        // TODO -- can we defer this until after load as well?
        execute("CREATE INDEX idx_customer_name ON customer ((c_w_id,c_d_id) HASH,c_last,c_first)");
        execute("CREATE UNIQUE INDEX idx_order ON oorder ((o_w_id,o_d_id) HASH,o_c_id,o_id DESC)");

        if (!db_connection.getAutoCommit()) {
            db_connection.commit();
        }
    }

    public void createIndexes() throws SQLException {
        execute("CREATE INDEX idx_customer_name ON customer ((c_w_id,c_d_id) HASH,c_last,c_first)");
        execute("CREATE UNIQUE INDEX idx_order ON oorder ((o_w_id,o_d_id) HASH,o_c_id,o_id DESC)");
    }
    
    @Override
    public void enableForeignKeyConstraints() throws SQLException {
        execute("ALTER TABLE DISTRICT DROP CONSTRAINT IF EXISTS D_FKEY_W");
        execute("ALTER TABLE DISTRICT ADD CONSTRAINT D_FKEY_W FOREIGN KEY (D_W_ID) " +
                "REFERENCES WAREHOUSE(W_ID) NOT VALID");

        execute("ALTER TABLE CUSTOMER DROP CONSTRAINT IF EXISTS C_FKEY_D");
        execute("ALTER TABLE CUSTOMER ADD CONSTRAINT C_FKEY_D FOREIGN KEY (C_W_ID, C_D_ID) " +
                "REFERENCES DISTRICT (D_W_ID, D_ID) NOT VALID");

        execute("ALTER TABLE STOCK DROP CONSTRAINT IF EXISTS S_FKEY_W");
        execute("ALTER TABLE STOCK DROP CONSTRAINT IF EXISTS S_FKEY_I");
        execute("ALTER TABLE STOCK ADD CONSTRAINT S_FKEY_W FOREIGN KEY(S_W_ID) " +
                "REFERENCES WAREHOUSE(W_ID) NOT VALID");
        execute("ALTER TABLE STOCK ADD CONSTRAINT S_FKEY_I FOREIGN KEY(S_I_ID)  " +
                "REFERENCES ITEM(I_ID) NOT VALID");

        execute("ALTER TABLE OORDER DROP CONSTRAINT IF EXISTS O_FKEY_C");
        execute("ALTER TABLE OORDER ADD CONSTRAINT O_FKEY_C FOREIGN KEY (O_W_ID, O_D_ID, O_C_ID) " +
                "REFERENCES CUSTOMER (C_W_ID, C_D_ID, C_ID) NOT VALID");

        execute("ALTER TABLE NEW_ORDER DROP CONSTRAINT IF EXISTS NO_FKEY_O");
        execute("ALTER TABLE NEW_ORDER ADD CONSTRAINT NO_FKEY_O FOREIGN KEY " +
                "(NO_W_ID, NO_D_ID, NO_O_ID) REFERENCES OORDER (O_W_ID, O_D_ID, O_ID) NOT VALID");

        execute("ALTER TABLE HISTORY DROP CONSTRAINT IF EXISTS H_FKEY_C");
        execute("ALTER TABLE HISTORY DROP CONSTRAINT IF EXISTS H_FKEY_D");
        execute("ALTER TABLE HISTORY ADD CONSTRAINT H_FKEY_C FOREIGN KEY " +
                "(H_C_W_ID, H_C_D_ID, H_C_ID) REFERENCES CUSTOMER (C_W_ID, C_D_ID, C_ID) NOT VALID");
        execute("ALTER TABLE HISTORY ADD CONSTRAINT H_FKEY_D FOREIGN KEY " +
                "(H_W_ID, H_D_ID) REFERENCES DISTRICT (D_W_ID, D_ID) NOT VALID");

        execute("ALTER TABLE ORDER_LINE DROP CONSTRAINT IF EXISTS OL_FKEY_O");
        execute("ALTER TABLE ORDER_LINE DROP CONSTRAINT IF EXISTS OL_FKEY_S");
        execute("ALTER TABLE ORDER_LINE ADD CONSTRAINT OL_FKEY_O FOREIGN KEY " +
                "(OL_W_ID, OL_D_ID, OL_O_ID) REFERENCES OORDER (O_W_ID, O_D_ID, O_ID) NOT VALID");
        execute("ALTER TABLE ORDER_LINE ADD CONSTRAINT OL_FKEY_S FOREIGN KEY " +
                "(OL_SUPPLY_W_ID, OL_I_ID) REFERENCES STOCK (S_W_ID, S_I_ID) NOT VALID");
    }
    
    @Override
    public void createSqlProcedures() throws Exception {
        try (Statement st = db_connection.createStatement()) {
            StringBuilder argsSb = new StringBuilder();
            StringBuilder updateStatements = new StringBuilder();

            // Below, we create 15 procedures, each taking 'i' number of
            // arguments and 'i' update statements. Thus, the number of
            // arguments being built in 'argsSb' and number of updates
            // in 'updateStmt' increases in every iteration.
            argsSb.append("wid int");
            for (int i = 1; i <= 15; ++i) {
                argsSb.append(String.format(", i%d int, q%d int, y%d int, r%d int", i, i, i, i));
                updateStatements.append(String.format(
                        "UPDATE STOCK SET S_QUANTITY = q%d, S_YTD = y%d, S_ORDER_CNT = S_ORDER_CNT + 1, " +
                                "S_REMOTE_CNT = r%d WHERE S_W_ID = wid AND S_I_ID = i%d;",
                                i, i, i, i));
                String updateStmt =
                        String.format("CREATE PROCEDURE updatestock%d (%s) AS '%s' LANGUAGE SQL;",
                                i, argsSb.toString(), updateStatements.toString());

                st.execute(String.format("DROP PROCEDURE IF EXISTS updatestock%d", i));
                st.execute(updateStmt);
            }

            StockLevel.InitializeGetStockCountProc(db_connection);
        }
    }
}
