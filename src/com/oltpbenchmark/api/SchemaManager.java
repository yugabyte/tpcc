package com.oltpbenchmark.api;

import com.oltpbenchmark.benchmarks.tpcc.TPCCConstants;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class Column {
    final String name;
    final String decl;
    public Column(String name, String decl) {
        this.name = name;
        this.decl = decl;
    }
}

class Table {
    private String name;
    private final List<Column> columns = new ArrayList<>();
    private String primaryKey = null;

    String name() { return name; }

    static class Builder {
        private final Table table = new Table();
        Builder name(String name) {
            table.name = name;
            return this;
        }
        Builder column(String name, String decl) {
            table.columns.add(new Column(name, decl));
            return this;
        }
        Builder primaryKey(String primaryKeyDecl) {
            table.primaryKey = primaryKeyDecl;
            return this;
        }

        public Table build() {
            return table;
        }
    }

    static Builder Builder() { return new Builder(); }

    String getDropDdl() {
        return String.format("DROP TABLE IF EXISTS %s CASCADE", name);
    }

    String getCreateDdl() {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("CREATE TABLE %s ( ", name));
        boolean is_first = true;
        for (Column c : columns) {
            if (is_first) {
                sb.append(String.format("\n%s %s", c.name, c.decl));
                is_first = false;
            } else {
                // Add comma at end of previous line
                sb.append(String.format(",\n%s %s", c.name, c.decl));
            }
        }
        if (primaryKey != null) {
            sb.append(String.format(",\n PRIMARY KEY %s", primaryKey));
        }
        sb.append("\n)");
        return sb.toString();
    }

    String getInsertDml() {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("INSERT INTO %s VALUES (", name));
        sb.append("?");
        for (int i = 1; i < columns.size(); ++i) {
            sb.append(", ?");
        }
        sb.append(")");
        return (sb.toString());
    }
}

public class SchemaManager {
    private static final Logger LOG = Logger.getLogger(SchemaManager.class);

    private final Connection db_connection;

    private static final Map<String, Table> tables = Stream.of(
        Table.Builder()
            .name(TPCCConstants.TABLENAME_ORDERLINE)
            .column("ol_w_id", "int NOT NULL")
            .column("ol_d_id", "int NOT NULL")
            .column("ol_o_id", "int NOT NULL")
            .column("ol_number", "int NOT NULL")
            .column("ol_i_id", "int NOT NULL")
            .column("ol_delivery_d", "timestamp NULL DEFAULT NULL")
            .column("ol_amount", "decimal(6,2) NOT NULL")
            .column("ol_supply_w_id", "int NOT NULL")
            .column("ol_quantity", "decimal(2,0) NOT NULL")
            .column("ol_dist_info", "char(24) NOT NULL")
            .primaryKey("((ol_w_id,ol_d_id) HASH,ol_o_id,ol_number)")
        .build(),
        Table.Builder()
            .name(TPCCConstants.TABLENAME_NEWORDER)
            .column("no_w_id", "int NOT NULL")
            .column("no_d_id", "int NOT NULL")
            .column("no_o_id", "int NOT NULL")
            .primaryKey("((no_w_id,no_d_id) HASH,no_o_id)")
        .build(),
        Table.Builder()
            .name(TPCCConstants.TABLENAME_STOCK)
            .column("s_w_id", "int NOT NULL")
            .column("s_i_id", "int NOT NULL")
            .column("s_quantity", "decimal(4,0) NOT NULL")
            .column("s_ytd", "decimal(8,2) NOT NULL")
            .column("s_order_cnt", "int NOT NULL")
            .column("s_remote_cnt", "int NOT NULL")
            .column("s_data", "varchar(50) NOT NULL")
            .column("s_dist_01", "char(24) NOT NULL")
            .column("s_dist_02", "char(24) NOT NULL")
            .column("s_dist_03", "char(24) NOT NULL")
            .column("s_dist_04", "char(24) NOT NULL")
            .column("s_dist_05", "char(24) NOT NULL")
            .column("s_dist_06", "char(24) NOT NULL")
            .column("s_dist_07", "char(24) NOT NULL")
            .column("s_dist_08", "char(24) NOT NULL")
            .column("s_dist_09", "char(24) NOT NULL")
            .column("s_dist_10", "char(24) NOT NULL")
            .primaryKey("(s_w_id HASH, s_i_id ASC)")
        .build(),
        Table.Builder()
            .name(TPCCConstants.TABLENAME_OPENORDER)
            .column("o_w_id", "int NOT NULL")
            .column("o_d_id", "int NOT NULL")
            .column("o_id", "int NOT NULL")
            .column("o_c_id", "int NOT NULL")
            .column("o_carrier_id", "int DEFAULT NULL")
            .column("o_ol_cnt", "decimal(2,0) NOT NULL")
            .column("o_all_local", "decimal(1,0) NOT NULL")
            .column("o_entry_d", "timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP")
            .primaryKey("((o_w_id,o_d_id) HASH,o_id)")
        .build(),
        Table.Builder()
            .name(TPCCConstants.TABLENAME_HISTORY)
            .column("h_c_id", "int NOT NULL")
            .column("h_c_d_id", "int NOT NULL")
            .column("h_c_w_id", "int NOT NULL")
            .column("h_d_id", "int NOT NULL")
            .column("h_w_id", "int NOT NULL")
            .column("h_date", "timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP")
            .column("h_amount", "decimal(6,2) NOT NULL")
            .column("h_data",  "varchar(24) NOT NULL")
        .build(),
        Table.Builder()
            .name(TPCCConstants.TABLENAME_CUSTOMER)
            .column("c_w_id", "int NOT NULL")
            .column("c_d_id", "int NOT NULL")
            .column("c_id", "int NOT NULL")
            .column("c_discount", "decimal(4,4) NOT NULL")
            .column("c_credit", "char(2) NOT NULL")
            .column("c_last", "varchar(16) NOT NULL")
            .column("c_first", "varchar(16) NOT NULL")
            .column("c_credit_lim", "decimal(12,2) NOT NULL")
            .column("c_balance", "decimal(12,2) NOT NULL")
            .column("c_ytd_payment", "float NOT NULL")
            .column("c_payment_cnt", "int NOT NULL")
            .column("c_delivery_cnt", "int NOT NULL")
            .column("c_street_1", "varchar(20) NOT NULL")
            .column("c_street_2", "varchar(20) NOT NULL")
            .column("c_city", "varchar(20) NOT NULL")
            .column("c_state", "char(2) NOT NULL")
            .column("c_zip", "char(9) NOT NULL")
            .column("c_phone", "char(16) NOT NULL")
            .column("c_since", "timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP")
            .column("c_middle", "char(2) NOT NULL")
            .column("c_data", "varchar(500) NOT NULL")
            .primaryKey("((c_w_id,c_d_id) HASH,c_id)")
        .build(),
        Table.Builder()
            .name(TPCCConstants.TABLENAME_DISTRICT)
            .column("d_w_id", "int NOT NULL")
            .column("d_id", "int NOT NULL")
            .column("d_ytd", "decimal(12,2) NOT NULL")
            .column("d_tax", "decimal(4,4) NOT NULL")
            .column("d_next_o_id", "int NOT NULL")
            .column("d_name", "varchar(10) NOT NULL")
            .column("d_street_1", "varchar(20) NOT NULL")
            .column("d_street_2", "varchar(20) NOT NULL")
            .column("d_city", "varchar(20) NOT NULL")
            .column("d_state", "char(2) NOT NULL")
            .column("d_zip", "char(9) NOT NULL")
            .primaryKey("((d_w_id,d_id) HASH)")
        .build(),
        Table.Builder()
            .name(TPCCConstants.TABLENAME_ITEM)
            .column("i_id", "int NOT NULL")
            .column("i_name", "varchar(24) NOT NULL")
            .column("i_price", "decimal(5,2) NOT NULL")
            .column("i_data", "varchar(50) NOT NULL")
            .column("i_im_id", "int NOT NULL")
            .primaryKey("(i_id)")
        .build(),
        Table.Builder()
            .name(TPCCConstants.TABLENAME_WAREHOUSE)
            .column("w_id", "int NOT NULL")
            .column("w_ytd", "decimal(12,2) NOT NULL")
            .column("w_tax", "decimal(4,4) NOT NULL")
            .column("w_name", "varchar(10) NOT NULL")
            .column("w_street_1", "varchar(20) NOT NULL")
            .column("w_street_2", "varchar(20) NOT NULL")
            .column("w_city", "varchar(20) NOT NULL")
            .column("w_state", "char(2) NOT NULL")
            .column("w_zip", "char(9) NOT NULL")
            .primaryKey("(w_id)")
        .build()
    ).collect(Collectors.toMap(Table::name, e -> e));

    SchemaManager(Connection db_connection) {
        this.db_connection = db_connection;
    }

    public static Set<String> getTableNames() {
        return tables.keySet();
    }

    public static String getInsertDml(String table_name) {
        Table t = tables.get(table_name);
        if (t == null) {
            throw new RuntimeException(String.format("Unknown table: %s", table_name));
        }
        return t.getInsertDml();
    }

    private void execute(String sql) throws SQLException {
        LOG.info(sql);
        db_connection.createStatement().execute(sql);
    }

    void create() throws SQLException {
        for (Table t : tables.values()) {
            execute(t.getDropDdl());
        }
        for (Table t : tables.values()) {
            execute(t.getCreateDdl());
        }
        // TODO -- can we defer this until after load as well?
        execute("CREATE INDEX idx_customer_name ON customer ((c_w_id,c_d_id) HASH,c_last,c_first)");
        execute("CREATE UNIQUE INDEX idx_order ON oorder ((o_w_id,o_d_id) HASH,o_c_id,o_id DESC)");

        if (!db_connection.getAutoCommit()) {
            db_connection.commit();
        }
    }

    void enableForeignKeyConstraints() throws SQLException {
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
}
