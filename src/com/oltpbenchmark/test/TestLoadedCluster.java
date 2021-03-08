package com.oltpbenchmark.test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public class TestLoadedCluster {

    int selectSingleInt(Connection conn, String query) throws SQLException {
        return conn.createStatement().executeQuery(query).getInt(0);
    }

    void testItems(Connection conn) throws SQLException {
        assert 100000 == selectSingleInt(conn, "SELECT COUNT(*) FROM ITEM");

        assert 10000 >= selectSingleInt(conn, "SELECT MAX(I_IM_ID) FROM ITEM");
        assert 1 <= selectSingleInt(conn, "SELECT MIN(I_IM_ID) FROM ITEM");

        // TODO -- i think this is currently broken in implementation
        assert 14 <= selectSingleInt(conn, "SELECT MIN(LENGTH(I_NAME)) FROM ITEM");
        assert 24 >= selectSingleInt(conn, "SELECT MAX(LENGTH(I_NAME)) FROM ITEM");

        assert 1 <= selectSingleInt(conn, "SELECT MIN(I_PRICE) FROM ITEM");
        assert 100 >= selectSingleInt(conn, "SELECT MAX(I_PRICE) FROM ITEM");


        assert 26 <= selectSingleInt(conn, "SELECT MIN(LENGTH(I_DATA)) FROM ITEM"); // currently 22
        assert 50 >= selectSingleInt(conn, "SELECT MAX(LENGTH(I_DATA)) FROM ITEM"); // currently 49
        int numOriginal = selectSingleInt(conn, "SELECT COUNT(*) FROM ITEM WHERE I_DATA LIKE '%ORIGINAL%'");
        assert 9800 <= numOriginal && numOriginal <= 12000;
    }

    public static void main(String[] args) {
        Connection conn;
    }
}
