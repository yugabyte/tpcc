/******************************************************************************
 *  Copyright 2015 by OLTPBenchmark Project                                   *
 *                                                                            *
 *  Licensed under the Apache License, Version 2.0 (the "License");           *
 *  you may not use this file except in compliance with the License.          *
 *  You may obtain a copy of the License at                                   *
 *                                                                            *
 *    http://www.apache.org/licenses/LICENSE-2.0                              *
 *                                                                            *
 *  Unless required by applicable law or agreed to in writing, software       *
 *  distributed under the License is distributed on an "AS IS" BASIS,         *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  *
 *  See the License for the specific language governing permissions and       *
 *  limitations under the License.                                            *
 ******************************************************************************/

package com.oltpbenchmark.util;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import com.oltpbenchmark.catalog.Column;
import com.oltpbenchmark.catalog.Table;

public abstract class SQLUtil {
    /**
     * Simple pretty-print debug method for the current row in the given ResultSet
     */
    public static String debug(ResultSet rs) throws SQLException {
        ResultSetMetaData rs_md = rs.getMetaData();
        int num_cols = rs_md.getColumnCount();
        String[] data = new String[num_cols];
        for (int i = 0; i < num_cols; i++) {
            data[i] = rs.getObject(i+1).toString();
        } // FOR
        
        return (String.format("ROW[%02d] -> [%s]", rs.getRow(), String.join(",", data)));
    }

    /**
     * Automatically generate the single-row 'INSERT' SQL string for this table.
     */
    public static String getInsertSQL(Table catalog_tbl) {
    	StringBuilder sb = new StringBuilder();
    	sb.append("INSERT INTO ")
    	  .append(catalog_tbl.getName());

		// Column Names
		sb.append(" VALUES ").append("(");
		boolean first = true;
    	for (Column ignored : catalog_tbl.getColumns()) {
    		if (!first) {
    			sb.append(", ");
    		}
    		sb.append("?");
    		first = false;
    	}
		sb.append(")");

    	return (sb.toString());
    }
}