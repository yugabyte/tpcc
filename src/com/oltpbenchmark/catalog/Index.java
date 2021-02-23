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

package com.oltpbenchmark.catalog;

import java.util.SortedMap;
import java.util.TreeMap;


import com.oltpbenchmark.types.SortDirectionType;

public class Index extends AbstractCatalogObject {
    private static final long serialVersionUID = 1L;

    private final SortedMap<Integer, IndexColumn> columns = new TreeMap<>();
    
    private static class IndexColumn {
        final String name;
        final SortDirectionType dir;
        IndexColumn(String name, SortDirectionType dir) {
            this.name = name;
            this.dir = dir;
        }
        @Override
        public String toString() {
            return this.name + " / " + this.dir;
        }
    } // CLASS
    
    public Index(String name) {
        super(name);
    }

    public void addColumn(String colName, SortDirectionType colOrder, int colPosition) {
        assert(!this.columns.containsKey(colPosition));
        this.columns.put(colPosition, new IndexColumn(colName, colOrder));
    }


}
