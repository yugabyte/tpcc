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

/**
 * Column Catalog Object
 * @author pavlo
 */
public class Column extends AbstractCatalogObject implements Cloneable {
	private static final long serialVersionUID = 1L;
	
	private final Table catalog_tbl;

    public Column(Table catalog_tbl, String name) {
    	super(name);
        this.catalog_tbl = catalog_tbl;
    }
    
    @Override
    protected Object clone() {
        try {
            return super.clone();
        } catch (CloneNotSupportedException ex) {
            throw new RuntimeException(ex);
        }
    }

    public int getIndex() {
        return (this.catalog_tbl.getColumnIndex(this));
    }
    
    public String fullName() {
        return String.format("%s.%s", this.catalog_tbl.getName(), this.name);
    }

    
    @Override
    public String toString() {
        return (this.getName());
    }
    
}
