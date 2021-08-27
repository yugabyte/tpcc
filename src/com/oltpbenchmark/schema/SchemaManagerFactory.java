package com.oltpbenchmark.schema;

import java.sql.Connection;

import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.schema.defaultschema.DefaultSchemaManager;
import com.oltpbenchmark.schema.geopartitioned.GeoPartitionedSchemaManager;

public class SchemaManagerFactory {
    public static SchemaManager getSchemaManager(WorkloadConfiguration workConf, Connection conn) {
        return workConf.getGeoPartitioningEnabled()
                ? new GeoPartitionedSchemaManager(workConf.getGeoPartitioningPolicy(), conn)
                : new DefaultSchemaManager(conn);
    }
}
