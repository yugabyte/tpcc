package com.oltpbenchmark;

import java.util.Optional;

import org.apache.commons.configuration.ConfigurationException;

import com.oltpbenchmark.util.GeoPartitionPolicy;
import com.oltpbenchmark.util.InvalidUserConfiguration;
import com.oltpbenchmark.util.PlacementBlock;
import com.oltpbenchmark.util.PlacementPolicy;

public class GeoPartitionedConfigFileOptions extends ConfigFileOptionsBase {

    public GeoPartitionedConfigFileOptions(String filePath) throws ConfigurationException {
        super(filePath);
    }

    private boolean isGeoPartitioningEnabled() {
        return isFlagSet("enableGeoPartitionedWorkload");
    }

    private Optional<Integer> getNumberOfPartitions() {
        return getIntOpt("numberOfPartitions");
    }

    private int getNumTablespaces() {
        return xmlConfig.getList("tablespaces/tablespace/name").size();
    }

    public GeoPartitionPolicy getGeoPartitionPlacement(final int totalWarehousesAcrossShards, int numWarehouses, int startWarehouseIdForShard) {
        // First check whether partitioning enabled.
        if (!isGeoPartitioningEnabled()) {
            // Partitioning is disabled. No point setting or looking for any other options.
            return null;
        }

        // Verify that the total number of warehouses can be evenly divided across partitions.
        final int numPartitions = getNumberOfPartitions().get();
        if (totalWarehousesAcrossShards % numPartitions != 0)  {
            throw new InvalidUserConfiguration(String.format("Invalid values for totalWarehouses (%d) and numPartitions (%d) " +
                                    "Warehouses must be evenly spread across partitions.", totalWarehousesAcrossShards,
                                    numPartitions));
        }

        // Verify that the current TPCC client can access only a single partition.
        GeoPartitionPolicy policy = new GeoPartitionPolicy(numPartitions, totalWarehousesAcrossShards);
        int partitionForStartWarehouse = policy.getPartitionForWarehouse(startWarehouseIdForShard);
        int partitionForEndWarehouse = policy.getPartitionForWarehouse(startWarehouseIdForShard + numWarehouses - 1);

        if (partitionForStartWarehouse != partitionForEndWarehouse) {
            throw new InvalidUserConfiguration(String.format("Invalid values for numPartitions (%d), numWarehouses (%d) " +
                    "startWarehouseId (%d) and totalWarehouses (%d). A client should access only a single partition.",
                    numPartitions, numWarehouses, startWarehouseIdForShard, totalWarehousesAcrossShards));
        }

        // Iterate through all the tablespaces in the config.
        // The lists in XMLConfiguration are 1-based, so start the
        // loop from 1.
        boolean foundTablespaceForPartitionedTables = false;
        boolean foundTablespaceForItemTable = false;
        boolean foundTablespaceForPartitions = false;
        for (int ii = 1; ii <= getNumTablespaces(); ++ii) {
            final String path = String.format("tablespaces/tablespace[%d]/", ii);

            // Get Tablespace name.
            final String name = getRequiredStringOpt(path + "name");

            if (isFlagSet(path + "storePartitionedTables")) {
                policy.setTablespaceForPartitionedTables(name);
                foundTablespaceForPartitionedTables = true;
            }

            if (isFlagSet(path + "storeItemTable")) {
                policy.setTablespaceForItemTable(name);
                foundTablespaceForItemTable = true;
            }

            if (isFlagSet(path + "storePartitions")) {
                policy.addTablespaceForPartition(name);
                foundTablespaceForPartitions = true;
            }

            final int totalReplicas = getIntOpt(path + "replicationFactor").get();
            PlacementPolicy placementPolicy = new PlacementPolicy(totalReplicas);

            // Iterate through placement blocks.
            int numPlacementBlocks = xmlConfig.getList(path + "placementPolicy/placementBlock/cloud").size();
            int totalMinReplicationFactor = 0;
            for (int i = 1; i <= numPlacementBlocks; ++i) {
                String blockPath = String.format(path + "placementPolicy/placementBlock[%d]/", i);

                String cloud = getRequiredStringOpt(blockPath + "cloud");
                String region = getRequiredStringOpt(blockPath + "region");
                String zone = getRequiredStringOpt(blockPath + "zone");
                int minReplicas = getIntOpt(blockPath + "minReplicationFactor").get();
                totalMinReplicationFactor += minReplicas;

                PlacementBlock block = new PlacementBlock(cloud, region, zone, minReplicas);
                placementPolicy.addPlacementBlock(block);
            }
            if (totalMinReplicationFactor > totalReplicas) {
                throw new InvalidUserConfiguration(String.format("Invalid Geoplacement policy: The sum of minReplicationFactor %d" +
                    " is greater than total replication factor %d", totalMinReplicationFactor,
                    totalReplicas));
            }
            policy.addTablespacePlacementPolicy(name, placementPolicy);
        }
        if (!foundTablespaceForPartitionedTables) {
            throw new InvalidUserConfiguration("Invalid Geoplacement policy: No tablespace specified for storing partitioned tables");

        }
        if (!foundTablespaceForItemTable) {
            throw new InvalidUserConfiguration("Invalid Geoplacement policy: No tablespace specified for storing the Item table");
        }
        if (!foundTablespaceForPartitions) {
            throw new InvalidUserConfiguration("Invalid Geoplacement policy: No tablespace specified for storing partition tables");
        }
        final int numTablespacesForPartitions = policy.getTablespacesForPartitions().size();
        if (policy.getNumPartitions() > numTablespacesForPartitions) {
            throw new InvalidUserConfiguration(String.format("Invalid Geoplacement policy: Insufficient number of tablespaces " +
                                    "(%d) to store all partitions (%d)", policy.getNumPartitions(),
                                    numTablespacesForPartitions));
        }
        return policy;
    }


}
