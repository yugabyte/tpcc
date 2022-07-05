package com.oltpbenchmark.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;

/*
 * For geo-partitioned clusters, GeoPartitionPolicy encapsulates the
 * information representing the number of partitions the data set
 * of the TPCC tables must be split into. It also specifies how
 * these partitions should be replicated and placed across different
 * geo-locations.
 * Each partition is typically associated with a tablespace. A tablespace
 * contains placement configuration to specify how the partition should
 * be replicated and placed.
 */
public class GeoPartitionPolicy {
    private final Map<String, PlacementPolicy> tablespaceToPlacementPolicy = new HashMap<>();
    private final Map<String, String> tablegroupToTablespace = new HashMap<>();

    private String tablespaceForPartitionedTables;

    private String tablespaceForItemTable;

    private final List<String> tablespacesForPartitions = new ArrayList<>();
    private final List<String> tablegroupsForPartitions = new ArrayList<>();

    private final int numWarehouses;

    private final int numPartitions;

    private final boolean useTablegroups;

    public GeoPartitionPolicy(int numPartitions, int numWarehouses, boolean useTablegroups) {
        this.numPartitions = numPartitions;
        this.numWarehouses = numWarehouses;
        this.useTablegroups = useTablegroups;
    }

    public int getNumPartitions() {
        return numPartitions;
    }

    public boolean shouldUseTablegroups() {
        return useTablegroups;
    }

    // Getters and setters.
    public Map<String, PlacementPolicy> getTablespaceToPlacementPolicy() {
        return tablespaceToPlacementPolicy;
    }

    public Map<String, String> getTablegroups() {
        return tablegroupToTablespace;
    }

    public String getTablespaceForPartitionedTables() {
        return tablespaceForPartitionedTables;
    }

    public void setTablespaceForPartitionedTables(String tablespaceForPartitionedTables) {
        this.tablespaceForPartitionedTables = tablespaceForPartitionedTables;
    }

    public String getTablespaceForItemTable() {
        return tablespaceForItemTable;
    }

    public void setTablespaceForItemTable(String tablespaceForItemTable) {
        this.tablespaceForItemTable = tablespaceForItemTable;
    }

    public List<String> getTablespacesForPartitions() {
        return tablespacesForPartitions;
    }
    public List<String> getTablegroupsForPartitions() {
        return tablegroupsForPartitions;
    }

    public int getNumWarehouses() {
        return numWarehouses;
    }

    public void addTablespacePlacementPolicy(String tablespace, PlacementPolicy policy) {
        tablespaceToPlacementPolicy.put(tablespace, policy);
    }

    public void addTablegroup(String tablegroup, String tablespace) {
        tablegroupToTablespace.put(tablegroup, tablespace);
    }

    public void addTablespaceForPartition(String tablespace) {
        tablespacesForPartitions.add(tablespace);
    }

    public void addTablegroupForPartition(String tablegroup) {
        tablegroupsForPartitions.add(tablegroup);
    }

    public String getTablespaceCreationJson(String tablespace) {
        PlacementPolicy placementPolicy =  tablespaceToPlacementPolicy.get(tablespace);
        // Handle errors.
        Gson gson = new Gson();
        return gson.toJson(placementPolicy);
    }

    public String getTablespaceForPartition(int idx) {
        return tablespacesForPartitions.get(idx);
    }

    public String getTablegroupForPartition(int idx) {
        return tablegroupsForPartitions.get(idx);
    }

    public int getStartWarehouseForPartition(int idx) {
        return (idx - 1) * numWareHousesPerSplit() + 1;
    }

    public int getEndWarehouseForPartition(int idx) {
        return idx * numWareHousesPerSplit() + 1;
    }

    public int getPartitionForWarehouse(int warehouseId) {
        return ((warehouseId - 1) / numWareHousesPerSplit()) + 1;
    }

    public int numWareHousesPerSplit() {
        return numWarehouses/numPartitions;
    }
}
