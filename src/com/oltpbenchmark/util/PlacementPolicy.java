package com.oltpbenchmark.util;

import java.util.ArrayList;
import java.util.List;

import com.google.gson.annotations.SerializedName;

/*
 * For GeoPartitioned clusters, the configuration specifying how the
 * tables should be placed is a list of cloud.region.zone.min_number_of_replicas
 * blocks in addition to the total replication factor.
 * 
 * This class encapsulates such a placement policy. 
 */
 
public class PlacementPolicy {
    @SerializedName(value = "num_replicas")
    private final int numReplicas;
    
    @SerializedName(value = "placement_blocks")
    private final List<PlacementBlock> placementBlocks = new ArrayList<PlacementBlock>();

    public PlacementPolicy(int numReplicas) {
        this.numReplicas = numReplicas;
    }

    public void addPlacementBlock(PlacementBlock block) {
        placementBlocks.add(block);
    }
};