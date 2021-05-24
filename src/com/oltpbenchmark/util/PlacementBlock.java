package com.oltpbenchmark.util;

import com.google.gson.annotations.SerializedName;

/*
 * For GeoPartitioned clusters, the configuration specifying how the
 * tables should be placed is a list of cloud.region.zone.min_number_of_replicas
 * blocks. 'PlacementBlock' class represents one such c.r.z.m block.
 */
public class PlacementBlock {
    private final String cloud;
    private final String region;
    private final String zone;
    
    @SerializedName(value = "min_num_replicas")
    private final int minNumReplicas;

    public PlacementBlock(String cloud, String region, String zone, int minNumReplicas) {
        this.cloud = cloud;
        this.region = region;
        this.zone = zone;
        this.minNumReplicas = minNumReplicas;
    }

    public String getCloud() {
        return cloud;
    }

    public String getRegion() {
        return region;
    }

    public String getZone() {
        return zone;
    }

    public int getMinNumReplicas() {
        return minNumReplicas;
    }
}
