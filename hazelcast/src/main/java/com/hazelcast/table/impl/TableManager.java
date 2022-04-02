package com.hazelcast.table.impl;


import java.util.HashMap;
import java.util.Map;

public class TableManager {

    private final HashMap[] partitionMaps;

    public TableManager(int partitions) {
        this.partitionMaps = new HashMap[partitions];

        for (int k = 0; k < partitions; k++) {
            partitionMaps[k] = new HashMap();
        }
    }

    public Map get(int partition, StringBuffer name) {
        return partitionMaps[partition];
    }
}
