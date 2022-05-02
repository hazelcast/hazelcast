package com.hazelcast.table.impl;


import com.hazelcast.tpc.OffheapAllocator;
import com.hazelcast.tpc.offheapmap.OffheapMap;

import java.util.HashMap;
import java.util.Map;

public class TableManager {

    private final HashMap[] partitionMaps;
    private final OffheapMap[] offheapMaps;

    public TableManager(int partitions) {
        this.partitionMaps = new HashMap[partitions];
        for (int k = 0; k < partitions; k++) {
            partitionMaps[k] = new HashMap();
        }

        this.offheapMaps = new OffheapMap[partitions];
        for (int k = 0; k < partitions; k++) {
            offheapMaps[k] = new OffheapMap(1024, new OffheapAllocator());
        }
    }

    public Map get(int partition, StringBuffer name) {
        return partitionMaps[partition];
    }

    public OffheapMap getOffheapMap(int partition, StringBuffer name) {
        return offheapMaps[partition];
    }
}
