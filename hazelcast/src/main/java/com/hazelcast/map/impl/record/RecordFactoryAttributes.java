package com.hazelcast.map.impl.record;

public class RecordFactoryAttributes {
    private final String name;
    private final int partitionId;

    public RecordFactoryAttributes(String name, int partitionId) {
        this.name = name;
        this.partitionId = partitionId;
    }

    public String getName() {
        return name;
    }

    public int getPartitionId() {
        return partitionId;
    }
}
