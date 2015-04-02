package com.hazelcast.map.impl;

import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

/**
 * Contains the data related to a map partition event
 */
public class MapPartitionEventData extends AbstractEventData {

    private int partitionId;

    public MapPartitionEventData() {
    }

    public MapPartitionEventData(String source, String mapName, Address caller, int partitionId) {
        super(source, mapName, caller, -1);
        this.partitionId = partitionId;
    }

    public int getPartitionId() {
        return partitionId;
    }

    @Override
    public void writeData(ObjectDataOutput out)
            throws IOException {
        super.writeData(out);
        out.writeInt(partitionId);
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {
        super.readData(in);
        partitionId = in.readInt();
    }

    @Override
    public String toString() {
        return "MapPartitionEventData{"
                + super.toString()
                + ", partitionId=" + partitionId
                + '}';
    }
}
