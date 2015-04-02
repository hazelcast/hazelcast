package com.hazelcast.spi.impl;

import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;

import java.io.IOException;

public class PortableMapPartitionLostEvent implements Portable {

    private int partitionId;

    private String uuid;

    public PortableMapPartitionLostEvent() {
    }

    public PortableMapPartitionLostEvent(int partitionId, String uuid) {
        this.partitionId = partitionId;
        this.uuid = uuid;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public String getUuid() {
        return uuid;
    }

    @Override
    public int getFactoryId() {
        return SpiPortableHook.ID;
    }

    @Override
    public int getClassId() {
        return SpiPortableHook.MAP_PARTITION_LOST_EVENT;
    }

    @Override
    public void writePortable(PortableWriter writer)
            throws IOException {
        writer.writeInt("p", partitionId);
        writer.writeUTF("u", uuid);

    }

    @Override
    public void readPortable(PortableReader reader)
            throws IOException {
        partitionId = reader.readInt("p");
        uuid = reader.readUTF("u");
    }
}
