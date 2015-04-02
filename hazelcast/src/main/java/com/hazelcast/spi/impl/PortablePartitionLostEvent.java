package com.hazelcast.spi.impl;

import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;

import java.io.IOException;

public class PortablePartitionLostEvent
        implements Portable {

    private int partitionId;

    private int lostBackupCount;

    private Address source;

    public PortablePartitionLostEvent() {
    }

    public PortablePartitionLostEvent(int partitionId, int lostBackupCount, Address source) {
        this.partitionId = partitionId;
        this.lostBackupCount = lostBackupCount;
        this.source = source;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public int getLostBackupCount() {
        return lostBackupCount;
    }

    public Address getSource() {
        return source;
    }

    @Override
    public int getFactoryId() {
        return SpiPortableHook.ID;
    }

    @Override
    public int getClassId() {
        return SpiPortableHook.PARTITION_LOST_EVENT;
    }

    @Override
    public void writePortable(PortableWriter writer)
            throws IOException {
        writer.writeInt("p", partitionId);
        writer.writeInt("l", lostBackupCount);
        final ObjectDataOutput out = writer.getRawDataOutput();
        out.writeObject(source);
    }

    @Override
    public void readPortable(PortableReader reader)
            throws IOException {
        partitionId = reader.readInt("p");
        lostBackupCount = reader.readInt("l");
        final ObjectDataInput in = reader.getRawDataInput();
        source = in.readObject();
    }
}
