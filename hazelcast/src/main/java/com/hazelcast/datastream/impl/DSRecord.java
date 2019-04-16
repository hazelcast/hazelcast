package com.hazelcast.datastream.impl;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

public class DSRecord implements IdentifiedDataSerializable {
    public int partitionId;
    public long offset;
    public Object data;
    public String stream;

    @Override
    public int getFactoryId() {
        return 0;
    }

    @Override
    public int getId() {
        return 0;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(offset);
        // out.writeInt(data);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {

    }
}
