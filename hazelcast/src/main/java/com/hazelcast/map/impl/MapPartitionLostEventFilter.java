package com.hazelcast.map.impl;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.spi.EventFilter;

import java.io.IOException;

public class MapPartitionLostEventFilter implements EventFilter, DataSerializable {

    @Override
    public boolean eval(Object arg) {
        return false;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof MapPartitionLostEventFilter;
    }

    @Override
    public int hashCode() {
        return 0;
    }

}
