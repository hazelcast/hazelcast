package com.hazelcast.core;

import com.hazelcast.impl.ThreadContext;
import com.hazelcast.nio.DataSerializable;
import com.hazelcast.nio.SerializationHelper;

import java.io.*;

public abstract class HazelcastInstanceAwareObject implements HazelcastInstanceAware, Serializable {
    protected transient HazelcastInstance hazelcastInstance = null;

    public HazelcastInstance getHazelcastInstance() {
        return hazelcastInstance;
    }

    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        this.hazelcastInstance = hazelcastInstance;
    }

    public void writeData(DataOutput out) throws IOException {
    }

    public void readData(DataInput in) throws IOException {
        hazelcastInstance = ThreadContext.get().getCurrentFactory();
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        writeData(out);
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        readData(in);
    }
}
