package com.hazelcast.core;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import com.hazelcast.impl.ThreadContext;

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
