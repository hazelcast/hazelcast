package com.hazelcast.concurrent.atomicreference;

import com.hazelcast.nio.serialization.Data;

public class AtomicReferenceWrapper {

    private Data value;

    public Data get() {
        return value;
    }

    public void set(Data value) {
        this.value = value;
    }

    public boolean compareAndSet(Data expect, Data value) {
        if (!contains(expect)) {
            return false;
        }
        this.value = value;
        return true;
    }

    public boolean contains(Data expected) {
        if (value == null) {
            return expected == null;
        }
        return value.equals(expected);
    }

    public Data getAndSet(Data value) {
        Data tempValue = this.value;
        this.value = value;
        return tempValue;
    }

    public boolean isNull() {
        return value == null;
    }
}
