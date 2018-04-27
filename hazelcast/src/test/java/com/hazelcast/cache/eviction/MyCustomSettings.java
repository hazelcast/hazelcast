package com.hazelcast.cache.eviction;

import java.io.Serializable;

class MyCustomSettings implements Serializable {
    private int capacity;

    public MyCustomSettings(int capacity) {
        setCapacity(capacity);
    }

    public int getCapacity() {
        return capacity;
    }

    public void setCapacity(int capacity) {
        if (capacity < 0) {
            throw new IllegalArgumentException("Capacity is less than 0!");
        }
        this.capacity = capacity;
    }
}
