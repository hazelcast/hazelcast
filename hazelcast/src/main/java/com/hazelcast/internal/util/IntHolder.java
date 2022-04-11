package com.hazelcast.internal.util;

public class IntHolder {
    private int integer;

    public void increment() {
        integer++;
    }

    public void setInt(int integer) {
        this.integer = integer;
    }

    public int getInt() {
        return integer;
    }
}
