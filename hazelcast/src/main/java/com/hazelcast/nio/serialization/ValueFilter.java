package com.hazelcast.nio.serialization;

public class ValueFilter {

    public boolean apply(Object value) {
        return true;
    }

    public boolean apply(byte value) {
        return true;
    }

    public boolean apply(short value) {
        return true;
    }

    public boolean apply(int value) {
        return true;
    }

    public boolean apply(long value) {
        return true;
    }

    public boolean apply(float value) {
        return true;
    }

    public boolean apply(double value) {
        return true;
    }

    public boolean apply(boolean value) {
        return true;
    }

    public boolean apply(char value) {
        return true;
    }

}
