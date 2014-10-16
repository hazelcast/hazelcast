package com.hazelcast.spi;

public interface BackoffPolicy {
    public static final int EMPTY_STATE = 0;

    public int apply(int state, int iterationNo);
}
