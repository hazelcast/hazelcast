package com.hazelcast.spi;

public interface BackoffPolicy {

    int EMPTY_STATE = 0;

    int apply(int state);

    int nextState(int state);
}
