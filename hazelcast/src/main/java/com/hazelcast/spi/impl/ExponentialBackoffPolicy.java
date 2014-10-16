package com.hazelcast.spi.impl;

import com.hazelcast.spi.BackoffPolicy;
import com.hazelcast.util.ExceptionUtil;

public class ExponentialBackoffPolicy implements BackoffPolicy {
    public static final int BACKOFF_MULTIPLIER = 2;
    public static final int FIRST_STATE = 1;

    @Override
    public int apply(int state, int iterationNo) {
        if (state == BackoffPolicy.EMPTY_STATE) {
            state = FIRST_STATE;
        } else {
            state *= BACKOFF_MULTIPLIER;
        }
        try {
            Thread.sleep(state);
        } catch (InterruptedException e) {
            throw ExceptionUtil.rethrow(e);
        }
        return state;
    }
}
