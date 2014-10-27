package com.hazelcast.spi.impl;

import com.hazelcast.spi.BackoffPolicy;
import com.hazelcast.util.ExceptionUtil;

public class ExponentialBackoffPolicy implements BackoffPolicy {
    public static final int BACKOFF_MULTIPLIER = 2;
    public static final int FIRST_STATE = 1;

    @Override
    public int apply(int state) {
        state = nextState(state);
        try {
            Thread.sleep(state);
        } catch (InterruptedException e) {
            throw ExceptionUtil.rethrow(e);
        }
        return state;
    }

    @Override
    public int nextState(int state) {
        if (state == BackoffPolicy.EMPTY_STATE) {
            state = FIRST_STATE;
        } else if (state < 500) {
            state *= BACKOFF_MULTIPLIER;
        } else {
            //do nothing
        }
        return state;
    }
}
