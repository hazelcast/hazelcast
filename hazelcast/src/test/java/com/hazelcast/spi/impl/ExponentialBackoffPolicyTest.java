package com.hazelcast.spi.impl;

import com.hazelcast.spi.BackoffPolicy;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.SlowTest;
import com.hazelcast.util.Clock;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.*;

@RunWith(HazelcastParallelClassRunner.class)
@Category(SlowTest.class)
public class ExponentialBackoffPolicyTest {

    private final int[] minWaitingTime = new int[]{0, 1, 2, 4, 8, 16, 32, 64, 128, 256, 512};

    @Test
    public void testBackOff() {
        ExponentialBackoffPolicy backoffPolicy = new ExponentialBackoffPolicy();

        int state = BackoffPolicy.EMPTY_STATE;
        for (int i = 1; i <= 10; i++) {
            long startTime = Clock.currentTimeMillis();
            state = backoffPolicy.apply(state, i);
            long endTime = Clock.currentTimeMillis();
            long diff = endTime - startTime;
            assertTrue("Wrong waiting time calculation in iteration no " + i + ". Expected: " + minWaitingTime[i]
                    + ", Actual: " + diff, diff >= minWaitingTime[i]);
        }
    }
}
