package com.hazelcast.util;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith (HazelcastSerialClassRunner.class)
@Category (QuickTest.class)
public class ExponentialBackoffSleeperTest
{
    @Test
    public void testSleep() throws InterruptedException
    {
        ExponentialBackoffSleeper sleeper = ExponentialBackoffSleeper.builder()
                .factor(1.2)
                .initialMs(20)
                .maxMs(35)
                .build();

        assertEquals(20L, sleeper.sleep());
        assertEquals(24L, sleeper.sleep());
        assertEquals(29L, sleeper.sleep());
        assertEquals(35L, sleeper.sleep());
        assertEquals(35L, sleeper.sleep());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFactorNegative() {
        ExponentialBackoffSleeper.builder().factor(-2d);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFactorTooSmall() {
        ExponentialBackoffSleeper.builder().factor(0.9d);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInitialNegative() {
        ExponentialBackoffSleeper.builder().initialMs(-100);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMaxTooSmall() {
        ExponentialBackoffSleeper.builder().maxMs(-100);
    }

    @Test
    public void testTimeout() throws InterruptedException {
        ExponentialBackoffSleeper sleeper = ExponentialBackoffSleeper.builder()
                .factor(1.1)
                .initialMs(100)
                .timeoutMs(250)
                .build();

        assertEquals(100L, sleeper.sleep());
        assertEquals(110L, sleeper.sleep());
        assertFalse(sleeper.isTimedOut());

        long time = sleeper.sleep();
        // Expected = 250 - (100 + 110) = 40ms
        // In verification, allow 20ms leeway either way for test randomness and build agent slowness
        String realSleepTime = "actual sleep time was " + time;
        assertTrue(realSleepTime, time <= 60L);
        assertTrue(realSleepTime, time >= 20L);
        assertTrue(realSleepTime + ", but the sleeper isn't detecting it's timed out", sleeper.isTimedOut());

        // verify that no further sleeps are done after the timer has timed out
        assertEquals(0L, sleeper.sleep());
    }
}
