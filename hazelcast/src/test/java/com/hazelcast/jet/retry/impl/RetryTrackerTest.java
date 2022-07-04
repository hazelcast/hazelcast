/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.retry.impl;

import com.hazelcast.jet.retry.IntervalFunction;
import com.hazelcast.jet.retry.RetryStrategies;
import com.hazelcast.jet.retry.RetryStrategy;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.function.LongSupplier;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class RetryTrackerTest {

    private final NanoTimeSupplier timeSupplier = new NanoTimeSupplier();

    @Test
    public void when_attemptsNegative_retryIndefinitely() {
        RetryStrategy strategy = RetryStrategies.custom()
                .maxAttempts(-1)
                .build();
        RetryTracker tracker = new RetryTracker(strategy, timeSupplier);
        assertFalse(tracker.needsToWait());

        for (int i = 0; i < 1_000_000; i++) {
            tracker.attemptFailed();
            assertTrue(tracker.shouldTryAgain());
            assertTrue(tracker.needsToWait());
            advanceTime(tracker.getNextWaitTimeMs());
            assertFalse(tracker.needsToWait());
        }
    }

    @Test
    public void when_attemptsZero_doNotRetry() {
        RetryStrategy strategy = RetryStrategies.custom()
                .maxAttempts(0)
                .build();
        RetryTracker tracker = new RetryTracker(strategy, timeSupplier);
        assertFalse(tracker.needsToWait());
        tracker.attemptFailed();
        assertFalse(tracker.shouldTryAgain());
    }

    @Test
    public void when_attemptsPositive_doRetry() {
        RetryStrategy strategy = RetryStrategies.custom()
                .maxAttempts(2)
                .build();
        RetryTracker tracker = new RetryTracker(strategy, timeSupplier);
        assertFalse(tracker.needsToWait());

        //1st retry
        tracker.attemptFailed();
        assertTrue(tracker.shouldTryAgain());
        advanceTime(tracker.getNextWaitTimeMs());
        assertFalse(tracker.needsToWait());

        //2nd retry
        tracker.attemptFailed();
        assertTrue(tracker.shouldTryAgain());
        advanceTime(tracker.getNextWaitTimeMs());
        assertFalse(tracker.needsToWait());

        tracker.attemptFailed();
        assertFalse(tracker.shouldTryAgain());
    }

    @Test
    public void when_reset() {
        RetryStrategy strategy = RetryStrategies.custom()
                .maxAttempts(1)
                .build();
        RetryTracker tracker = new RetryTracker(strategy, timeSupplier);
        assertFalse(tracker.needsToWait());

        //1st retry
        tracker.attemptFailed();
        assertTrue(tracker.shouldTryAgain());
        advanceTime(tracker.getNextWaitTimeMs());
        assertFalse(tracker.needsToWait());

        tracker.attemptFailed();
        assertFalse(tracker.shouldTryAgain());

        tracker.reset();

        //1st retry
        tracker.attemptFailed();
        assertTrue(tracker.shouldTryAgain());
        advanceTime(tracker.getNextWaitTimeMs());
        assertFalse(tracker.needsToWait());

        tracker.attemptFailed();
        assertFalse(tracker.shouldTryAgain());
    }

    @Test
    public void when_constantRetryPeriod() {
        RetryStrategy strategy = RetryStrategies.custom()
                .intervalFunction(IntervalFunction.constant(1000))
                .build();
        RetryTracker tracker = new RetryTracker(strategy, timeSupplier);
        assertFalse(tracker.needsToWait());

        tracker.attemptFailed();
        assertTrue(tracker.needsToWait());
        advanceTime(999);
        assertTrue(tracker.needsToWait());
        advanceTime(1);
        assertFalse(tracker.needsToWait());

        tracker.attemptFailed();
        assertTrue(tracker.needsToWait());
        advanceTime(999);
        assertTrue(tracker.needsToWait());
        advanceTime(1);
        assertFalse(tracker.needsToWait());
    }

    @Test
    public void when_exponentialBackoff() {
        RetryStrategy strategy = RetryStrategies.custom()
                .intervalFunction((IntervalFunction) attempt -> attempt * 1000L)
                .build();
        RetryTracker tracker = new RetryTracker(strategy, timeSupplier);
        assertFalse(tracker.needsToWait());

        tracker.attemptFailed();
        assertTrue(tracker.needsToWait());
        advanceTime(999);
        assertTrue(tracker.needsToWait());
        advanceTime(1);
        assertFalse(tracker.needsToWait());

        tracker.attemptFailed();
        assertTrue(tracker.needsToWait());
        advanceTime(1999);
        assertTrue(tracker.needsToWait());
        advanceTime(1);
        assertFalse(tracker.needsToWait());

        tracker.attemptFailed();
        assertTrue(tracker.needsToWait());
        advanceTime(2999);
        assertTrue(tracker.needsToWait());
        advanceTime(1);
        assertFalse(tracker.needsToWait());
    }

    private void advanceTime(long millis) {
        timeSupplier.advance(millis);
    }

    private static class NanoTimeSupplier implements LongSupplier {

        private long value;

        public void advance(long millis) {
            value = value + MILLISECONDS.toNanos(millis);
        }

        @Override
        public long getAsLong() {
            return value;
        }
    }

}
