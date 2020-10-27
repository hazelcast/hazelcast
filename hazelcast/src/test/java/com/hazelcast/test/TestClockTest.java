/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.test;

import com.hazelcast.internal.util.Clock;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.hazelcast.test.HazelcastTestSupport.sleepAtLeastMillis;
import static com.hazelcast.test.HazelcastTestSupport.spawn;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class TestClockTest {

    @Test
    public void testClockStop() {
        TestClock.stop();
        long now = Clock.currentTimeMillis();
        HazelcastTestSupport.sleepAtLeastMillis(300);
        assertEquals(now, Clock.currentTimeMillis());
    }

    @Test
    public void testClockStopAndStart() {
        TestClock.stop();
        long now = Clock.currentTimeMillis();
        HazelcastTestSupport.sleepAtLeastMillis(10);
        TestClock.start();
        assertTrue(now < Clock.currentTimeMillis());
    }

    @Test
    public void testClockStopAndAddDelta() {
        TestClock.stop();
        long now = Clock.currentTimeMillis();
        TestClock.delta(3000);
        assertEquals(now + 3000L, Clock.currentTimeMillis());
    }

    @Test
    public void testClockIsInherited()
            throws ExecutionException, InterruptedException {
        TestClock.stop();
        long now = Clock.currentTimeMillis();
        sleepAtLeastMillis(10);
        Future<Long> nowFromChildThread = spawn(Clock::currentTimeMillis);
        assertEquals(now, nowFromChildThread.get().longValue());
    }

    @Test
    public void testClockInit()
            throws ExecutionException, InterruptedException {
        TestClock.init();
        Future<Long> nowFromChildThread = spawn(() -> {
            TestClock.stop();
            sleepAtLeastMillis(10);
            return Clock.currentTimeMillis();
        });
        assertEquals(nowFromChildThread.get().longValue(), Clock.currentTimeMillis());
    }

    @Test
    public void testDeltaFailsWhenNotStopped() {
        assertThrows(AssertionError.class, () -> TestClock.delta(3000));
    }
}
