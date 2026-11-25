/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.impl;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import static com.hazelcast.spi.impl.AbstractInvocationFuture.UNRESOLVED;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * A response completes the future while an interrupted waiter is in {@code onInterruptDetected()}, and the waiter
 * spends the response's unpark permit before it would park again. The waiter must still return the response.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class AbstractInvocationFuture_InterruptRaceTest extends AbstractInvocationFuture_AbstractTest {

    @Before
    public void useResponseRacingFuture() {
        future = new ResponseRacesInterruptFuture();
    }

    @Test
    public void get_whenResponseRacesInterrupt() throws Exception {
        assertWaiterReturnsValue(future::get);
    }

    @Test
    public void getWithTimeout_whenResponseRacesInterrupt() throws Exception {
        assertWaiterReturnsValue(() -> future.get(5, MINUTES));
    }

    @Test
    public void join_whenResponseRacesInterrupt() throws Exception {
        assertWaiterReturnsValue(future::join);
    }

    @Test
    public void joinInternal_whenResponseRacesInterrupt() throws Exception {
        assertWaiterReturnsValue(future::joinInternal);
    }

    private void assertWaiterReturnsValue(Callable<Object> waitForValue) throws Exception {
        AtomicReference<Thread> waiter = new AtomicReference<>();
        AtomicBoolean interruptRestored = new AtomicBoolean();
        Future<Object> result = spawn(() -> {
            waiter.set(Thread.currentThread());
            Object response = waitForValue.call();
            interruptRestored.set(Thread.currentThread().isInterrupted());
            return response;
        });
        assertTrueEventually(() -> assertNotSame(UNRESOLVED, future.getState()));

        waiter.get().interrupt();

        try {
            assertSame(value, result.get(30, SECONDS));
        } catch (TimeoutException e) {
            AssertionError error = new AssertionError("Waiter is " + waiter.get().getState() + " although future.isDone()="
                    + future.isDone() + ", at " + Arrays.toString(waiter.get().getStackTrace()), e);
            // release the waiter so it does not stay parked in the shared test JVM
            LockSupport.unpark(waiter.get());
            throw error;
        }
        assertTrue(interruptRestored.get());
    }

    private class ResponseRacesInterruptFuture extends TestFuture {

        @Override
        protected void onInterruptDetected() {
            complete(value);
            super.onInterruptDetected();
            // stands in for the double completion warning parking in the logger, for example on a contended ReentrantLock
            LockSupport.parkNanos(1);
        }
    }
}
