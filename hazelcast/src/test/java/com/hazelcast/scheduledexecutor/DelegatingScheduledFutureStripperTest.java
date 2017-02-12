/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.scheduledexecutor;

import com.hazelcast.scheduledexecutor.impl.DelegatingScheduledFutureStripper;
import com.hazelcast.spi.impl.executionservice.impl.DelegatingTaskScheduler;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class DelegatingScheduledFutureStripperTest {

    ScheduledExecutorService scheduler;
    ExecutorService executor;
    DelegatingTaskScheduler taskScheduler;

    @Before
    public void setup() {
        executor = Executors.newSingleThreadExecutor();
        scheduler = Executors.newSingleThreadScheduledExecutor();
        taskScheduler = new DelegatingTaskScheduler(scheduler, executor);
    }

    @After
    public void teardown()
            throws InterruptedException {
        scheduler.shutdownNow();
        scheduler.awaitTermination(10, TimeUnit.SECONDS);
    }

    @Test(expected = NullPointerException.class)
    public void constructWithNull() {
        new DelegatingScheduledFutureStripper(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void compareTo() {
        ScheduledFuture<Integer> future = new DelegatingScheduledFutureStripper<Integer>(
                scheduler.schedule(new SimpleCallableTestTask(), 0, TimeUnit.SECONDS));
        future.compareTo(null);
    }

    @Test
    public void getDelay() {
        ScheduledFuture<Integer> future = new DelegatingScheduledFutureStripper<Integer>(
                scheduler.schedule(new SimpleCallableTestTask(), 0, TimeUnit.SECONDS));
        assertEquals(0, future.getDelay(TimeUnit.SECONDS));

        future = new DelegatingScheduledFutureStripper<Integer>(
                scheduler.schedule(new SimpleCallableTestTask(), 10, TimeUnit.SECONDS));
        assertEquals(10, future.getDelay(TimeUnit.SECONDS), 1);
    }

    @Test
    public void cancel()
            throws ExecutionException, InterruptedException {
        ScheduledFuture outter = mock(ScheduledFuture.class);
        ScheduledFuture inner = mock(ScheduledFuture.class);
        when(outter.get()).thenReturn(inner);

        new DelegatingScheduledFutureStripper(outter).cancel(true);

        verify(inner).cancel(eq(true));
    }

    @Test
    public void cancel_twice()
            throws ExecutionException, InterruptedException, TimeoutException {
        ScheduledFuture original = taskScheduler.schedule(new SimpleCallableTestTask(), 10, TimeUnit.SECONDS);
        ScheduledFuture stripper = new DelegatingScheduledFutureStripper(original);

        stripper.cancel(true);
        stripper.cancel(true);
    }


    @Test
    public void isDone()
            throws ExecutionException, InterruptedException {
        ScheduledFuture outter = mock(ScheduledFuture.class);
        ScheduledFuture inner = mock(ScheduledFuture.class);
        when(outter.get()).thenReturn(inner);

        when(outter.isDone()).thenReturn(true);
        when(inner.isDone()).thenReturn(false);
        assertFalse(new DelegatingScheduledFutureStripper(outter).isDone());

        when(outter.isDone()).thenReturn(true);
        when(inner.isDone()).thenReturn(true);
        assertTrue(new DelegatingScheduledFutureStripper(outter).isDone());
    }

    @Test
    public void isCancelled()
            throws ExecutionException, InterruptedException {
        ScheduledFuture outter = mock(ScheduledFuture.class);
        ScheduledFuture inner = mock(ScheduledFuture.class);
        when(outter.get()).thenReturn(inner);

        when(outter.isCancelled()).thenReturn(false);
        when(inner.isCancelled()).thenReturn(false);
        assertFalse(new DelegatingScheduledFutureStripper(outter).isCancelled());

        when(outter.isCancelled()).thenReturn(true);
        when(inner.isCancelled()).thenReturn(false);
        assertTrue(new DelegatingScheduledFutureStripper(outter).isCancelled());

        when(outter.isCancelled()).thenReturn(false);
        when(inner.isCancelled()).thenReturn(true);
        assertTrue(new DelegatingScheduledFutureStripper(outter).isCancelled());
    }

    @Test
    public void get()
            throws ExecutionException, InterruptedException {
        ScheduledFuture original = taskScheduler.schedule(new SimpleCallableTestTask(), 0, TimeUnit.SECONDS);
        ScheduledFuture stripper = new DelegatingScheduledFutureStripper(original);

        assertTrue(original.get() instanceof Future);
        assertEquals(5, stripper.get());
    }

    @Test(expected = InterruptedException.class)
    public void get_interrupted()
            throws ExecutionException, InterruptedException {
        ScheduledFuture outter = mock(ScheduledFuture.class);
        ScheduledFuture inner = mock(ScheduledFuture.class);
        when(outter.get()).thenThrow(new InterruptedException());
        when(inner.get()).thenReturn(2);

        new DelegatingScheduledFutureStripper(outter).get();
    }

    @Test(expected = ExecutionException.class)
    public void get_executionExc()
            throws ExecutionException, InterruptedException {
        ScheduledFuture outter = mock(ScheduledFuture.class);
        ScheduledFuture inner = mock(ScheduledFuture.class);
        when(outter.get()).thenThrow(new ExecutionException(new NullPointerException()));
        when(inner.get()).thenReturn(2);

        new DelegatingScheduledFutureStripper(outter).get();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void get_unsupported()
            throws ExecutionException, InterruptedException, TimeoutException {
        ScheduledFuture future = scheduler.schedule(new SimpleCallableTestTask(), 0, TimeUnit.SECONDS);
        new DelegatingScheduledFutureStripper(future).get(1, TimeUnit.SECONDS);
    }

    @Test
    public void equals() {
        ScheduledFuture original = taskScheduler.schedule(new SimpleCallableTestTask(), 0, TimeUnit.SECONDS);
        ScheduledFuture joker = taskScheduler.schedule(new SimpleCallableTestTask(), 1, TimeUnit.SECONDS);

        ScheduledFuture testA = new DelegatingScheduledFutureStripper(original);
        ScheduledFuture testB = new DelegatingScheduledFutureStripper(original);
        ScheduledFuture testC = new DelegatingScheduledFutureStripper(joker);

        assertTrue(testA.equals(testA));
        assertTrue(testA.equals(testB));
        assertFalse(testA.equals(null));
        assertFalse(testA.equals(testC));
    }

    private static class SimpleCallableTestTask
            implements Callable<Integer> {
        @Override
        public Integer call()
                throws Exception {
            return 5;
        }
    }

    private static class SimpleRunnableTestTask
            implements Runnable {
        @Override
        public void run() {
        }
    }
}
