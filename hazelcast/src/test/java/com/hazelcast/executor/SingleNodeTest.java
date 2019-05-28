/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.executor;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberSelector;
import com.hazelcast.monitor.LocalExecutorStats;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SingleNodeTest extends ExecutorServiceTestSupport {

    private IExecutorService executor;

    @Before
    public void setUp() {
        executor = createSingleNodeExecutorService("test", 1);
    }

    @Test
    public void hazelcastInstanceAware_expectInjection() throws Throwable {
        HazelcastInstanceAwareRunnable task = new HazelcastInstanceAwareRunnable();
        try {
            executor.submit(task).get();
        } catch (ExecutionException e) {
            throw e.getCause();
        }
    }

    @Test(expected = NullPointerException.class)
    @SuppressWarnings("ConstantConditions")
    public void submitNullTask_expectFailure() {
        executor.submit((Callable<?>) null);
    }

    @Test
    public void submitBasicTask() throws Exception {
        Callable<String> task = new BasicTestCallable();
        Future future = executor.submit(task);
        assertEquals(future.get(), BasicTestCallable.RESULT);
    }

    @Test(expected = RejectedExecutionException.class)
    public void alwaysFalseMemberSelector_expectRejection() {
        HazelcastInstanceAwareRunnable task = new HazelcastInstanceAwareRunnable();
        executor.execute(task, new MemberSelector() {
            @Override
            public boolean select(Member member) {
                return false;
            }
        });
    }

    @Test
    public void executionCallback_notifiedOnSuccess() {
        final CountDownLatch latch = new CountDownLatch(1);
        Callable<String> task = new BasicTestCallable();
        ExecutionCallback<String> executionCallback = new ExecutionCallback<String>() {
            public void onResponse(String response) {
                latch.countDown();
            }

            public void onFailure(Throwable t) {
            }
        };
        executor.submit(task, executionCallback);
        assertOpenEventually(latch);
    }

    @Test
    public void executionCallback_notifiedOnFailure() {
        final CountDownLatch latch = new CountDownLatch(1);
        FailingTestTask task = new FailingTestTask();
        ExecutionCallback<String> executionCallback = new ExecutionCallback<String>() {
            public void onResponse(String response) {
            }

            public void onFailure(Throwable t) {
                latch.countDown();
            }
        };
        executor.submit(task, executionCallback);
        assertOpenEventually(latch);
    }

    @Test(expected = CancellationException.class)
    public void timeOut_thenCancel() throws ExecutionException, InterruptedException {
        SleepingTask task = new SleepingTask(1);
        Future future = executor.submit(task);
        try {
            future.get(1, TimeUnit.MILLISECONDS);
            fail("Should throw TimeoutException!");
        } catch (TimeoutException expected) {
            ignore(expected);
        }
        assertFalse(future.isDone());
        assertTrue(future.cancel(true));
        assertTrue(future.isCancelled());
        assertTrue(future.isDone());
        future.get();
    }

    @Test(expected = CancellationException.class)
    public void cancelWhileQueued() throws ExecutionException, InterruptedException {
        Callable task1 = new SleepingTask(100);
        Future inProgressFuture = executor.submit(task1);

        Callable task2 = new BasicTestCallable();
        // this future should not be an instance of CompletedFuture,
        // because even if we get an exception, isDone is returning true
        Future queuedFuture = executor.submit(task2);

        try {
            assertFalse(queuedFuture.isDone());
            assertTrue(queuedFuture.cancel(true));
            assertTrue(queuedFuture.isCancelled());
            assertTrue(queuedFuture.isDone());
        } finally {
            inProgressFuture.cancel(true);
        }

        queuedFuture.get();
    }

    @Test
    public void isDoneAfterGet() throws Exception {
        Callable<String> task = new BasicTestCallable();
        Future future = executor.submit(task);
        assertEquals(future.get(), BasicTestCallable.RESULT);
        assertTrue(future.isDone());
    }

    @Test
    public void issue129() throws Exception {
        for (int i = 0; i < 1000; i++) {
            Callable<String> task1 = new BasicTestCallable();
            Callable<String> task2 = new BasicTestCallable();
            Future<String> future1 = executor.submit(task1);
            Future<String> future2 = executor.submit(task2);
            assertEquals(future2.get(), BasicTestCallable.RESULT);
            assertTrue(future2.isDone());
            assertEquals(future1.get(), BasicTestCallable.RESULT);
            assertTrue(future1.isDone());
        }
    }

    @Test
    public void issue292() throws Exception {
        final BlockingQueue<Member> qResponse = new ArrayBlockingQueue<Member>(1);
        executor.submit(new MemberCheck(), new ExecutionCallback<Member>() {
            public void onResponse(Member response) {
                qResponse.offer(response);
            }

            public void onFailure(Throwable t) {
            }
        });
        assertNotNull(qResponse.poll(10, TimeUnit.SECONDS));
    }

    @Test(timeout = 10000)
    public void taskSubmitsNestedTask() throws Exception {
        Callable<String> task = new NestedExecutorTask();
        executor.submit(task).get();
    }

    @Test
    public void getManyTimesFromSameFuture() throws Exception {
        Callable<String> task = new BasicTestCallable();
        Future<String> future = executor.submit(task);
        for (int i = 0; i < 4; i++) {
            assertEquals(future.get(), BasicTestCallable.RESULT);
            assertTrue(future.isDone());
        }
    }

    @Test
    public void invokeAll() throws Exception {
        // only one task
        ArrayList<Callable<String>> tasks = new ArrayList<Callable<String>>();
        tasks.add(new BasicTestCallable());
        List<Future<String>> futures = executor.invokeAll(tasks);
        assertEquals(futures.size(), 1);
        assertEquals(futures.get(0).get(), BasicTestCallable.RESULT);
        // more tasks
        tasks.clear();
        for (int i = 0; i < 1000; i++) {
            tasks.add(new BasicTestCallable());
        }
        futures = executor.invokeAll(tasks);
        assertEquals(futures.size(), 1000);
        for (int i = 0; i < 1000; i++) {
            assertEquals(futures.get(i).get(), BasicTestCallable.RESULT);
        }
    }

    @Test
    public void invokeAllTimeoutCancelled() throws Exception {
        List<? extends Callable<Boolean>> singleTask = Collections.singletonList(new SleepingTask(0));
        List<Future<Boolean>> futures = executor.invokeAll(singleTask, 5, TimeUnit.SECONDS);
        assertEquals(futures.size(), 1);
        assertEquals(futures.get(0).get(), Boolean.TRUE);

        List<Callable<Boolean>> tasks = new ArrayList<Callable<Boolean>>();
        for (int i = 0; i < 1000; i++) {
            tasks.add(new SleepingTask(i < 2 ? 0 : 20));
        }
        futures = executor.invokeAll(tasks, 5, TimeUnit.SECONDS);
        assertEquals(futures.size(), 1000);
        for (int i = 0; i < 1000; i++) {
            if (i < 2) {
                assertEquals(futures.get(i).get(), Boolean.TRUE);
            } else {
                try {
                    futures.get(i).get();
                    fail();
                } catch (CancellationException expected) {
                    ignore(expected);
                }
            }
        }
    }

    @Test
    public void invokeAllTimeoutSuccess() throws Exception {
        // only one task
        ArrayList<Callable<String>> tasks = new ArrayList<Callable<String>>();
        tasks.add(new BasicTestCallable());
        List<Future<String>> futures = executor.invokeAll(tasks, 5, TimeUnit.SECONDS);
        assertEquals(futures.size(), 1);
        assertEquals(futures.get(0).get(), BasicTestCallable.RESULT);
        // more tasks
        tasks.clear();
        for (int i = 0; i < 1000; i++) {
            tasks.add(new BasicTestCallable());
        }
        futures = executor.invokeAll(tasks, 5, TimeUnit.SECONDS);
        assertEquals(futures.size(), 1000);
        for (int i = 0; i < 1000; i++) {
            assertEquals(futures.get(i).get(), BasicTestCallable.RESULT);
        }
    }

    /**
     * Shutdown-related method behaviour when the cluster is running
     */
    @Test
    public void shutdownBehaviour() {
        // fresh instance, is not shutting down
        assertFalse(executor.isShutdown());
        assertFalse(executor.isTerminated());
        executor.shutdown();
        assertTrue(executor.isShutdown());
        assertTrue(executor.isTerminated());
        // shutdownNow() should return an empty list and be ignored
        List<Runnable> pending = executor.shutdownNow();
        assertTrue(pending.isEmpty());
        assertTrue(executor.isShutdown());
        assertTrue(executor.isTerminated());
        // awaitTermination() should return immediately false
        try {
            boolean terminated = executor.awaitTermination(60L, TimeUnit.SECONDS);
            assertFalse(terminated);
        } catch (InterruptedException ie) {
            fail("InterruptedException");
        }
        assertTrue(executor.isShutdown());
        assertTrue(executor.isTerminated());
    }

    /**
     * Shutting down the cluster should act as the ExecutorService shutdown
     */
    @Test(expected = RejectedExecutionException.class)
    public void clusterShutdown() {
        shutdownNodeFactory();
        sleepSeconds(2);

        assertNotNull(executor);
        assertTrue(executor.isShutdown());
        assertTrue(executor.isTerminated());

        // new tasks must be rejected
        Callable<String> task = new BasicTestCallable();
        executor.submit(task);
    }

    @Test
    public void executorServiceStats() throws InterruptedException, ExecutionException {
        final int iterations = 10;
        LatchRunnable.latch = new CountDownLatch(iterations);
        LatchRunnable runnable = new LatchRunnable();
        for (int i = 0; i < iterations; i++) {
            executor.execute(runnable);
        }
        assertOpenEventually(LatchRunnable.latch);
        Future<Boolean> future = executor.submit(new SleepingTask(10));
        future.cancel(true);
        try {
            future.get();
        } catch (CancellationException ignored) {
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                LocalExecutorStats stats = executor.getLocalExecutorStats();
                assertEquals(iterations + 1, stats.getStartedTaskCount());
                assertEquals(iterations, stats.getCompletedTaskCount());
                assertEquals(0, stats.getPendingTaskCount());
                assertEquals(1, stats.getCancelledTaskCount());
            }
        });
    }

    static class LatchRunnable implements Runnable, Serializable {

        static CountDownLatch latch;

        @Override
        public void run() {
            latch.countDown();
        }
    }
}
