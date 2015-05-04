/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
@Category(QuickTest.class)
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
    public void submitNullTask_expectFailure() throws Exception {
        executor.submit((Callable<?>)null);
    }

    @Test
    public void submitBasicTask() throws Exception {
        Callable<String> task = new BasicTestTask();
        Future future = executor.submit(task);
        assertEquals(future.get(), BasicTestTask.RESULT);
    }

    @Test(expected = RejectedExecutionException.class)
    public void alwaysFalseMemberSelector_expectRejection() {
        HazelcastInstanceAwareRunnable task = new HazelcastInstanceAwareRunnable();
        executor.execute(task, new MemberSelector() {
            @Override
            public boolean select(final Member member) {
                return false;
            }
        });
    }

    @Test
    public void executionCallback_notifiedOnSuccess() throws Exception {
        Callable<String> task = new BasicTestTask();
        final CountDownLatch latch = new CountDownLatch(1);
        final ExecutionCallback<String> executionCallback = new ExecutionCallback<String>() {
            public void onResponse(String response) {
                latch.countDown();
            }
            public void onFailure(Throwable t) { }
        };
        executor.submit(task, executionCallback);
        assertTrue(latch.await(2, TimeUnit.SECONDS));
    }

    @Test
    public void executionCallback_notifiedOnFailure() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        executor.submit(new FailingTestTask(), new ExecutionCallback<String>() {
            @Override
            public void onResponse(String response) {
            }

            @Override
            public void onFailure(Throwable t) {
                latch.countDown();
            }
        });
        assertTrue(latch.await(10, TimeUnit.SECONDS));
    }

    @Test(expected = CancellationException.class)
    public void timeOut_thenCancel() throws ExecutionException, InterruptedException {
        SleepingTask task = new SleepingTask(1);
        Future future = executor.submit(task);
        try {
            future.get(1, TimeUnit.MILLISECONDS);
            fail("Should throw TimeoutException!");
        } catch (TimeoutException expected) {
            consume(expected);
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
        Future inProgFuture = executor.submit(task1);

        Callable task2 = new BasicTestTask();
        /* This future should not be an instance of CompletedFuture
         * Because even if we get an exception, isDone is returning true */
        Future queuedFuture = executor.submit(task2);

        try {
            assertFalse(queuedFuture.isDone());
            assertTrue(queuedFuture.cancel(true));
            assertTrue(queuedFuture.isCancelled());
            assertTrue(queuedFuture.isDone());
        } catch (AssertionError e) {
            throw e;
        } finally {
            inProgFuture.cancel(true);
        }

        queuedFuture.get();

    }

    @Test
    public void isDoneAfterGet() throws Exception {
        Callable<String> task = new BasicTestTask();
        Future future = executor.submit(task);
        assertEquals(future.get(), BasicTestTask.RESULT);
        assertTrue(future.isDone());
    }

    @Test
    public void issue129() throws Exception {
        for (int i = 0; i < 1000; i++) {
            Callable<String>
                    task1 = new BasicTestTask(),
                    task2 = new BasicTestTask();
            Future<String>
                    future1 = executor.submit(task1),
                    future2 = executor.submit(task2);
            assertEquals(future2.get(), BasicTestTask.RESULT);
            assertTrue(future2.isDone());
            assertEquals(future1.get(), BasicTestTask.RESULT);
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
            public void onFailure(Throwable t) { }
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
        Callable<String> task = new BasicTestTask();
        Future<String> future = executor.submit(task);
        for (int i = 0; i < 4; i++) {
            assertEquals(future.get(), BasicTestTask.RESULT);
            assertTrue(future.isDone());
        }
    }

    @Test
    public void invokeAll() throws Exception {
        // Only one task
        ArrayList<Callable<String>> tasks = new ArrayList<Callable<String>>();
        tasks.add(new BasicTestTask());
        List<Future<String>> futures = executor.invokeAll(tasks);
        assertEquals(futures.size(), 1);
        assertEquals(futures.get(0).get(), BasicTestTask.RESULT);
        // More tasks
        tasks.clear();
        for (int i = 0; i < 1000; i++) {
            tasks.add(new BasicTestTask());
        }
        futures = executor.invokeAll(tasks);
        assertEquals(futures.size(), 1000);
        for (int i = 0; i < 1000; i++) {
            assertEquals(futures.get(i).get(), BasicTestTask.RESULT);
        }
    }

    @Test
    public void invokeAllTimeoutCancelled() throws Exception {
        final List<? extends Callable<Boolean>> singleTask = Collections.singletonList(new SleepingTask(0));
        List<Future<Boolean>> futures = executor.invokeAll(singleTask, 5, TimeUnit.SECONDS);
        assertEquals(futures.size(), 1);
        assertEquals(futures.get(0).get(), Boolean.TRUE);

        final List<Callable<Boolean>> tasks = new ArrayList<Callable<Boolean>>();
        for (int i = 0; i < 1000; i++) {
            tasks.add(new SleepingTask(i < 2 ? 0 : 20));
        }
        futures = executor.invokeAll(tasks, 5, TimeUnit.SECONDS);
        assertEquals(futures.size(), 1000);
        for (int i = 0; i < 1000; i++) {
            if (i < 2) {
                assertEquals(futures.get(i).get(), Boolean.TRUE);
            } else {
                //noinspection EmptyCatchBlock
                try {
                    futures.get(i).get();
                    fail();
                } catch (CancellationException expected) { }
            }
        }
    }

    @Test
    public void invokeAllTimeoutSuccess() throws Exception {
        // Only one task
        ArrayList<Callable<String>> tasks = new ArrayList<Callable<String>>();
        tasks.add(new BasicTestTask());
        List<Future<String>> futures = executor.invokeAll(tasks, 5, TimeUnit.SECONDS);
        assertEquals(futures.size(), 1);
        assertEquals(futures.get(0).get(), BasicTestTask.RESULT);
        // More tasks
        tasks.clear();
        for (int i = 0; i < 1000; i++) {
            tasks.add(new BasicTestTask());
        }
        futures = executor.invokeAll(tasks, 5, TimeUnit.SECONDS);
        assertEquals(futures.size(), 1000);
        for (int i = 0; i < 1000; i++) {
            assertEquals(futures.get(i).get(), BasicTestTask.RESULT);
        }
    }

    /**
     * Shutdown-related method behaviour when the cluster is running
     */
    @Test
    public void shutdownBehaviour() throws Exception {
        // Fresh instance, is not shutting down
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
    public void clusterShutdown() throws Exception {
        shutdownNodeFactory();
        Thread.sleep(2000);

        assertNotNull(executor);
        assertTrue(executor.isShutdown());
        assertTrue(executor.isTerminated());

        // New tasks must be rejected
        Callable<String> task = new BasicTestTask();
        executor.submit(task);
    }

    @Test
    public void executorServiceStats() throws InterruptedException, ExecutionException {
        final int k = 10;
        LatchRunnable.latch = new CountDownLatch(k);
        final LatchRunnable r = new LatchRunnable();
        for (int i = 0; i < k; i++) {
            executor.execute(r);
        }
        assertOpenEventually(LatchRunnable.latch);
        final Future<Boolean> f = executor.submit(new SleepingTask(10));
        f.cancel(true);
        try {
            f.get();
        } catch (CancellationException ignored) {
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                final LocalExecutorStats stats = executor.getLocalExecutorStats();
                assertEquals(k + 1, stats.getStartedTaskCount());
                assertEquals(k, stats.getCompletedTaskCount());
                assertEquals(0, stats.getPendingTaskCount());
                assertEquals(1, stats.getCancelledTaskCount());
            }
        });
    }

    static class LatchRunnable implements Runnable, Serializable {
        static CountDownLatch latch;
        @Override public void run() {
            latch.countDown();
        }
    }
}
