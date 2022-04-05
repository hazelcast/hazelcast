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

package com.hazelcast.durableexecutor;

import com.hazelcast.cluster.Member;
import com.hazelcast.executor.ExecutorServiceTestSupport;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DurableSingleNodeTest extends ExecutorServiceTestSupport {

    private DurableExecutorService executor;

    @Before
    public void setUp() {
        executor = createSingleNodeDurableExecutorService("test", 1);
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
        Future<String> future = executor.submit(task);
        assertEquals(future.get(), BasicTestCallable.RESULT);
    }

    @Test
    public void executionCallback_notifiedOnSuccess() {
        final CountDownLatch latch = new CountDownLatch(1);
        Callable<String> task = new BasicTestCallable();
        Runnable callback = latch::countDown;
        executor.submit(task).toCompletableFuture().thenRun(callback);
        assertOpenEventually(latch);
    }

    @Test
    public void executionCallback_notifiedOnFailure() {
        final CountDownLatch latch = new CountDownLatch(1);
        FailingTestTask task = new FailingTestTask();
        executor.submit(task).toCompletableFuture().exceptionally(v -> {
            latch.countDown();
            return null;
        });
        assertOpenEventually(latch);
    }

    @Test
    public void isDoneAfterGet() throws Exception {
        Callable<String> task = new BasicTestCallable();
        Future<String> future = executor.submit(task);
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
        final BlockingQueue<Member> responseQueue = new ArrayBlockingQueue<>(1);
        executor.submit(new MemberCheck()).toCompletableFuture().thenAccept(responseQueue::offer);
        assertNotNull(responseQueue.poll(10, TimeUnit.SECONDS));
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

    /**
     * Shutdown-related method behaviour when the cluster is running.
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

    // FIXME as soon as executor.getLocalExecutorStats() is implemented
    //@Test
    //public void executorServiceStats() throws Exception {
    //    final int executeCount = 10;
    //    LatchRunnable.latch = new CountDownLatch(executeCount);
    //    final LatchRunnable r = new LatchRunnable();
    //    for (int i = 0; i < executeCount; i++) {
    //        executor.execute(r);
    //    }
    //    assertOpenEventually(LatchRunnable.latch);
    //    final Future<Boolean> future = executor.submit(new SleepingTask(10));
    //    future.cancel(true);
    //    try {
    //        future.get();
    //    } catch (CancellationException ignored) {
    //    }
    //
    //    assertTrueEventually(new AssertTask() {
    //        @Override
    //        public void run()
    //                throws Exception {
    //            final LocalExecutorStats stats = executor.getLocalExecutorStats();
    //            assertEquals(executeCount + 1, stats.getStartedTaskCount());
    //            assertEquals(executeCount, stats.getCompletedTaskCount());
    //            assertEquals(0, stats.getPendingTaskCount());
    //            assertEquals(1, stats.getCancelledTaskCount());
    //        }
    //    });
    //}
}
