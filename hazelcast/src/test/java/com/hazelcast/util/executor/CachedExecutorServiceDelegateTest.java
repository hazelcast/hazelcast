/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.util.executor;

import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static com.hazelcast.test.HazelcastTestSupport.assertOpenEventually;
import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static com.hazelcast.test.HazelcastTestSupport.randomString;
import static com.hazelcast.util.FutureUtil.checkAllDone;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class CachedExecutorServiceDelegateTest {

    private static final String NAME = "test-executor";

    private ManagedExecutorService cachedExecutorService;
    private NodeEngine nodeEngine;

    @Before
    public void setup() {
        cachedExecutorService = new NamedThreadPoolExecutor("test", 0, Integer.MAX_VALUE, 60L, TimeUnit.SECONDS,
                new SynchronousQueue<Runnable>(), Executors.defaultThreadFactory());

        ExecutionService executionService = mock(ExecutionService.class);
        when(executionService.getExecutor(ExecutionService.ASYNC_EXECUTOR))
                .thenReturn(cachedExecutorService);

        nodeEngine = mock(NodeEngine.class);
        when(nodeEngine.getExecutionService()).thenReturn(executionService);
    }

    @After
    public void cleanup() {
        cachedExecutorService.shutdown();
    }

    @Test(expected = IllegalArgumentException.class)
    public void nonPositiveMaxPoolSize() {
        newManagedExecutorService(-1, 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void nonPositiveQueueCapacity() {
        newManagedExecutorService(1, -1);
    }

    @Test
    public void getName() throws Exception {
        ManagedExecutorService executor = newManagedExecutorService();
        assertEquals(NAME, executor.getName());
    }

    @Test
    public void getMaximumPoolSize() throws Exception {
        int maxPoolSize = 123;
        assertEquals(maxPoolSize, newManagedExecutorService(maxPoolSize, 1).getMaximumPoolSize());
    }

    @Test
    public void getPoolSize_whenNoTasksSubmitted() throws Exception {
        assertEquals(0, newManagedExecutorService().getPoolSize());
    }

    @Test
    public void getPoolSize_whenTaskSubmitted() throws Exception {
        int maxPoolSize = 3;
        ManagedExecutorService executorService = newManagedExecutorService(maxPoolSize, 100);

        final CountDownLatch startLatch = new CountDownLatch(maxPoolSize);
        final CountDownLatch finishLatch = new CountDownLatch(1);

        try {
            for (int i = 0; i < maxPoolSize * 2; i++) {
                executorService.execute(new Runnable() {
                    @Override
                    public void run() {
                        startLatch.countDown();
                        assertOpenEventually(finishLatch);
                    }
                });
            }
            assertOpenEventually(startLatch);

            assertEquals(maxPoolSize, executorService.getPoolSize());
        } finally {
            finishLatch.countDown();
        }
    }

    @Test
    public void getQueueSize_whenNoTasksSubmitted() throws Exception {
        assertEquals(0, newManagedExecutorService().getQueueSize());
    }

    @Test
    public void getQueueSize_whenTaskSubmitted() throws Exception {
        int queueSize = 10;
        ManagedExecutorService executorService = newManagedExecutorService(1, queueSize);

        CountDownLatch finishLatch = startLongRunningTask(executorService);
        try {
            executeNopTask(executorService);
            assertEquals(1, executorService.getQueueSize());
            assertEquals(1, executorService.getQueueSize());
        } finally {
            finishLatch.countDown();
        }
    }

    @Test
    public void getRemainingQueueCapacity_whenNoTasksSubmitted() throws Exception {
        int queueSize = 123;
        assertEquals(queueSize, newManagedExecutorService(1, queueSize).getRemainingQueueCapacity());
    }

    @Test
    public void getRemainingQueueCapacity_whenTaskSubmitted() throws Exception {
        int queueSize = 10;
        ManagedExecutorService executorService = newManagedExecutorService(1, queueSize);

        CountDownLatch finishLatch = startLongRunningTask(executorService);
        try {
            executeNopTask(executorService);
            assertEquals(queueSize - 1, executorService.getRemainingQueueCapacity());
        } finally {
            finishLatch.countDown();
        }
    }

    @Test
    public void getCompletedTaskCount_whenNoTasksSubmitted() throws Exception {
        assertEquals(0, newManagedExecutorService().getCompletedTaskCount());
    }

    @Test
    public void getCompletedTaskCount_whenTasksSubmitted() throws Exception {
        final int taskCount = 10;
        final ManagedExecutorService executorService = newManagedExecutorService();

        for (int i = 0; i < taskCount; i++) {
            executeNopTask(executorService);
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(taskCount, executorService.getCompletedTaskCount());
            }
        });
    }

    @Test
    public void execute() throws Exception {
        final int taskCount = 10;
        ManagedExecutorService executorService = newManagedExecutorService(1, taskCount);

        final CountDownLatch latch = new CountDownLatch(taskCount);
        for (int i = 0; i < taskCount; i++) {
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    latch.countDown();
                }
            });
        }

        assertOpenEventually(latch);
    }

    @Test(expected = RejectedExecutionException.class)
    public void execute_rejected_whenShutdown() throws Exception {
        ManagedExecutorService executorService = newManagedExecutorService();
        executorService.shutdown();

        executeNopTask(executorService);
    }

    @Test
    public void submitRunnable() throws Exception {
        final int taskCount = 10;
        ManagedExecutorService executorService = newManagedExecutorService(1, taskCount);

        Future[] futures = new Future[taskCount];
        for (int i = 0; i < taskCount; i++) {
            futures[i] = executorService.submit(new Runnable() {
                @Override
                public void run() {
                }
            });
        }

        checkAllDone(Arrays.asList(futures));
    }

    @Test
    public void submitCallable() throws Exception {
        final int taskCount = 10;
        ManagedExecutorService executorService = newManagedExecutorService(1, taskCount);

        final String result = randomString();
        Future[] futures = new Future[taskCount];
        for (int i = 0; i < taskCount; i++) {
            futures[i] = executorService.submit(new Callable() {
                @Override
                public Object call() throws Exception {
                    return result;
                }
            });
        }

        checkAllDone(Arrays.asList(futures));
        for (Future future : futures) {
            assertEquals(result, future.get());
        }
    }

    @Test
    public void submitRunnable_withResult() throws Exception {
        final int taskCount = 10;
        ManagedExecutorService executorService = newManagedExecutorService(1, taskCount);

        final String result = randomString();
        Future[] futures = new Future[taskCount];
        for (int i = 0; i < taskCount; i++) {
            futures[i] = executorService.submit(new Runnable() {
                @Override
                public void run() {
                }
            }, result);
        }

        checkAllDone(Arrays.asList(futures));
        for (Future future : futures) {
            assertEquals(result, future.get());
        }
    }

    @Test
    public void shutdown() throws Exception {
        ManagedExecutorService executorService = newManagedExecutorService();
        Future<Object> future = executorService.submit(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
                return null;
            }
        });

        executorService.shutdown();
        assertTrue(executorService.isShutdown());
        future.get();
    }

    @Test
    public void shutdownNow() throws Exception {
        ManagedExecutorService executorService = newManagedExecutorService();
        CountDownLatch finishLatch = startLongRunningTask(executorService);
        try {

            Future<Object> future = executorService.submit(new Callable<Object>() {
                @Override
                public Object call() throws Exception {
                    return null;
                }
            });

            List<Runnable> tasks = executorService.shutdownNow();
            assertTrue(executorService.isShutdown());

            assertEquals(1, tasks.size());
            try {
                future.get();
            } catch (CancellationException expected) {
            }
        } finally {
            finishLatch.countDown();
        }
    }

    public void isShutdown_whenRunning() throws Exception {
        assertFalse(newManagedExecutorService().isShutdown());
    }

    @Test
    public void isShutdown() throws Exception {
        ManagedExecutorService executorService = newManagedExecutorService();
        executorService.shutdown();
        assertTrue(executorService.isShutdown());
    }

    @Test
    public void isTerminated() throws Exception {
        ManagedExecutorService executorService = newManagedExecutorService();
        executorService.shutdown();
        assertTrue(executorService.isTerminated());
    }

    @Test
    public void isTerminated_whenRunning() throws Exception {
        assertFalse(newManagedExecutorService().isTerminated());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void awaitTermination() throws Exception {
        newManagedExecutorService().awaitTermination(1, TimeUnit.SECONDS);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void invokeAll() throws Exception {
        newManagedExecutorService().invokeAll(Collections.singleton(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                return null;
            }
        }));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void invokeAll_withTimeout() throws Exception {
        newManagedExecutorService().invokeAll(Collections.singleton(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                return null;
            }
        }), 1, TimeUnit.SECONDS);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void invokeAny() throws Exception {
        newManagedExecutorService().invokeAny(Collections.singleton(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                return null;
            }
        }));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void invokeAny_withTimeout() throws Exception {
        newManagedExecutorService().invokeAny(Collections.singleton(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                return null;
            }
        }), 1, TimeUnit.SECONDS);
    }

    private CountDownLatch startLongRunningTask(ExecutorService executorService) {
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch finishLatch = new CountDownLatch(1);
        executorService.execute(new Runnable() {
            @Override
            public void run() {
                startLatch.countDown();
                assertOpenEventually(finishLatch);
            }
        });
        assertOpenEventually(startLatch);
        return finishLatch;
    }

    private void executeNopTask(ExecutorService executorService) {
        executorService.execute(new Runnable() {
            @Override
            public void run() {
            }
        });
    }

    private ManagedExecutorService newManagedExecutorService() {
        return newManagedExecutorService(1, 10);
    }

    private ManagedExecutorService newManagedExecutorService(int maxPoolSize, int queueCapacity) {
        return new CachedExecutorServiceDelegate(nodeEngine, NAME, cachedExecutorService, maxPoolSize, queueCapacity);
    }
}
