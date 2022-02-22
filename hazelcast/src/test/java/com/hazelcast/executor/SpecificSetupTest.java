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

package com.hazelcast.executor;

import com.hazelcast.config.Config;
import com.hazelcast.config.ExecutorConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.partition.PartitionAware;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.spi.properties.ClusterProperty.OPERATION_CALL_TIMEOUT_MILLIS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SpecificSetupTest extends ExecutorServiceTestSupport {

    @Test
    public void managedContext_mustInitializeRunnable() throws Exception {
        final AtomicBoolean initialized = new AtomicBoolean();
        Config config = smallInstanceConfig()
                .addExecutorConfig(new ExecutorConfig("test", 1))
                .setManagedContext(obj -> {
                    if (obj instanceof RunnableWithManagedContext) {
                        initialized.set(true);
                    }
                    return obj;
                });
        IExecutorService executor = createHazelcastInstance(config).getExecutorService("test");
        executor.submit(new RunnableWithManagedContext()).get();
        assertTrue("The task should have been initialized by the ManagedContext", initialized.get());
    }

    @Test
    public void statsIssue2039() throws Exception {
        Config config = smallInstanceConfig();
        String name = "testStatsIssue2039";
        config.addExecutorConfig(new ExecutorConfig(name).setQueueCapacity(1).setPoolSize(1));
        HazelcastInstance instance = createHazelcastInstance(config);
        IExecutorService executorService = instance.getExecutorService(name);
        SleepLatchRunnable runnable = new SleepLatchRunnable();
        executorService.execute(runnable);
        assertTrue(SleepLatchRunnable.startLatch.await(30, SECONDS));
        Future<?> waitingInQueue = executorService.submit(new EmptyRunnable());
        Future<?> rejected = executorService.submit(new EmptyRunnable());
        try {
            rejected.get(1, MINUTES);
        } catch (Exception e) {
            if (!(e.getCause() instanceof RejectedExecutionException)) {
                fail(e.toString());
            }
        } finally {
            SleepLatchRunnable.sleepLatch.countDown();
        }

        waitingInQueue.get(1, MINUTES);

        LocalExecutorStats stats = executorService.getLocalExecutorStats();
        assertEquals(2, stats.getStartedTaskCount());
        assertEquals(0, stats.getPendingTaskCount());
    }

    @Test
    public void operationTimeoutConfigProp() throws Exception {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        Config config = smallInstanceConfig();
        int timeoutSeconds = 3;
        config.setProperty(OPERATION_CALL_TIMEOUT_MILLIS.getName(), String.valueOf(SECONDS.toMillis(timeoutSeconds)));
        HazelcastInstance hz1 = factory.newHazelcastInstance(config);
        HazelcastInstance hz2 = factory.newHazelcastInstance(config);
        IExecutorService executor = hz1.getExecutorService(randomString());
        Future<Boolean> future = executor.submitToMember(new SleepingTask(3 * timeoutSeconds),
                hz2.getCluster().getLocalMember());
        Boolean result = future.get(1, MINUTES);
        assertTrue(result);
    }

    private static class SleepLatchRunnable implements Runnable, Serializable {

        static CountDownLatch startLatch;
        static CountDownLatch sleepLatch;

        SleepLatchRunnable() {
            startLatch = new CountDownLatch(1);
            sleepLatch = new CountDownLatch(1);
        }

        @Override
        public void run() {
            startLatch.countDown();
            assertOpenEventually(sleepLatch);
        }
    }

    static class RunnableWithManagedContext implements Runnable, Serializable {
        @Override
        public void run() {
        }
    }

    static class EmptyRunnable implements Runnable, Serializable, PartitionAware<String> {
        @Override
        public void run() {
        }

        @Override
        public String getPartitionKey() {
            return "key";
        }
    }
}
