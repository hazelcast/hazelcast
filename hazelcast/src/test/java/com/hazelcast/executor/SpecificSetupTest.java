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

import com.hazelcast.config.Config;
import com.hazelcast.config.ExecutorConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.ManagedContext;
import com.hazelcast.core.PartitionAware;
import com.hazelcast.monitor.LocalExecutorStats;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.instance.GroupProperties.PROP_OPERATION_CALL_TIMEOUT_MILLIS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class SpecificSetupTest extends ExecutorServiceTestSupport {

    /*
    @Test
    public void testIssue4667() throws ExecutionException, InterruptedException {
        try {
//            TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
            HazelcastInstance
                    instance1 =
                            Hazelcast.newHazelcastInstance(),
//                            factory.newHazelcastInstance(),
                    instance2 =
                            Hazelcast.newHazelcastInstance();
//                            factory.newHazelcastInstance();
            IExecutorService executorService = instance1.getExecutorService(randomString());
            Future<Boolean> future = executorService.submitToMember(new SleepingTask(1000),
                    instance2.getCluster().getLocalMember());
            assertTrue(future.cancel(true));
            try { future.get(); } catch (Exception ignored) {}
        } finally {
            Hazelcast.shutdownAll();
        }
    }
    */

    @Test
    public void managedContext_mustInitializeRunnable() throws Exception {
        final AtomicBoolean initialized = new AtomicBoolean();
        final Config config = new Config()
                .addExecutorConfig(new ExecutorConfig("test", 1))
                .setManagedContext(new ManagedContext() {
                    @Override public Object initialize(Object obj) {
                        if (obj instanceof RunnableWithManagedContext) {
                            initialized.set(true);
                        }
                        return obj;
                    }
                });
        IExecutorService executor = createHazelcastInstance(config).getExecutorService("test");
        executor.submit(new RunnableWithManagedContext()).get();
        assertTrue("The task should have been initialized by the ManagedContext", initialized.get());
    }

    @Test
    public void statsIssue2039() throws Exception {
        final Config config = new Config();
        final String name = "testStatsIssue2039";
        config.addExecutorConfig(new ExecutorConfig(name).setQueueCapacity(1).setPoolSize(1));
        final HazelcastInstance instance = createHazelcastInstance(config);
        final IExecutorService executorService = instance.getExecutorService(name);
        final SleepLatchRunnable r = new SleepLatchRunnable();
        executorService.execute(r);
        assertTrue(SleepLatchRunnable.startLatch.await(30, SECONDS));
        Future waitingInQueue = executorService.submit(new EmptyRunnable());
        Future rejected = executorService.submit(new EmptyRunnable());
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

        final LocalExecutorStats stats = executorService.getLocalExecutorStats();
        assertEquals(2, stats.getStartedTaskCount());
        assertEquals(0, stats.getPendingTaskCount());
    }

    @Test
    public void operationTimeoutConfigProp() throws Exception {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        Config config = new Config();
        int timeoutSeconds = 3;
        config.setProperty(PROP_OPERATION_CALL_TIMEOUT_MILLIS, String.valueOf(SECONDS.toMillis(timeoutSeconds)));
        HazelcastInstance hz1 = factory.newHazelcastInstance(config);
        HazelcastInstance hz2 = factory.newHazelcastInstance(config);
        IExecutorService executor = hz1.getExecutorService(randomString());
        Future<Boolean> f = executor.submitToMember(new SleepingTask(3 * timeoutSeconds),
                hz2.getCluster().getLocalMember());
        Boolean result = f.get(1, MINUTES);
        assertTrue(result);
    }

    private static class SleepLatchRunnable implements Runnable, Serializable {
        static CountDownLatch startLatch;
        static CountDownLatch sleepLatch;

        SleepLatchRunnable() {
            startLatch = new CountDownLatch(1);
            sleepLatch = new CountDownLatch(1);
        }
        @Override public void run() {
            startLatch.countDown();
            assertOpenEventually(sleepLatch);
        }
    }

    static class RunnableWithManagedContext implements Runnable, Serializable {
        @Override public void run() { }
    }

    static class EmptyRunnable implements Runnable, Serializable, PartitionAware {
        @Override public void run() { }
        @Override public Object getPartitionKey() {
            return "key";
        }
    }
}
