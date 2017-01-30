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
 *
 */

package com.hazelcast.scheduledexecutor;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.ICountDownLatch;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.ExecutionException;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({SlowTest.class, ParallelTest.class})
public class ScheduledExecutorServiceSlowTest
        extends ScheduledExecutorServiceTestSupport {

    @Test
    public void schedule_withLongSleepingCallable_blockingOnGet()
            throws ExecutionException, InterruptedException {

        int delay = 0;
        double expectedResult = 169.4;

        HazelcastInstance[] instances = createClusterWithCount(2);
        ICountDownLatch runsCountLatch = instances[0].getCountDownLatch("runsCountLatchName");
        runsCountLatch.trySetCount(1);

        IScheduledExecutorService executorService = getScheduledExecutor(instances, "s");
        IScheduledFuture<Double> future = executorService.schedule(
                new ScheduledExecutorServiceBasicTest.ICountdownLatchCallableTask("runsCountLatchName", 15000), delay, SECONDS);

        double result = future.get();

        assertEquals(expectedResult, result, 0);
        assertEquals(true, future.isDone());
        assertEquals(false, future.isCancelled());
    }

    @Test
    public void schedule_withStatefulRunnable_durable()
            throws ExecutionException, InterruptedException {

        HazelcastInstance[] instances = createClusterWithCount(4);
        IScheduledExecutorService executorService = getScheduledExecutor(instances, "s");
        int waitStateSyncPeriodToAvoidPassiveState = 2000;

        String key = generateKeyOwnedBy(instances[1]);
        ICountDownLatch latch = instances[0].getCountDownLatch("latch");
        IAtomicLong runC = instances[0].getAtomicLong("runC");
        IAtomicLong loadC = instances[0].getAtomicLong("loadC");

        latch.trySetCount(1);

        IScheduledFuture future = executorService.scheduleOnKeyOwnerAtFixedRate(
                new ScheduledExecutorServiceBasicTest.StatefulRunnableTask("latch", "runC", "loadC"),
                key, 10, 10, SECONDS);

        // Wait for task to get scheduled and start
        latch.await(11, SECONDS);

        Thread.sleep(waitStateSyncPeriodToAvoidPassiveState);

        instances[1].getLifecycleService().shutdown();

        // Reset latch - task should be running on a replica now
        latch.trySetCount(7);
        latch.await(70, SECONDS);
        future.cancel(false);

        assertEquals(getPartitionService(instances[0]).getPartitionId(key), future.getHandler().getPartitionId());
        assertEquals(8, runC.get(), 1);
        assertEquals(1, loadC.get());
    }

    @Test
    public void stats_longRunningTask_durable()
            throws ExecutionException, InterruptedException {
        HazelcastInstance[] instances = createClusterWithCount(4);

        String key = generateKeyOwnedBy(instances[1]);

        ICountDownLatch latch = instances[0].getCountDownLatch("latch");
        latch.trySetCount(6);

        IScheduledExecutorService executorService = getScheduledExecutor(instances, "s");
        IScheduledFuture future = executorService.scheduleOnKeyOwnerAtFixedRate(
                new ScheduledExecutorServiceBasicTest.ICountdownLatchRunnableTask("latch"), key, 0, 10, SECONDS);

        Thread.sleep(12000);

        instances[1].getLifecycleService().shutdown();

        latch.await(70, SECONDS);
        sleepSeconds(4); // Wait for run-cycle to finish before cancelling, in order for stats to get updated.
        future.cancel(false);

        ScheduledTaskStatistics stats = future.getStats();
        assertEquals(6, stats.getTotalRuns());
    }

    @Test
    public void stats_manyRepetitionsTask()
            throws ExecutionException, InterruptedException {
        HazelcastInstance[] instances = createClusterWithCount(4);

        ICountDownLatch latch = instances[0].getCountDownLatch("latch");
        latch.trySetCount(6);

        IScheduledExecutorService executorService = getScheduledExecutor(instances, "s");
        IScheduledFuture future = executorService.scheduleAtFixedRate(
                new ScheduledExecutorServiceBasicTest.ICountdownLatchRunnableTask("latch"), 0, 10, SECONDS);


        latch.await(120, SECONDS);
        sleepSeconds(4); // Wait for run-cycle to finish before cancelling, in order for stats to get updated.
        future.cancel(false);

        ScheduledTaskStatistics stats = future.getStats();
        assertEquals(6, stats.getTotalRuns());
    }

    @Test
    public void scheduleRandomPartitions_getAllScheduled_durable()
            throws ExecutionException, InterruptedException {

        HazelcastInstance[] instances = createClusterWithCount(3);
        IScheduledExecutorService s = getScheduledExecutor(instances, "s");

        int expectedTotal = 11;
        IScheduledFuture[] futures = new IScheduledFuture[expectedTotal];
        for (int i=0; i < expectedTotal; i++) {
            futures[i] = s.schedule(new PlainCallableTask(i), 0, SECONDS);
        }

        instances[1].getLifecycleService().shutdown();

        assertEquals(expectedTotal, countScheduledTasksOn(s), 0);

        // Verify all tasks
        for (int i=0; i < expectedTotal; i++) {
            assertEquals(25.0 + i, futures[i].get());
        }
    }

    @Test
    public void scheduleRandomPartitions_periodicTask_getAllScheduled_durable()
            throws ExecutionException, InterruptedException {

        HazelcastInstance[] instances = createClusterWithCount(3);
        IScheduledExecutorService s = getScheduledExecutor(instances, "s");
        String key = generateKeyOwnedBy(instances[1]);
        String runsCounterName = "runs";

        ICountDownLatch runsLatch = instances[0].getCountDownLatch(runsCounterName);
        runsLatch.trySetCount(2);

        int expectedTotal = 11;
        for (int i=0; i < expectedTotal; i++) {
            s.scheduleOnKeyOwnerAtFixedRate(new ICountdownLatchRunnableTask(runsCounterName), key, 0, 2, SECONDS);
        }

        runsLatch.await(10, SECONDS);
        instances[1].getLifecycleService().shutdown();

        assertEquals(expectedTotal, countScheduledTasksOn(s), 0);
    }

    @Test
    public void schedulePeriodicTask_withMultipleSchedulers_atRandomPartitions_thenGetAllScheduled()
            throws ExecutionException, InterruptedException {

        String runsCounterName = "runs";
        HazelcastInstance[] instances = createClusterWithCount(3);
        ICountDownLatch runsLatch = instances[0].getCountDownLatch(runsCounterName);

        int numOfSchedulers = 10;
        int numOfTasks = 10;
        int expectedTotal = numOfSchedulers * numOfTasks;

        runsLatch.trySetCount(expectedTotal);

        for (int i = 0; i < numOfSchedulers; i++) {
            IScheduledExecutorService s = getScheduledExecutor(instances, "scheduler_" + i);
            String key = generateKeyOwnedBy(instances[1]);

            for (int k = 0; k < numOfTasks; k++) {
                s.scheduleOnKeyOwnerAtFixedRate(new ICountdownLatchRunnableTask(runsCounterName), key, 0, 2, SECONDS);
            }
        }

        runsLatch.await(10, SECONDS);

        int actualTotal = 0;
        for (int i = 0; i < numOfSchedulers; i++) {
            actualTotal += countScheduledTasksOn(getScheduledExecutor(instances, "scheduler_" + i));
        }

        assertEquals(expectedTotal, actualTotal, 0);
    }

    @Test
    public void schedulePeriodicTask_withMultipleSchedulers_atRandomPartitions_shutdownOrDestroy_thenGetAllScheduled()
            throws ExecutionException, InterruptedException {

        String runsCounterName = "runs";
        HazelcastInstance[] instances = createClusterWithCount(3);
        ICountDownLatch runsLatch = instances[0].getCountDownLatch(runsCounterName);

        int numOfSchedulers = 10;
        int numOfTasks = 10;
        int expectedTotal = numOfSchedulers * numOfTasks;

        runsLatch.trySetCount(expectedTotal);

        for (int i = 0; i < numOfSchedulers; i++) {
            IScheduledExecutorService s = getScheduledExecutor(instances, "scheduler_" + i);
            String key = generateKeyOwnedBy(instances[1]);

            for (int k = 0; k < numOfTasks; k++) {
                s.scheduleOnKeyOwnerAtFixedRate(new ICountdownLatchRunnableTask(runsCounterName), key, 0, 2, SECONDS);
            }
        }

        runsLatch.await(10, SECONDS);

        getScheduledExecutor(instances, "scheduler_" + 0).shutdown();
        getScheduledExecutor(instances, "scheduler_" + 1).shutdown();
        getScheduledExecutor(instances, "scheduler_" + 3).destroy();

        int actualTotal = 0;
        for (int i = 0; i < numOfSchedulers; i++) {
            actualTotal += countScheduledTasksOn(getScheduledExecutor(instances, "scheduler_" + i));
        }


        assertEquals(expectedTotal - 3 /*numOfShutdownOrDestroy*/ * numOfTasks, actualTotal, 0);
    }
}
