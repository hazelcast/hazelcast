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

}
