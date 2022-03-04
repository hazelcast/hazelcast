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

package com.hazelcast.scheduledexecutor.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.ScheduledExecutorConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.IAtomicLong;
import com.hazelcast.cp.ICountDownLatch;
import com.hazelcast.scheduledexecutor.IScheduledExecutorService;
import com.hazelcast.scheduledexecutor.IScheduledFuture;
import com.hazelcast.scheduledexecutor.ScheduledTaskStatistics;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.scheduledexecutor.TaskUtils.named;
import static com.hazelcast.test.Accessors.getPartitionService;
import static java.lang.String.valueOf;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category({SlowTest.class, ParallelJVMTest.class})
public class ScheduledExecutorServiceSlowTest extends ScheduledExecutorServiceTestSupport {

    @Test
    public void schedule_withLongSleepingCallable_blockingOnGet() throws Exception {
        int delay = 0;
        double expectedResult = 169.4;

        HazelcastInstance[] instances = createClusterWithCount(2);

        ICountDownLatch initCountLatch = instances[0].getCPSubsystem().getCountDownLatch("initCountLatchName");
        initCountLatch.trySetCount(1);

        ICountDownLatch waitCountLatch = instances[0].getCPSubsystem().getCountDownLatch("waitCountLatchName");
        waitCountLatch.trySetCount(1);

        ICountDownLatch doneCountLatch = instances[0].getCPSubsystem().getCountDownLatch("doneCountLatchName");
        doneCountLatch.trySetCount(1);

        IScheduledExecutorService executorService = getScheduledExecutor(instances, "s");
        IScheduledFuture<Double> future = executorService.schedule(
                new ICountdownLatchCallableTask(initCountLatch.getName(), waitCountLatch.getName(), doneCountLatch.getName()), delay, SECONDS);

        assertOpenEventually(initCountLatch);

        int sleepPeriod = 10000;
        long start = System.currentTimeMillis();
        new Thread(() -> {
            sleepAtLeastMillis(sleepPeriod);
            waitCountLatch.countDown();
        }).start();

        double result = future.get();

        assertTrue(System.currentTimeMillis() - start > sleepPeriod);
        assertTrue(doneCountLatch.await(0, SECONDS));
        assertEquals(expectedResult, result, 0);
        assertTrue(future.isDone());
        assertFalse(future.isCancelled());
    }

    @Test
    public void schedule_withStatefulRunnable_durable() throws Exception {
        HazelcastInstance[] instances = createClusterWithCount(4);
        IScheduledExecutorService executorService = getScheduledExecutor(instances, "s");
        int waitStateSyncPeriodToAvoidPassiveState = 2000;

        String key = generateKeyOwnedBy(instances[1]);
        ICountDownLatch latch = instances[0].getCPSubsystem().getCountDownLatch("latch");
        IAtomicLong runC = instances[0].getCPSubsystem().getAtomicLong("runC");
        IAtomicLong loadC = instances[0].getCPSubsystem().getAtomicLong("loadC");

        latch.trySetCount(1);

        IScheduledFuture future = executorService.scheduleOnKeyOwnerAtFixedRate(
                new StatefulRunnableTask("latch", "runC", "loadC"),
                key, 10, 10, SECONDS);

        // wait for task to get scheduled and start
        latch.await(11, SECONDS);

        Thread.sleep(waitStateSyncPeriodToAvoidPassiveState);

        instances[1].getLifecycleService().shutdown();

        // reset latch - task should be running on a replica now
        latch.trySetCount(7);
        latch.await(70, SECONDS);
        future.cancel(false);

        assertEquals(getPartitionService(instances[0]).getPartitionId(key), future.getHandler().getPartitionId());
        assertEquals(8, runC.get(), 1);
        assertEquals(1, loadC.get());
    }

    @Test
    public void stats_longRunningTask_durable() throws Exception {
        HazelcastInstance[] instances = createClusterWithCount(4);

        String key = generateKeyOwnedBy(instances[1]);

        ICountDownLatch firstLatch = instances[0].getCPSubsystem().getCountDownLatch("firstLatch");
        firstLatch.trySetCount(2);

        ICountDownLatch lastLatch = instances[0].getCPSubsystem().getCountDownLatch("lastLatch");
        lastLatch.trySetCount(6);

        IScheduledExecutorService executorService = getScheduledExecutor(instances, "s");
        IScheduledFuture future = executorService.scheduleOnKeyOwnerAtFixedRate(
                new ICountdownLatchRunnableTask("firstLatch", "lastLatch"), key, 0, 10, SECONDS);

        firstLatch.await(12, SECONDS);

        instances[1].getLifecycleService().shutdown();

        lastLatch.await(70, SECONDS);
        // wait for run-cycle to finish before cancelling, in order for stats to get updated
        sleepSeconds(4);
        future.cancel(false);

        ScheduledTaskStatistics stats = future.getStats();
        assertEquals(6, stats.getTotalRuns(), 1);
    }

    @Test
    public void stats_manyRepetitionsTask() throws Exception {
        HazelcastInstance[] instances = createClusterWithCount(4);

        ICountDownLatch latch = instances[0].getCPSubsystem().getCountDownLatch("latch");
        latch.trySetCount(6);

        IScheduledExecutorService executorService = getScheduledExecutor(instances, "s");
        IScheduledFuture future = executorService.scheduleAtFixedRate(
                new ICountdownLatchRunnableTask("latch"), 0, 10, SECONDS);

        latch.await(120, SECONDS);
        future.cancel(false);

        ScheduledTaskStatistics stats = future.getStats();
        assertEquals(6, stats.getTotalRuns(), 1);
    }

    @Test
    public void scheduleRandomPartitions_getAllScheduled_durable() throws Exception {
        ScheduledExecutorConfig scheduledExecutorConfig = new ScheduledExecutorConfig()
                .setName("s")
                .setDurability(2);

        Config config = new Config()
                // keep the partition count low, makes test faster, and chances of partition loss, less
                .setProperty("hazelcast.partition.count", "10")
                .addScheduledExecutorConfig(scheduledExecutorConfig);

        HazelcastInstance[] instances = createClusterWithCount(4, config);
        IScheduledExecutorService s = getScheduledExecutor(instances, "s");

        int expectedTotal = 11;
        IScheduledFuture[] futures = new IScheduledFuture[expectedTotal];
        for (int i = 0; i < expectedTotal; i++) {
            futures[i] = s.schedule(new PlainCallableTask(i), 0, SECONDS);
        }

        instances[1].shutdown();

        assertEquals(expectedTotal, countScheduledTasksOn(s), 0);

        // verify all tasks
        for (int i = 0; i < expectedTotal; i++) {
            assertEquals(25.0 + i, futures[i].get());
        }
    }

    @Test
    public void scheduleRandomPartitions_periodicTask_getAllScheduled_durable() throws Exception {
        HazelcastInstance[] instances = createClusterWithCount(3);
        IScheduledExecutorService s = getScheduledExecutor(instances, "s");
        String key = generateKeyOwnedBy(instances[1]);
        String runsCounterName = "runs";

        ICountDownLatch runsLatch = instances[0].getCPSubsystem().getCountDownLatch(runsCounterName);
        runsLatch.trySetCount(2);

        int expectedTotal = 11;
        for (int i = 0; i < expectedTotal; i++) {
            s.scheduleOnKeyOwnerAtFixedRate(new ICountdownLatchRunnableTask(runsCounterName), key, 0, 2, SECONDS);
        }

        runsLatch.await(10, SECONDS);
        instances[1].getLifecycleService().shutdown();

        assertEquals(expectedTotal, countScheduledTasksOn(s), 0);
    }

    @Test
    public void schedulePeriodicTask_withMultipleSchedulers_atRandomPartitions_thenGetAllScheduled() throws Exception {
        String runsCounterName = "runs";
        HazelcastInstance[] instances = createClusterWithCount(3);
        ICountDownLatch runsLatch = instances[0].getCPSubsystem().getCountDownLatch(runsCounterName);

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
            throws Exception {
        String runsCounterName = "runs";
        HazelcastInstance[] instances = createClusterWithCount(3);
        ICountDownLatch runsLatch = instances[0].getCPSubsystem().getCountDownLatch(runsCounterName);

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

        assertEquals(expectedTotal - 3 * numOfTasks, actualTotal, 0);
    }

    @Test
    public void schedulePeriodicTask_withMultipleSchedulers_atRandomPartitions_killMember_thenGetAllScheduled() throws Exception {
        String runsCounterName = "runs";
        HazelcastInstance[] instances = createClusterWithCount(10);
        ICountDownLatch runsLatch = instances[0].getCPSubsystem().getCountDownLatch(runsCounterName);

        int numOfSchedulers = 20;
        int numOfTasks = 10;
        int expectedTotal = numOfSchedulers * numOfTasks;

        runsLatch.trySetCount(expectedTotal);

        for (int i = 0; i < numOfSchedulers; i++) {
            IScheduledExecutorService s = getScheduledExecutor(instances, "scheduler_" + i);
            String key = generateKeyOwnedBy(instances[i % instances.length]);

            for (int k = 0; k < numOfTasks; k++) {
                s.scheduleOnKeyOwner(new ICountdownLatchRunnableTask(runsCounterName), key, 0, SECONDS);
            }
        }

        runsLatch.await(10, SECONDS);

        instances[1].getLifecycleService().terminate();

        int actualTotal = 0;
        for (int i = 0; i < numOfSchedulers; i++) {
            actualTotal += countScheduledTasksOn(getScheduledExecutor(instances, "scheduler_" + i));
        }

        assertEquals(expectedTotal, actualTotal, 0);
    }

    @Test
    public void cancelUninterruptedTask_waitUntilRunCompleted_checkStatusIsCancelled() throws Exception {
        HazelcastInstance[] instances = createClusterWithCount(1);

        String runFinishedLatchName = "runFinishedLatch";
        ICountDownLatch latch = instances[0].getCPSubsystem().getCountDownLatch(runFinishedLatchName);
        latch.trySetCount(1);

        IScheduledExecutorService executorService = getScheduledExecutor(instances, "s");
        IScheduledFuture future = executorService.scheduleAtFixedRate(new HotLoopBusyTask(runFinishedLatchName), 0, 1, SECONDS);

        assertFalse(future.isCancelled());
        assertFalse(future.isDone());

        future.cancel(false);

        assertTrue(future.isCancelled());
        assertTrue(future.isDone());

        // wait till the task is actually done, since even though we cancelled the task is current task is still running
        latch.await(60, SECONDS);

        // make sure SyncState goes through
        sleepSeconds(10);

        // check once more that the task status is consistent
        assertTrue(future.isCancelled());
        assertTrue(future.isDone());
    }

    @Test
    public void cancelUninterruptedTask_waitUntilRunCompleted_killMember_checkStatusIsCancelled() throws Exception {
        HazelcastInstance[] instances = createClusterWithCount(2);

        String key = generateKeyOwnedBy(instances[1]);

        String runFinishedLatchName = "runFinishedLatch";
        ICountDownLatch latch = instances[0].getCPSubsystem().getCountDownLatch(runFinishedLatchName);
        latch.trySetCount(1);

        IScheduledExecutorService executorService = getScheduledExecutor(instances, "s");
        IScheduledFuture future = executorService.scheduleOnKeyOwnerAtFixedRate(
                new HotLoopBusyTask(runFinishedLatchName), key, 0, 1, SECONDS);

        assertFalse(future.isCancelled());
        assertFalse(future.isDone());

        future.cancel(false);

        assertTrue(future.isCancelled());
        assertTrue(future.isDone());

        // wait till the task is actually done, since even though we cancelled the task is current task is still running
        latch.await(60, SECONDS);

        // make sure SyncState goes through
        sleepSeconds(10);

        instances[1].getLifecycleService().terminate();

        // check once more that the task status is consistent
        assertTrue(future.isCancelled());
        assertTrue(future.isDone());
    }

    @Test
    public void reschedulingAfterMigration_whenCurrentNodePreviouslyOwnedTask() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        Config config = new Config();
        config.addScheduledExecutorConfig(new ScheduledExecutorConfig("scheduler").setCapacity(1000));

        HazelcastInstance first = factory.newHazelcastInstance(config);

        int tasksCount = 1000;
        final IScheduledExecutorService scheduler = first.getScheduledExecutorService("scheduler");
        for (int i = 1; i <= tasksCount; i++) {
            scheduler.scheduleAtFixedRate(named(valueOf(i), new EchoTask()), 5, 10, SECONDS);
        }

        assertTrueEventually(new AllTasksRunningWithinNumOfNodes(scheduler, 1));

        // start a second member
        HazelcastInstance second = factory.newHazelcastInstance();
        waitAllForSafeState(first, second);

        assertTrueEventually(new AllTasksRunningWithinNumOfNodes(scheduler, 2));

        // kill the second member, tasks should now get rescheduled back in first member
        second.getLifecycleService().terminate();
        waitAllForSafeState(first);

        assertTrueEventually(new AllTasksRunningWithinNumOfNodes(scheduler, 1));
    }

    @Test(timeout = 1800000)
    public void schedule_thenDisposeLeakTest() {
        Config config = new Config()
                .addScheduledExecutorConfig(new ScheduledExecutorConfig()
                        .setName("s")
                        .setCapacity(10000));

        HazelcastInstance[] instances = createClusterWithCount(2, config);
        final IScheduledExecutorService executorService = getScheduledExecutor(instances, "s");

        final AtomicBoolean running = new AtomicBoolean(true);
        long counter = 0;
        long limit = 2000000;

        Executors.newSingleThreadExecutor().submit(new Runnable() {
            @Override
            public void run() {
                while (running.get()) {
                    for (Collection<IScheduledFuture<Object>> collection : executorService.getAllScheduledFutures().values()) {
                        for (IScheduledFuture future : collection) {
                            if (future.getStats().getTotalRuns() >= 1) {
                                future.dispose();
                            }
                        }
                    }
                }
            }
        });

        while (running.get()) {
            try {
                executorService.schedule(new PlainCallableTask(), 1, SECONDS);
                Thread.yield();

                if (counter++ % 1000 == 0) {
                    System.out.println("Tasks: " + counter);
                }

                if (counter >= limit) {
                    running.set(false);
                }
            } catch (Exception ex) {
                ex.printStackTrace();
                running.set(false);
            }
        }

        // wait for running tasks to finish, keeping log clean of PassiveMode exceptions
        sleepSeconds(5);
    }
}
