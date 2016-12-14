/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.Config;
import com.hazelcast.config.ScheduledExecutorConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.ICountDownLatch;
import com.hazelcast.core.IMap;
import com.hazelcast.core.Member;
import com.hazelcast.core.PartitionAware;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.scheduledexecutor.impl.DistributedScheduledExecutorService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.partition.IPartitionLostEvent;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.executor.ManagedExecutorService;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.scheduledexecutor.TaskUtils.named;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ScheduledExecutorServiceTest extends HazelcastTestSupport {

    protected TestHazelcastInstanceFactory factory;

    @After
    public void tearDown() {
        if (factory != null) {
            factory.shutdownAll();
        }
    }

    @Test
    public void config()
            throws ExecutionException, InterruptedException {

        String schedulerName = "foobar";

        ScheduledExecutorConfig sec = new ScheduledExecutorConfig()
                .setName(schedulerName)
                .setDurability(5)
                .setPoolSize(24);

        Config config = new Config().addScheduledExecutorConfig(sec);

        HazelcastInstance[] instances = createClusterWithCount(1, config);
        IScheduledFuture future = instances[0].getScheduledExecutorService(schedulerName)
                                              .schedule(new PlainCallableTask(), 0, TimeUnit.SECONDS);

        NodeEngineImpl nodeEngine = getNodeEngineImpl(instances[0]);
        ManagedExecutorService mes = (ManagedExecutorService) nodeEngine.getExecutionService()
                                                                        .getScheduledDurable(sec.getName());
        DistributedScheduledExecutorService dses = nodeEngine.getService(DistributedScheduledExecutorService.SERVICE_NAME);

        assertNotNull(mes);
        assertEquals(24, mes.getMaximumPoolSize());
        assertEquals(5, dses.getPartition(future.getHandler().getPartitionId())
                            .getOrCreateContainer(schedulerName).getDurability());
        assertEquals(1, dses.getPartition(future.getHandler().getPartitionId())
                            .getOrCreateContainer("other").getDurability());
    }

    @Test
    public void handlerTaskAndSchedulerNames_withCallable()
            throws ExecutionException, InterruptedException {

        int delay = 0;
        String schedulerName = "s";
        String taskName = "TestCallable";

        HazelcastInstance[] instances = createClusterWithCount(2);
        IScheduledExecutorService executorService = instances[0].getScheduledExecutorService(schedulerName);
        IScheduledFuture<Double> future = executorService.schedule(
                named(taskName, new PlainCallableTask()), delay, TimeUnit.SECONDS);

        future.get();

        ScheduledTaskHandler handler = future.getHandler();
        assertEquals(schedulerName, handler.getSchedulerName());
        assertEquals(taskName, handler.getTaskName());
    }

    @Test
    public void handlerTaskAndSchedulerNames_withRunnable()
            throws ExecutionException, InterruptedException {

        int delay = 0;
        String schedulerName = "s";
        String taskName = "TestRunnable";

        HazelcastInstance[] instances = createClusterWithCount(2);

        ICountDownLatch latch = instances[0].getCountDownLatch("latch");
        latch.trySetCount(1);

        IScheduledExecutorService executorService = instances[0].getScheduledExecutorService(schedulerName);
        IScheduledFuture future = executorService.schedule(
                named(taskName, new ICountdownLatchRunnableTask("latch", instances[0])), delay, TimeUnit.SECONDS);


        latch.await(10, TimeUnit.SECONDS);

        ScheduledTaskHandler handler = future.getHandler();
        assertEquals(schedulerName, handler.getSchedulerName());
        assertEquals(taskName, handler.getTaskName());
    }

    @Test
    @Ignore
    public void stats()
            throws ExecutionException, InterruptedException {
        double delay = 2.0;
        double delayGracePeriod = 2.0; //Could fail sporadically, if so will remove

        HazelcastInstance[] instances = createClusterWithCount(2);
        Object key = generateKeyOwnedBy(instances[1]);
        IScheduledExecutorService executorService = getScheduledExecutor(instances, "s");
        IScheduledFuture<Double> future = executorService.scheduleOnKeyOwner(
                 new PlainCallableTask(), key, (int) delay, TimeUnit.SECONDS);

        future.get();
        ScheduledTaskStatistics stats = future.getStats();

        assertEquals(1, stats.getTotalRuns());
        assertNotNull(stats.getLastIdleTime(TimeUnit.SECONDS));
        assertNotNull(stats.getLastRunDuration(TimeUnit.SECONDS));
        assertNotNull(stats.getTotalIdleTime(TimeUnit.SECONDS));
        assertNotNull(stats.getTotalRunTime(TimeUnit.SECONDS));
        assertNotNull(stats.getTotalRuns());
    }

    @Test
    @Ignore
    public void stats_whenMemberOwned()
            throws ExecutionException, InterruptedException {
        double delay = 2.0;
        double delayGracePeriod = 2.0; //Could fail sporadically, if so will remove

        HazelcastInstance[] instances = createClusterWithCount(2);
        IScheduledExecutorService executorService = getScheduledExecutor(instances, "s");
        IScheduledFuture<Double> future = executorService.scheduleOnMember(
                new PlainCallableTask(), instances[0].getCluster().getLocalMember(), (int) delay, TimeUnit.SECONDS);

        future.get();
        ScheduledTaskStatistics stats = future.getStats();

        assertEquals(1, stats.getTotalRuns());
        assertNotNull(stats.getLastIdleTime(TimeUnit.SECONDS));
        assertNotNull(stats.getLastRunDuration(TimeUnit.SECONDS));
        assertNotNull(stats.getTotalIdleTime(TimeUnit.SECONDS));
        assertNotNull(stats.getTotalRunTime(TimeUnit.SECONDS));
        assertNotNull(stats.getTotalRuns());
    }

    @Test
    public void stats_manyRepetitionsTask()
            throws ExecutionException, InterruptedException {
        HazelcastInstance[] instances = createClusterWithCount(4);

        ICountDownLatch latch = instances[0].getCountDownLatch("latch");
        latch.trySetCount(6);

        IScheduledExecutorService executorService = getScheduledExecutor(instances, "s");
        IScheduledFuture future = executorService.scheduleAtFixedRate(
                new ICountdownLatchRunnableTask("latch", instances[0]), 0, 10, TimeUnit.SECONDS);


        latch.await(120, TimeUnit.SECONDS);
        future.cancel(false);

        ScheduledTaskStatistics stats = future.getStats();
        assertEquals(6, stats.getTotalRuns());
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
                new ICountdownLatchRunnableTask("latch", instances[0]), key, 0, 10, TimeUnit.SECONDS);

        Thread.sleep(12000);

        instances[1].getLifecycleService().shutdown();

        latch.await(70, TimeUnit.SECONDS);
        future.cancel(false);

        ScheduledTaskStatistics stats = future.getStats();
        assertEquals(6, stats.getTotalRuns());
    }


    @Test
    public void schedule_withCallable()
            throws ExecutionException, InterruptedException {

        int delay = 5;
        double expectedResult = 25.0;

        HazelcastInstance[] instances = createClusterWithCount(2);
        IScheduledExecutorService executorService = getScheduledExecutor(instances, "s");
        IScheduledFuture<Double> future = executorService.schedule(
                new PlainCallableTask(), delay, TimeUnit.SECONDS);

        double result = future.get();

        assertEquals(expectedResult, result, 0);
        assertEquals(true, future.isDone());
        assertEquals(false, future.isCancelled());
    }

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
                new ICountdownLatchCallableTask("runsCountLatchName", 15000, instances[0]), delay, TimeUnit.SECONDS);

        double result = future.get();

        assertEquals(expectedResult, result, 0);
        assertEquals(true, future.isDone());
        assertEquals(false, future.isCancelled());
    }

    @Test
    public void schedule_withMapChanges_durable()
            throws ExecutionException, InterruptedException {

        int delay = 0;

        HazelcastInstance[] instances = createClusterWithCount(2);
        IMap<String, Integer> map = instances[1].getMap("map");
        for (int i = 0; i < 100000; i++) {
            map.put(String.valueOf(i), i);
        }

        Object key = generateKeyOwnedBy(instances[0]);
        ICountDownLatch runsCountLatch = instances[1].getCountDownLatch("runsCountLatchName");
        runsCountLatch.trySetCount(1);

        IAtomicLong runEntryCounter = instances[1].getAtomicLong("runEntryCounterName");

        IScheduledExecutorService executorService = getScheduledExecutor(instances, "s");
        executorService.scheduleOnKeyOwner(
                new ICountdownLatchMapIncrementCallableTask("map", "runEntryCounterName",
                        "runsCountLatchName", instances[0]), key, delay, TimeUnit.SECONDS);

        Thread.sleep(2000);
        instances[0].getLifecycleService().shutdown();

        runsCountLatch.await(2, TimeUnit.MINUTES);

        for (int i = 0; i < 100000; i++) {
            assertTrue(map.get(String.valueOf(i)) == (i + 1));
        }

        assertEquals(2, runEntryCounter.get());

    }

    @Test
    public void schedule_withLongSleepingCallable_cancelledAndGet()
            throws ExecutionException, InterruptedException {

        int delay = 0;

        HazelcastInstance[] instances = createClusterWithCount(2);
        ICountDownLatch runsCountLatch = instances[0].getCountDownLatch("runsCountLatchName");
        runsCountLatch.trySetCount(1);

        IScheduledExecutorService executorService = getScheduledExecutor(instances, "s");
        IScheduledFuture<Double> future = executorService.schedule(
                new ICountdownLatchCallableTask("runsCountLatchName", 15000, instances[0]), delay, TimeUnit.SECONDS);

        Thread.sleep(4000);
        future.cancel(false);

        runsCountLatch.await(15, TimeUnit.SECONDS);

        assertEquals(true, future.isDone());
        assertEquals(true, future.isCancelled());
    }

    @Test
    public void schedule_withNegativeDelay()
            throws ExecutionException, InterruptedException {

        int delay = -2;
        double expectedResult = 25.0;

        HazelcastInstance[] instances = createClusterWithCount(2);
        IScheduledExecutorService executorService = getScheduledExecutor(instances, "s");
        IScheduledFuture<Double> future = executorService.schedule(
                new PlainCallableTask(), delay, TimeUnit.SECONDS);

        double result = future.get();

        assertEquals(expectedResult, result, 0);
        assertEquals(true, future.isDone());
        assertEquals(false, future.isCancelled());
    }

    @Test(expected = DuplicateTaskException.class)
    public void schedule_duplicate()
            throws ExecutionException, InterruptedException {
        int delay = 1;
        String taskName = "Test";

        HazelcastInstance[] instances = createClusterWithCount(2);
        IScheduledExecutorService executorService = getScheduledExecutor(instances, "s");
        executorService.schedule(
                named(taskName, new PlainCallableTask()), delay, TimeUnit.SECONDS);

        executorService.schedule(
                named(taskName, new PlainCallableTask()), delay, TimeUnit.SECONDS);
    }

    @Test(expected = CancellationException.class)
    public void schedule_thenCancelAndGet()
            throws ExecutionException, InterruptedException {
        int delay = 1;
        String taskName = "Test";

        HazelcastInstance[] instances = createClusterWithCount(2);
        IScheduledExecutorService executorService = getScheduledExecutor(instances, "s");
        IScheduledFuture<Double> first = executorService.schedule(
                named(taskName, new PlainCallableTask()), delay, TimeUnit.MINUTES);

        first.cancel(false);
        first.get();
    }

    @Test()
    public void schedule_getDelay()
            throws ExecutionException, InterruptedException {
        int delay = 20;
        String taskName = "Test";

        HazelcastInstance[] instances = createClusterWithCount(2);
        IScheduledExecutorService executorService = getScheduledExecutor(instances, "s");
        IScheduledFuture<Double> first = executorService.schedule(
                named(taskName, new PlainCallableTask()), delay, TimeUnit.MINUTES);

        assertEquals(19, first.getDelay(TimeUnit.MINUTES));
    }

    @Test()
    public void schedule_cancel()
            throws ExecutionException, InterruptedException {

        HazelcastInstance[] instances = createClusterWithCount(2);

        ICountDownLatch latch = instances[0].getCountDownLatch("latch");
        latch.trySetCount(1);

        IScheduledExecutorService executorService = getScheduledExecutor(instances, "s");
        IScheduledFuture future = executorService.scheduleAtFixedRate(
                new ICountdownLatchRunnableTask("latch", instances[0]), 1, 1, TimeUnit.SECONDS);


        Thread.sleep(5000);

        assertFalse(future.isCancelled());
        assertFalse(future.isDone());

        future.cancel(false);

        assertTrue(future.isCancelled());
        assertTrue(future.isDone());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void schedule_compareTo()
            throws ExecutionException, InterruptedException {

        HazelcastInstance[] instances = createClusterWithCount(2);

        IScheduledExecutorService executorService = getScheduledExecutor(instances, "s");
        IScheduledFuture<Double> first = executorService.schedule(
                new PlainCallableTask(), 1, TimeUnit.MINUTES);
        IScheduledFuture<Double> second = executorService.schedule(
                new PlainCallableTask(), 2, TimeUnit.MINUTES);

        assertTrue(first.compareTo(second) == -1);
    }

    @Test(expected = StaleTaskException.class)
    public void schedule_thenDisposeThenGet()
            throws ExecutionException, InterruptedException {
        int delay = 1;
        String taskName = "Test";

        HazelcastInstance[] instances = createClusterWithCount(2);
        IScheduledExecutorService executorService = getScheduledExecutor(instances, "s");
        IScheduledFuture<Double> first = executorService.schedule(
                named(taskName, new PlainCallableTask()), delay, TimeUnit.SECONDS);

        first.dispose();
        first.get();
    }

    @Test(expected = RejectedExecutionException.class)
    public void schedule_whenShutdown()
            throws ExecutionException, InterruptedException {
        int delay = 1;

        HazelcastInstance[] instances = createClusterWithCount(2);
        IScheduledExecutorService executorService = getScheduledExecutor(instances, "s");
        executorService.schedule(new PlainCallableTask(), delay, TimeUnit.SECONDS);
        executorService.shutdown();

        executorService.schedule(new PlainCallableTask(), delay, TimeUnit.SECONDS);
    }
    @Test()
    public void schedule_whenPartitionLost()
            throws ExecutionException, InterruptedException {
        int delay = 1;

        HazelcastInstance[] instances = createClusterWithCount(2);
        IScheduledExecutorService executorService = getScheduledExecutor(instances, "s");
        final IScheduledFuture future = executorService.schedule(new PlainCallableTask(), delay, TimeUnit.SECONDS);
        ScheduledTaskHandler handler = future.getHandler();

        int partitionOwner = handler.getPartitionId();
        IPartitionLostEvent internalEvent = new IPartitionLostEvent(partitionOwner, 1, null);
        ((InternalPartitionServiceImpl) getNodeEngineImpl(instances[0]).getPartitionService()).onPartitionLost(internalEvent);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                try {
                    future.get();
                } catch (IllegalStateException ex) {
                    assertEquals("Partition holding this Scheduled task was lost along with all backups.",
                            ex.getMessage());
                }
            }
        });
    }

    @Test(expected = StaleTaskException.class)
    public void schedule_getHandlerDisposeThenRecreateFutureAndGet()
            throws ExecutionException, InterruptedException {
        int delay = 1;
        String taskName = "Test";

        HazelcastInstance[] instances = createClusterWithCount(2);
        IScheduledExecutorService executorService = getScheduledExecutor(instances, "s");
        IScheduledFuture<Double> first = executorService.schedule(
                named(taskName, new PlainCallableTask()), delay, TimeUnit.SECONDS);

        ScheduledTaskHandler handler = first.getHandler();
        first.dispose();

        executorService.getScheduledFuture(handler).get();
    }

    @Test()
    public void schedule_partitionAware()
            throws ExecutionException, InterruptedException {
        int delay = 1;

        HazelcastInstance[] instances = createClusterWithCount(2);
        IScheduledExecutorService executorService = getScheduledExecutor(instances, "s");
        Callable<Double> task = new PlainPartitionAwareCallableTask();
        IScheduledFuture<Double> first = executorService.schedule(
                task, delay, TimeUnit.SECONDS);


        ScheduledTaskHandler handler = first.getHandler();
        int expectedPartition = instances[0].getPartitionService()
                                            .getPartition(((PartitionAware<String>) task).getPartitionKey())
                                            .getPartitionId();
        assertEquals(expectedPartition, handler.getPartitionId());
    }


    @Test
    public void schedule_withStatefulRunnable()
            throws ExecutionException, InterruptedException {

        HazelcastInstance[] instances = createClusterWithCount(4);
        IScheduledExecutorService executorService = getScheduledExecutor(instances, "s");

        ICountDownLatch latch = instances[0].getCountDownLatch("latch");
        latch.trySetCount(1);

        executorService.schedule(
                new StatefullRunnableTask("latch", "runC", "loadC", instances[1]), 2, TimeUnit.SECONDS);

        latch.await(10, TimeUnit.SECONDS);
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
                new StatefullRunnableTask("latch", "runC", "loadC", instances[1]),
                key, 10, 10, TimeUnit.SECONDS);

        // Wait for task to get scheduled and start
        latch.await(11, TimeUnit.SECONDS);

        Thread.sleep(waitStateSyncPeriodToAvoidPassiveState);

        instances[1].getLifecycleService().shutdown();

        // Reset latch - task should be running on a replica now
        latch.trySetCount(7);
        latch.await(70, TimeUnit.SECONDS);
        future.cancel(false);

        assertEquals(getPartitionService(instances[0]).getPartitionId(key), future.getHandler().getPartitionId());
        assertEquals(8, runC.get(), 1);
        assertEquals(1, loadC.get());
    }

    @Test
    public void scheduleWithRepetition()
            throws ExecutionException, InterruptedException {
        HazelcastInstance[] instances = createClusterWithCount(2);

        IScheduledExecutorService s = getScheduledExecutor(instances, "s");

        ICountDownLatch latch = instances[0].getCountDownLatch("latch");
        latch.trySetCount(3);

        IScheduledFuture future = s.scheduleAtFixedRate(new ICountdownLatchRunnableTask("latch", instances[0]),
                0, 1, TimeUnit.SECONDS);

        latch.await(10, TimeUnit.SECONDS);
        future.cancel(false);

        assertEquals(0, latch.getCount());
    }

    @Test
    public void scheduleOnMember()
            throws ExecutionException, InterruptedException {
        int delay = 1;

        HazelcastInstance[] instances = createClusterWithCount(2);
        IScheduledExecutorService executorService = getScheduledExecutor(instances, "s");

        MemberImpl member = getNodeEngineImpl(instances[0]).getLocalMember();
        IScheduledFuture<Double> future = executorService.scheduleOnMember(new PlainCallableTask(),
                member, delay, TimeUnit.SECONDS);

        assertEquals(true, future.getHandler().isAssignedToMember());
        assertEquals(25.0, future.get(), 0);
    }

    @Test
    public void scheduleOnMemberWithRepetition()
            throws InterruptedException {

        HazelcastInstance[] instances = createClusterWithCount(4);
        IScheduledExecutorService s = getScheduledExecutor(instances, "s");

        ICountDownLatch latch = instances[0].getCountDownLatch("latch");
        latch.trySetCount(4);

        Map<Member, IScheduledFuture<?>> futures = s
                .scheduleOnAllMembersAtFixedRate(new ICountdownLatchRunnableTask("latch", instances[0]),
                        0, 3, TimeUnit.SECONDS);

        latch.await(10, TimeUnit.SECONDS);

        assertEquals(0, latch.getCount());
        assertEquals(4, futures.size());
    }

    @Test
    public void scheduleOnKeyOwner_withNotPeriodicRunable()
            throws ExecutionException, InterruptedException {

        HazelcastInstance[] instances = createClusterWithCount(2);

        String key = generateKeyOwnedBy(instances[0]);
        IScheduledExecutorService s = getScheduledExecutor(instances, "s");

        ICountDownLatch latch = instances[0].getCountDownLatch("latch");
        latch.trySetCount(1);

        s.scheduleOnKeyOwner(new ICountdownLatchRunnableTask("latch", instances[0]),
                key, 2, TimeUnit.SECONDS).get();
        assertEquals(0, latch.getCount());

    }

    @Test
    public void scheduleOnKeyOwner_withNotPeriodicRunableDurable()
            throws ExecutionException, InterruptedException {

        HazelcastInstance[] instances = createClusterWithCount(2);

        String key = generateKeyOwnedBy(instances[1]);
        IScheduledExecutorService s = getScheduledExecutor(instances, "s");

        ICountDownLatch latch = instances[0].getCountDownLatch("latch");
        latch.trySetCount(1);

        IScheduledFuture future = s.scheduleOnKeyOwner(
                new ICountdownLatchRunnableTask("latch", instances[0]), key, 2, TimeUnit.SECONDS);

        instances[1].getLifecycleService().shutdown();
        future.get();
        assertEquals(0, latch.getCount());

    }

    @Test
    public void scheduleOnKeyOwner_withCallable()
            throws ExecutionException, InterruptedException {
        int delay = 1;
        String key = "TestKey";

        HazelcastInstance[] instances = createClusterWithCount(2);
        IScheduledExecutorService executorService = getScheduledExecutor(instances, "s");
        Callable<Double> task = new PlainPartitionAwareCallableTask();
        IScheduledFuture<Double> first = executorService.scheduleOnKeyOwner(
                task, key, delay, TimeUnit.SECONDS);


        ScheduledTaskHandler handler = first.getHandler();
        int expectedPartition = instances[0].getPartitionService()
                                            .getPartition(key)
                                            .getPartitionId();
        assertEquals(expectedPartition, handler.getPartitionId());
        assertEquals(25, first.get(), 0);
    }

    @Test
    public void scheduleOnKeyOwnerWithRepetition()
            throws InterruptedException {
        String key = "TestKey";

        HazelcastInstance[] instances = createClusterWithCount(2);
        IScheduledExecutorService executorService = getScheduledExecutor(instances, "s");

        ICountDownLatch latch = instances[0].getCountDownLatch("latch");
        latch.trySetCount(5);

        IScheduledFuture future = executorService.scheduleOnKeyOwnerAtFixedRate(
                new ICountdownLatchRunnableTask("latch", instances[0]), key,
                0, 1, TimeUnit.SECONDS);

        ScheduledTaskHandler handler = future.getHandler();
        int expectedPartition = instances[0].getPartitionService()
                                            .getPartition(key)
                                            .getPartitionId();

        assertEquals(expectedPartition, handler.getPartitionId());

        latch.await(10, TimeUnit.SECONDS);
        assertEquals(0, latch.getCount());
    }

    @Test
    public void getScheduled() {
        int delay = 1;
        String taskName = "Test";

        HazelcastInstance[] instances = createClusterWithCount(2);
        IScheduledExecutorService executorService = getScheduledExecutor(instances, "s");
        IScheduledFuture<Double> first = executorService.schedule(
                named(taskName, new PlainCallableTask()), delay, TimeUnit.SECONDS);

        ScheduledTaskHandler handler = first.getHandler();
        IScheduledFuture<Double> copy = executorService.getScheduledFuture(handler);

        assertEquals(first, copy);
    }

    @Test
    public void getAllScheduled()
            throws ExecutionException, InterruptedException {

        HazelcastInstance[] instances = createClusterWithCount(3);
        IScheduledExecutorService s = getScheduledExecutor(instances, "s");
        s.scheduleOnAllMembers(new PlainCallableTask(), 0, TimeUnit.SECONDS);

        Set<Member> members = instances[0].getCluster().getMembers();
        Map<Member, List<IScheduledFuture<Double>>> allScheduled = s.getAllScheduledFutures();

        assertEquals(members.size(), allScheduled.size());

        for (Member member : members) {
            assertEquals(1, allScheduled.get(member).size());
            assertEquals(25.0, allScheduled.get(member).get(0).get(), 0);
        }
    }

    public IScheduledExecutorService getScheduledExecutor(HazelcastInstance[] instances, String name) {
        return instances[0].getScheduledExecutorService(name);
    }

    public HazelcastInstance[] createClusterWithCount(int count) {
        return createClusterWithCount(count, new Config());
    }

    public HazelcastInstance[] createClusterWithCount(int count, Config config) {
        factory = createHazelcastInstanceFactory();
        return factory.newInstances(config, count);
    }

    static class StatefullRunnableTask
            implements Runnable, Serializable,
                       HazelcastInstanceAware, StatefulTask<String, Integer> {

        final String latchName;

        final String runCounterName;

        final String loadCounterName;

        int status = 0;

        transient HazelcastInstance instance;

        StatefullRunnableTask(String runsCountLatchName, String runCounterName, String loadCounterName, HazelcastInstance instance) {
            this.latchName = runsCountLatchName;
            this.runCounterName = runCounterName;
            this.loadCounterName = loadCounterName;
            this.instance = instance;
        }

        @Override
        public void run() {
            status++;
            instance.getAtomicLong(runCounterName).set(status);
            instance.getCountDownLatch(latchName).countDown();
        }

        @Override
        public void load(Map<String, Integer> snapshot) {
            status = snapshot.get("status");
            instance.getAtomicLong(loadCounterName).incrementAndGet();
        }

        @Override
        public void save(Map<String, Integer> snapshot) {
            snapshot.put("status", status);
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            this.instance = hazelcastInstance;
        }
    }

    static class ICountdownLatchCallableTask
            implements Callable<Double>, Serializable, HazelcastInstanceAware {

        final String runLatchName;

        final int sleepPeriod;

        transient HazelcastInstance instance;

        ICountdownLatchCallableTask(String runLatchName,
                                    int sleepPeriod, HazelcastInstance instance) {
            this.runLatchName = runLatchName;
            this.instance = instance;
            this.sleepPeriod = sleepPeriod;
        }

        @Override
        public Double call() {
            try {
                Thread.sleep(sleepPeriod);
            } catch (InterruptedException e) {
                Thread.interrupted();
            }

            instance.getCountDownLatch(runLatchName).countDown();
            return 77 * 2.2;
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            this.instance = hazelcastInstance;
        }
    }

    static class ICountdownLatchMapIncrementCallableTask
            implements Runnable, Serializable, HazelcastInstanceAware {

        final String runLatchName;

        final String runEntryCounterName;

        final String mapName;

        transient HazelcastInstance instance;

        ICountdownLatchMapIncrementCallableTask(String mapName, String runEntryCounterName,
                                                String runLatchName, HazelcastInstance instance) {
            this.mapName = mapName;
            this.runEntryCounterName = runEntryCounterName;
            this.runLatchName = runLatchName;
            this.instance = instance;
        }

        @Override
        public void run() {
            instance.getAtomicLong(runEntryCounterName).incrementAndGet();

            IMap<String, Integer> map = instance.getMap(mapName);
            for (int i = 0; i < 100000; i++) {
                if (map.get(String.valueOf(i)) == i) {
                    map.put(String.valueOf(i), i + 1);
                }
            }

            instance.getCountDownLatch(runLatchName).countDown();
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            this.instance = hazelcastInstance;
        }
    }

    static class ICountdownLatchRunnableTask implements Runnable, Serializable, HazelcastInstanceAware {

        final String runsCountlatchName;

        transient HazelcastInstance instance;

        ICountdownLatchRunnableTask(String runsCountlatchName, HazelcastInstance instance) {
            this.runsCountlatchName = runsCountlatchName;
            this.instance = instance;
        }

        @Override
        public void run() {
            instance.getCountDownLatch(runsCountlatchName).countDown();
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            this.instance = hazelcastInstance;
        }
    }

    static class PlainCallableTask implements Callable<Double>, Serializable {

        @Override
        public Double call()
                throws Exception {
            return 5 * 5.0;
        }

    }

    static class PlainPartitionAwareCallableTask implements Callable<Double>, Serializable, PartitionAware<String> {

        @Override
        public Double call()
                throws Exception {
            return 5 * 5.0;
        }

        @Override
        public String getPartitionKey() {
            return "TestKey";
        }
    }

}
