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
 */

package com.hazelcast.scheduledexecutor;

import com.hazelcast.config.Config;
import com.hazelcast.config.ScheduledExecutorConfig;
import com.hazelcast.core.HazelcastInstance;
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
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.RootCauseMatcher;
import com.hazelcast.util.executor.ManagedExecutorService;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.scheduledexecutor.TaskUtils.named;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ScheduledExecutorServiceBasicTest extends ScheduledExecutorServiceTestSupport {

    @Rule
    public ExpectedException expected = ExpectedException.none();

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
                .schedule(new PlainCallableTask(), 0, SECONDS);

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
    public void capacity_whenNoLimit()
            throws ExecutionException, InterruptedException {

        String schedulerName = "foobar";

        ScheduledExecutorConfig sec = new ScheduledExecutorConfig()
                .setName(schedulerName)
                .setDurability(1)
                .setPoolSize(1)
                .setCapacity(0);

        Config config = new Config().addScheduledExecutorConfig(sec);

        HazelcastInstance[] instances = createClusterWithCount(1, config);
        IScheduledExecutorService service = instances[0].getScheduledExecutorService(schedulerName);
        String keyOwner = "hitSamePartitionToCheckCapacity";

        for (int i = 0; i < 101; i++) {
            service.scheduleOnKeyOwner(new PlainCallableTask(), keyOwner, 0, TimeUnit.SECONDS);
        }
    }

    @Test
    public void capacity_whenDefault()
            throws ExecutionException, InterruptedException {

        String schedulerName = "foobar";

        HazelcastInstance[] instances = createClusterWithCount(1, null);
        IScheduledExecutorService service = instances[0].getScheduledExecutorService(schedulerName);
        String keyOwner = "hitSamePartitionToCheckCapacity";

        for (int i = 0; i < 100; i++) {
            service.scheduleOnKeyOwner(new PlainCallableTask(), keyOwner, 0, TimeUnit.SECONDS);
        }

        try {
            service.scheduleOnKeyOwner(new PlainCallableTask(), keyOwner, 0, TimeUnit.SECONDS);
            fail("Should have been rejected.");
        } catch (RejectedExecutionException ex) {
            assertTrue("Got wrong RejectedExecutionException",
                    ex.getMessage().equals("Maximum capacity of tasks reached."));
        }
    }

    @Test
    public void capacity_whenPositiveLimit()
            throws ExecutionException, InterruptedException {

        String schedulerName = "foobar";

        ScheduledExecutorConfig sec = new ScheduledExecutorConfig()
                .setName(schedulerName)
                .setDurability(1)
                .setPoolSize(1)
                .setCapacity(10);

        Config config = new Config().addScheduledExecutorConfig(sec);

        HazelcastInstance[] instances = createClusterWithCount(1, config);
        IScheduledExecutorService service = instances[0].getScheduledExecutorService(schedulerName);
        String keyOwner = "hitSamePartitionToCheckCapacity";

        for (int i = 0; i < 10; i++) {
            service.scheduleOnKeyOwner(new PlainCallableTask(), keyOwner, 0, TimeUnit.SECONDS);
        }

        try {
            service.scheduleOnKeyOwner(new PlainCallableTask(), keyOwner, 0, TimeUnit.SECONDS);
            fail("Should have been rejected.");
        } catch (RejectedExecutionException ex) {
            assertTrue("Got wrong RejectedExecutionException",
                    ex.getMessage().equals("Maximum capacity of tasks reached."));
        }
    }

    @Test
    public void capacity_onMember_whenPositiveLimit()
            throws ExecutionException, InterruptedException {

        String schedulerName = "foobar";

        ScheduledExecutorConfig sec = new ScheduledExecutorConfig()
                .setName(schedulerName)
                .setDurability(1)
                .setPoolSize(1)
                .setCapacity(10);

        Config config = new Config().addScheduledExecutorConfig(sec);

        HazelcastInstance[] instances = createClusterWithCount(1, config);
        IScheduledExecutorService service = instances[0].getScheduledExecutorService(schedulerName);
        Member member = instances[0].getCluster().getLocalMember();

        for (int i = 0; i < 10; i++) {
            service.scheduleOnMember(new PlainCallableTask(), member, 0, TimeUnit.SECONDS);
        }

        try {
            service.scheduleOnMember(new PlainCallableTask(), member, 0, TimeUnit.SECONDS);
            fail("Should have been rejected.");
        } catch (RejectedExecutionException ex) {
            assertTrue("Got wrong RejectedExecutionException",
                    ex.getMessage().equals("Maximum capacity of tasks reached."));
        }
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
                named(taskName, new PlainCallableTask()), delay, SECONDS);

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
                named(taskName, new ICountdownLatchRunnableTask("latch")), delay, SECONDS);


        latch.await(10, SECONDS);

        ScheduledTaskHandler handler = future.getHandler();
        assertEquals(schedulerName, handler.getSchedulerName());
        assertEquals(taskName, handler.getTaskName());
    }

    @Test
    public void stats()
            throws ExecutionException, InterruptedException {
        double delay = 2.0;

        HazelcastInstance[] instances = createClusterWithCount(2);
        Object key = generateKeyOwnedBy(instances[1]);
        IScheduledExecutorService executorService = getScheduledExecutor(instances, "s");
        IScheduledFuture<Double> future = executorService.scheduleOnKeyOwner(
                new PlainCallableTask(), key, (int) delay, SECONDS);

        future.get();
        ScheduledTaskStatistics stats = future.getStats();

        assertEquals(1, stats.getTotalRuns());
        assertNotNull(stats.getLastIdleTime(SECONDS));
        assertNotNull(stats.getLastRunDuration(SECONDS));
        assertNotNull(stats.getTotalIdleTime(SECONDS));
        assertNotNull(stats.getTotalRunTime(SECONDS));
        assertNotNull(stats.getTotalRuns());
    }

    @Test
    public void stats_whenMemberOwned()
            throws ExecutionException, InterruptedException {
        double delay = 2.0;

        HazelcastInstance[] instances = createClusterWithCount(2);
        IScheduledExecutorService executorService = getScheduledExecutor(instances, "s");
        IScheduledFuture<Double> future = executorService.scheduleOnMember(
                new PlainCallableTask(), instances[0].getCluster().getLocalMember(), (int) delay, SECONDS);

        future.get();
        ScheduledTaskStatistics stats = future.getStats();

        assertEquals(1, stats.getTotalRuns());
        assertNotNull(stats.getLastIdleTime(SECONDS));
        assertNotNull(stats.getLastRunDuration(SECONDS));
        assertNotNull(stats.getTotalIdleTime(SECONDS));
        assertNotNull(stats.getTotalRunTime(SECONDS));
        assertNotNull(stats.getTotalRuns());
    }

    @Test
    public void scheduleAndGet_withCallable()
            throws ExecutionException, InterruptedException {

        int delay = 5;
        double expectedResult = 25.0;

        HazelcastInstance[] instances = createClusterWithCount(2);
        IScheduledExecutorService executorService = getScheduledExecutor(instances, "s");
        IScheduledFuture<Double> future = executorService.schedule(
                new PlainCallableTask(), delay, SECONDS);

        double result = future.get();

        assertEquals(expectedResult, result, 0);
        assertEquals(true, future.isDone());
        assertEquals(false, future.isCancelled());
    }

    @Test
    public void scheduleAndGet_withCallable_durableAfterTaskCompletion()
            throws ExecutionException, InterruptedException {

        int delay = 5;
        double expectedResult = 25.0;

        HazelcastInstance[] instances = createClusterWithCount(2);
        String key = generateKeyOwnedBy(instances[1]);

        IScheduledExecutorService executorService = getScheduledExecutor(instances, "s");
        IScheduledFuture<Double> future = executorService.scheduleOnKeyOwner(
                new PlainCallableTask(), key, delay, SECONDS);

        double resultFromOriginalTask = future.get();

        instances[1].getLifecycleService().shutdown();

        double resultFromMigratedTask = future.get();

        assertEquals(expectedResult, resultFromOriginalTask, 0);
        assertEquals(expectedResult, resultFromMigratedTask, 0);
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
                        "runsCountLatchName"), key, delay, SECONDS);

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
                new ICountdownLatchCallableTask("runsCountLatchName", 15000), delay, SECONDS);

        Thread.sleep(4000);
        future.cancel(false);

        runsCountLatch.await(15, SECONDS);

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
                new PlainCallableTask(), delay, SECONDS);

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
                named(taskName, new PlainCallableTask()), delay, SECONDS);

        executorService.schedule(
                named(taskName, new PlainCallableTask()), delay, SECONDS);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void schedule_thenCancelInterrupted()
            throws ExecutionException, InterruptedException {
        int delay = 1;
        String taskName = "Test";

        HazelcastInstance[] instances = createClusterWithCount(2);
        IScheduledExecutorService executorService = getScheduledExecutor(instances, "s");
        IScheduledFuture<Double> first = executorService.schedule(
                named(taskName, new PlainCallableTask()), delay, TimeUnit.MINUTES);

        first.cancel(true);
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

    @Test(expected = TimeoutException.class)
    public void schedule_thenGetWithTimeout()
            throws ExecutionException, InterruptedException, TimeoutException {
        int delay = 5;
        String taskName = "Test";

        HazelcastInstance[] instances = createClusterWithCount(2);
        IScheduledExecutorService executorService = getScheduledExecutor(instances, "s");
        IScheduledFuture<Double> first = executorService.schedule(
                named(taskName, new PlainCallableTask()), delay, TimeUnit.MINUTES);

        first.get(2, TimeUnit.SECONDS);
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
    public void scheduleOnKeyOwner_getDelay()
            throws ExecutionException, InterruptedException {
        int delay = 20;
        String taskName = "Test";

        HazelcastInstance[] instances = createClusterWithCount(2);
        Object key = generateKeyOwnedBy(instances[1]);

        IScheduledExecutorService executorService = getScheduledExecutor(instances, "s");
        IScheduledFuture<Double> first = executorService.scheduleOnKeyOwner(
                named(taskName, new PlainCallableTask()), key, delay, TimeUnit.MINUTES);

        assertEquals(19, first.getDelay(TimeUnit.MINUTES));
    }

    @Test()
    public void scheduleOnMember_getDelay()
            throws ExecutionException, InterruptedException {
        int delay = 20;
        String taskName = "Test";

        HazelcastInstance[] instances = createClusterWithCount(2);

        IScheduledExecutorService executorService = getScheduledExecutor(instances, "s");
        IScheduledFuture<Double> first = executorService.scheduleOnMember(
                named(taskName, new PlainCallableTask()), instances[0].getCluster().getLocalMember(), delay, TimeUnit.MINUTES);

        assertEquals(19, first.getDelay(TimeUnit.MINUTES));
    }

    @Test()
    public void schedule_andCancel()
            throws ExecutionException, InterruptedException {

        HazelcastInstance[] instances = createClusterWithCount(2);

        ICountDownLatch latch = instances[0].getCountDownLatch("latch");
        latch.trySetCount(1);

        IScheduledExecutorService executorService = getScheduledExecutor(instances, "s");
        IScheduledFuture future = executorService.scheduleAtFixedRate(
                new ICountdownLatchRunnableTask("latch"), 1, 1, SECONDS);


        Thread.sleep(5000);

        assertFalse(future.isCancelled());
        assertFalse(future.isDone());

        future.cancel(false);

        assertTrue(future.isCancelled());
        assertTrue(future.isDone());
    }

    @Test()
    public void schedule_andCancel_onMember()
            throws ExecutionException, InterruptedException {

        HazelcastInstance[] instances = createClusterWithCount(2);

        ICountDownLatch latch = instances[0].getCountDownLatch("latch");
        latch.trySetCount(1);

        IScheduledExecutorService executorService = getScheduledExecutor(instances, "s");
        IScheduledFuture future = executorService.scheduleOnMemberAtFixedRate(
                new ICountdownLatchRunnableTask("latch"), instances[0].getCluster().getLocalMember(),
                1, 1, SECONDS);

        Thread.sleep(5000);

        assertFalse(future.isCancelled());
        assertFalse(future.isDone());

        future.cancel(false);

        assertTrue(future.isCancelled());
        assertTrue(future.isDone());
    }

    @Test()
    public void cancelledAndDone_durable()
            throws ExecutionException, InterruptedException {

        HazelcastInstance[] instances = createClusterWithCount(3);
        Object key = generateKeyOwnedBy(instances[1]);

        ICountDownLatch latch = instances[0].getCountDownLatch("latch");
        latch.trySetCount(1);

        IScheduledExecutorService executorService = getScheduledExecutor(instances, "s");
        IScheduledFuture future = executorService.scheduleOnKeyOwnerAtFixedRate(
                new ICountdownLatchRunnableTask("latch"), key, 0, 1, SECONDS);


        latch.await(10, SECONDS);

        assertFalse(future.isCancelled());
        assertFalse(future.isDone());

        future.cancel(false);

        assertTrue(future.isCancelled());
        assertTrue(future.isDone());

        instances[1].getLifecycleService().shutdown();

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
                named(taskName, new PlainCallableTask()), delay, SECONDS);

        first.dispose();
        first.get();
    }

    @Test(expected = StaleTaskException.class)
    public void schedule_thenDisposeThenGet_onMember()
            throws ExecutionException, InterruptedException {
        int delay = 1;
        String taskName = "Test";

        HazelcastInstance[] instances = createClusterWithCount(2);
        IScheduledExecutorService executorService = getScheduledExecutor(instances, "s");
        IScheduledFuture<Double> first = executorService.scheduleOnMember(
                named(taskName, new PlainCallableTask()), instances[0].getCluster().getLocalMember(), delay, SECONDS);

        first.dispose();
        first.get();
    }

    @Test(expected = RejectedExecutionException.class)
    public void schedule_whenShutdown()
            throws ExecutionException, InterruptedException {
        int delay = 1;

        HazelcastInstance[] instances = createClusterWithCount(2);
        IScheduledExecutorService executorService = getScheduledExecutor(instances, "s");
        executorService.schedule(new PlainCallableTask(), delay, SECONDS);
        executorService.shutdown();

        executorService.schedule(new PlainCallableTask(), delay, SECONDS);
    }

    @Test()
    public void schedule_whenPartitionLost()
            throws ExecutionException, InterruptedException {
        int delay = 1;

        HazelcastInstance[] instances = createClusterWithCount(2);
        IScheduledExecutorService executorService = getScheduledExecutor(instances, "s");
        final IScheduledFuture future = executorService.schedule(new PlainCallableTask(), delay, SECONDS);
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

    @Test
    public void schedule_getHandlerDisposeThenRecreateFutureAndGet()
            throws ExecutionException, InterruptedException {
        int delay = 1;
        String taskName = "Test";

        HazelcastInstance[] instances = createClusterWithCount(2);
        IScheduledExecutorService executorService = getScheduledExecutor(instances, "s");
        IScheduledFuture<Double> first = executorService.schedule(
                named(taskName, new PlainCallableTask()), delay, SECONDS);

        ScheduledTaskHandler handler = first.getHandler();
        first.dispose();

        expected.expect(ExecutionException.class);
        expected.expectCause(new RootCauseMatcher(StaleTaskException.class));
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
                task, delay, SECONDS);


        ScheduledTaskHandler handler = first.getHandler();
        int expectedPartition = getPartitionIdFromPartitionAwareTask(instances[0], (PartitionAware) task);

        assertEquals(expectedPartition, handler.getPartitionId());
    }

    @Test()
    public void schedule_partitionAware_runnable()
            throws ExecutionException, InterruptedException {
        int delay = 1;
        String completionLatchName = "completionLatch";

        HazelcastInstance[] instances = createClusterWithCount(2);
        ICountDownLatch completionLatch = instances[0].getCountDownLatch(completionLatchName);
        completionLatch.trySetCount(1);

        IScheduledExecutorService executorService = getScheduledExecutor(instances, "s");
        Runnable task = new PlainPartitionAwareRunnableTask(completionLatchName);
        IScheduledFuture first = executorService.schedule(
                task, delay, SECONDS);

        completionLatch.await(10, SECONDS);

        ScheduledTaskHandler handler = first.getHandler();
        int expectedPartition = getPartitionIdFromPartitionAwareTask(instances[0], (PartitionAware) task);
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
                new StatefulRunnableTask("latch", "runC", "loadC"), 2, SECONDS);

        latch.await(10, SECONDS);
    }

    @Test
    public void scheduleWithRepetition()
            throws ExecutionException, InterruptedException {
        HazelcastInstance[] instances = createClusterWithCount(2);

        IScheduledExecutorService s = getScheduledExecutor(instances, "s");

        ICountDownLatch latch = instances[0].getCountDownLatch("latch");
        latch.trySetCount(3);

        IScheduledFuture future = s.scheduleAtFixedRate(new ICountdownLatchRunnableTask("latch"),
                0, 1, SECONDS);

        latch.await(10, SECONDS);
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
                member, delay, SECONDS);

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
                .scheduleOnAllMembersAtFixedRate(new ICountdownLatchRunnableTask("latch"), 0, 3, SECONDS);

        latch.await(10, SECONDS);

        assertEquals(0, latch.getCount());
        assertEquals(4, futures.size());
    }

    @Test
    public void scheduleOnKeyOwner_thenGet()
            throws InterruptedException, ExecutionException {

        HazelcastInstance[] instances = createClusterWithCount(2);
        IScheduledExecutorService executorService = getScheduledExecutor(instances, "s");
        String key = generateKeyOwnedBy(instances[1]);

        IScheduledFuture<Double> future = executorService.scheduleOnKeyOwner(
                new PlainCallableTask(), key, 2, SECONDS);

        assertEquals(25.0, future.get(), 0.0);
    }

    @Test
    public void scheduleOnKeyOwner_withNotPeriodicRunable()
            throws ExecutionException, InterruptedException {

        HazelcastInstance[] instances = createClusterWithCount(2);

        String key = generateKeyOwnedBy(instances[0]);
        IScheduledExecutorService s = getScheduledExecutor(instances, "s");

        ICountDownLatch latch = instances[0].getCountDownLatch("latch");
        latch.trySetCount(1);

        s.scheduleOnKeyOwner(new ICountdownLatchRunnableTask("latch"),
                key, 2, SECONDS).get();
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

        IScheduledFuture future = s.scheduleOnKeyOwner(new ICountdownLatchRunnableTask("latch"), key, 2, SECONDS);

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
                task, key, delay, SECONDS);


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
                new ICountdownLatchRunnableTask("latch"), key,
                0, 1, SECONDS);

        ScheduledTaskHandler handler = future.getHandler();
        int expectedPartition = instances[0].getPartitionService()
                .getPartition(key)
                .getPartitionId();

        assertEquals(expectedPartition, handler.getPartitionId());

        latch.await(10, SECONDS);
        assertEquals(0, latch.getCount());
    }

    @Test
    public void getScheduled() {
        int delay = 1;
        String taskName = "Test";

        HazelcastInstance[] instances = createClusterWithCount(2);
        IScheduledExecutorService executorService = getScheduledExecutor(instances, "s");
        IScheduledFuture<Double> first = executorService.schedule(
                named(taskName, new PlainCallableTask()), delay, SECONDS);

        ScheduledTaskHandler handler = first.getHandler();
        IScheduledFuture<Double> copy = executorService.getScheduledFuture(handler);

        assertEquals(first, copy);
    }

    @Test
    public void scheduleOnAllMembers_getAllScheduled()
            throws ExecutionException, InterruptedException {

        HazelcastInstance[] instances = createClusterWithCount(3);
        IScheduledExecutorService s = getScheduledExecutor(instances, "s");
        s.scheduleOnAllMembers(new PlainCallableTask(), 0, SECONDS);

        Set<Member> members = instances[0].getCluster().getMembers();
        Map<Member, List<IScheduledFuture<Double>>> allScheduled = s.getAllScheduledFutures();

        assertEquals(members.size(), allScheduled.size());

        for (Member member : members) {
            assertEquals(1, allScheduled.get(member).size());
            assertEquals(25.0, allScheduled.get(member).get(0).get(), 0);
        }
    }

    @Test
    public void scheduleRandomPartitions_getAllScheduled()
            throws ExecutionException, InterruptedException {

        HazelcastInstance[] instances = createClusterWithCount(2);
        IScheduledExecutorService s = getScheduledExecutor(instances, "s");

        int expectedTotal = 11;
        IScheduledFuture[] futures = new IScheduledFuture[expectedTotal];
        for (int i = 0; i < expectedTotal; i++) {
            futures[i] = s.schedule(new PlainCallableTask(i), 0, SECONDS);
        }

        assertEquals(expectedTotal, countScheduledTasksOn(s), 0);

        // Dispose 1 task
        futures[0].dispose();

        // Recount
        assertEquals(expectedTotal - 1, countScheduledTasksOn(s), 0);

        // Verify all tasks
        for (int i = 1; i < expectedTotal; i++) {
            assertEquals(25.0 + i, futures[i].get());
        }
    }

    @Test
    public void getErroneous() throws InterruptedException, ExecutionException {
        int delay = 2;
        String taskName = "Test";
        String completionLatchName = "completionLatch";

        HazelcastInstance[] instances = createClusterWithCount(2);

        String key = generateKeyOwnedBy(instances[1]);
        IScheduledExecutorService executorService = getScheduledExecutor(instances, "s");
        ICountDownLatch latch = instances[1].getCountDownLatch(completionLatchName);
        latch.trySetCount(1);

        IScheduledFuture<Double> future = executorService.scheduleOnKeyOwner(
                named(taskName, new ErroneousCallableTask(completionLatchName)), key, delay, SECONDS);

        latch.await(10, SECONDS);
        expected.expect(ExecutionException.class);
        expected.expectCause(new RootCauseMatcher(IllegalStateException.class, "Erroneous task"));
        future.get();
    }

    @Test
    public void getErroneous_durable() throws InterruptedException, ExecutionException {
        int delay = 2;
        String taskName = "Test";
        String completionLatchName = "completionLatch";

        HazelcastInstance[] instances = createClusterWithCount(2);

        String key = generateKeyOwnedBy(instances[1]);
        IScheduledExecutorService executorService = getScheduledExecutor(instances, "s");
        ICountDownLatch latch = instances[1].getCountDownLatch(completionLatchName);
        latch.trySetCount(1);

        IScheduledFuture<Double> future = executorService.scheduleOnKeyOwner(
                named(taskName, new ErroneousCallableTask(completionLatchName)), key, delay, SECONDS);

        latch.await(10, SECONDS);
        instances[1].getLifecycleService().shutdown();
        Thread.sleep(2000);

        expected.expect(ExecutionException.class);
        expected.expectCause(new RootCauseMatcher(IllegalStateException.class, "Erroneous task"));
        future.get();
    }

}
