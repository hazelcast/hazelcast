/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.config.Config;
import com.hazelcast.config.ScheduledExecutorConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.IAtomicLong;
import com.hazelcast.cp.ICountDownLatch;
import com.hazelcast.internal.partition.PartitionLostEventImpl;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.map.IMap;
import com.hazelcast.partition.PartitionAware;
import com.hazelcast.scheduledexecutor.impl.DistributedScheduledExecutorService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.internal.partition.IPartitionLostEvent;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.internal.util.RootCauseMatcher;
import com.hazelcast.internal.util.executor.ManagedExecutorService;
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
import static com.hazelcast.internal.partition.IPartition.MAX_BACKUP_COUNT;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ScheduledExecutorServiceBasicTest extends ScheduledExecutorServiceTestSupport {


    @Rule
    public ExpectedException expected = ExpectedException.none();

    @Test
    public void config() {
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
    public void exception_suppressesFutureExecutions()
            throws ExecutionException, InterruptedException {
        HazelcastInstance[] instances = createClusterWithCount(2);
        IScheduledExecutorService service = instances[0].getScheduledExecutorService("test");

        final IScheduledFuture f = service.scheduleAtFixedRate(
                new ErroneousRunnableTask(), 1, 1, TimeUnit.SECONDS);

        assertTrueEventually(() -> assertTrue(f.isDone()));

        assertEquals(1L, f.getStats().getTotalRuns());
        expected.expect(ExecutionException.class);
        expected.expectCause(new RootCauseMatcher(IllegalStateException.class, "Erroneous task"));

        f.get();
    }

    @Test
    public void capacity_whenNoLimit() {
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
    public void capacity_whenDefault() {
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
            assertEquals("Got wrong RejectedExecutionException",
                    "Maximum capacity (100) of tasks reached, for scheduled executor (foobar). "
                            + "Reminder that tasks must be disposed if not needed.", ex.getMessage());
        }
    }

    @Test
    public void capacity_whenPositiveLimit() {
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
            assertEquals("Got wrong RejectedExecutionException",
                    "Maximum capacity (10) of tasks reached, for scheduled executor (foobar). "
                            + "Reminder that tasks must be disposed if not needed.", ex.getMessage());
        }
    }

    @Test
    public void capacity_onMember_whenPositiveLimit() {
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
            assertEquals("Got wrong RejectedExecutionException",
                    "Maximum capacity (10) of tasks reached, for scheduled executor (foobar). "
                            + "Reminder that tasks must be disposed if not needed.", ex.getMessage());
        }
    }

    @Test
    public void handlerTaskAndSchedulerNames_withCallable() throws Exception {
        int delay = 0;
        String schedulerName = "s";
        String taskName = "TestCallable";

        HazelcastInstance[] instances = createClusterWithCount(2);
        IScheduledExecutorService executorService = instances[0].getScheduledExecutorService(schedulerName);
        IScheduledFuture<Double> future = executorService.schedule(named(taskName, new PlainCallableTask()), delay, SECONDS);

        future.get();

        ScheduledTaskHandler handler = future.getHandler();
        assertEquals(schedulerName, handler.getSchedulerName());
        assertEquals(taskName, handler.getTaskName());
    }

    @Test
    public void handlerTaskAndSchedulerNames_withRunnable() {
        int delay = 0;
        String schedulerName = "s";
        String taskName = "TestRunnable";

        HazelcastInstance[] instances = createClusterWithCount(2);

        ICountDownLatch latch = instances[0].getCPSubsystem().getCountDownLatch("latch");
        latch.trySetCount(1);

        IScheduledExecutorService executorService = instances[0].getScheduledExecutorService(schedulerName);
        IScheduledFuture future = executorService.schedule(
                named(taskName, new ICountdownLatchRunnableTask("latch")), delay, SECONDS);

        assertOpenEventually(latch);

        ScheduledTaskHandler handler = future.getHandler();
        assertEquals(schedulerName, handler.getSchedulerName());
        assertEquals(taskName, handler.getTaskName());
    }

    @Test
    public void stats() throws Exception {
        double delay = 2.0;

        HazelcastInstance[] instances = createClusterWithCount(2);
        Object key = generateKeyOwnedBy(instances[1]);
        IScheduledExecutorService executorService = getScheduledExecutor(instances, "s");
        IScheduledFuture<Double> future = executorService.scheduleOnKeyOwner(new PlainCallableTask(), key, (int) delay, SECONDS);

        future.get();
        ScheduledTaskStatistics stats = future.getStats();

        assertEquals(1, stats.getTotalRuns());
        assertEquals(0, stats.getLastRunDuration(SECONDS));
        assertEquals(0, stats.getTotalRunTime(SECONDS));
        assertNotEquals(0, stats.getLastIdleTime(SECONDS));
        assertNotEquals(0, stats.getTotalIdleTime(SECONDS));
    }

    @Test
    public void stats_whenMemberOwned() throws Exception {
        double delay = 2.0;

        HazelcastInstance[] instances = createClusterWithCount(2);
        Member localMember = instances[0].getCluster().getLocalMember();
        IScheduledExecutorService executorService = getScheduledExecutor(instances, "s");
        IScheduledFuture<Double> future = executorService.scheduleOnMember(
                new PlainCallableTask(), localMember, (int) delay, SECONDS);

        future.get();
        ScheduledTaskStatistics stats = future.getStats();

        assertEquals(1, stats.getTotalRuns());
        assertEquals(0, stats.getLastRunDuration(SECONDS));
        assertEquals(0, stats.getTotalRunTime(SECONDS));
        assertNotEquals(0, stats.getLastIdleTime(SECONDS));
        assertNotEquals(0, stats.getTotalIdleTime(SECONDS));
    }

    @Test
    public void scheduleAndGet_withCallable() throws Exception {
        int delay = 5;
        double expectedResult = 25.0;

        HazelcastInstance[] instances = createClusterWithCount(2);
        IScheduledExecutorService executorService = getScheduledExecutor(instances, "s");
        IScheduledFuture<Double> future = executorService.schedule(new PlainCallableTask(), delay, SECONDS);

        double result = future.get();

        assertEquals(expectedResult, result, 0);
        assertTrue(future.isDone());
        assertFalse(future.isCancelled());
    }

    @Test
    public void scheduleAndGet_withCallable_durableAfterTaskCompletion() throws Exception {
        int delay = 5;
        double expectedResult = 25.0;

        HazelcastInstance[] instances = createClusterWithCount(2);
        String key = generateKeyOwnedBy(instances[1]);

        IScheduledExecutorService executorService = getScheduledExecutor(instances, "s");
        IScheduledFuture<Double> future = executorService.scheduleOnKeyOwner(new PlainCallableTask(), key, delay, SECONDS);

        double resultFromOriginalTask = future.get();

        instances[1].getLifecycleService().shutdown();

        double resultFromMigratedTask = future.get();

        assertEquals(expectedResult, resultFromOriginalTask, 0);
        assertEquals(expectedResult, resultFromMigratedTask, 0);
        assertTrue(future.isDone());
        assertFalse(future.isCancelled());
    }

    @Test
    public void schedule_withMapChanges_durable() {
        HazelcastInstance[] instances = createClusterWithCount(2);
        IMap<String, Integer> map = instances[1].getMap("map");
        for (int i = 0; i < MAP_INCREMENT_TASK_MAX_ENTRIES; i++) {
            map.put(String.valueOf(i), i);
        }

        Object key = generateKeyOwnedBy(instances[0]);
        ICountDownLatch startedLatch = instances[1].getCPSubsystem().getCountDownLatch("startedLatch");
        ICountDownLatch finishedLatch = instances[1].getCPSubsystem().getCountDownLatch("finishedLatch");
        startedLatch.trySetCount(1);
        finishedLatch.trySetCount(1);

        IAtomicLong runEntryCounter = instances[1].getCPSubsystem().getAtomicLong("runEntryCounterName");

        IScheduledExecutorService executorService = getScheduledExecutor(instances, "s");
        executorService.scheduleOnKeyOwner(new ICountdownLatchMapIncrementCallableTask("map",
                "runEntryCounterName", "startedLatch", "finishedLatch"), key, 0, SECONDS);

        assertOpenEventually(startedLatch);
        instances[0].getLifecycleService().shutdown();

        assertOpenEventually(finishedLatch);

        for (int i = 0; i < 10000; i++) {
            assertEquals(i + 1, (int) map.get(String.valueOf(i)));
        }

        assertEquals(2, runEntryCounter.get());
    }

    @Test
    public void schedule_withLongSleepingCallable_cancelledAndGet() {
        int delay = 0;

        HazelcastInstance[] instances = createClusterWithCount(2);
        ICountDownLatch runsCountLatch = instances[0].getCPSubsystem().getCountDownLatch("runsCountLatchName");
        runsCountLatch.trySetCount(1);

        IScheduledExecutorService executorService = getScheduledExecutor(instances, "s");
        IScheduledFuture<Double> future = executorService.schedule(
                new ICountdownLatchCallableTask("runsCountLatchName", 15000), delay, SECONDS);

        sleepSeconds(4);
        future.cancel(false);

        assertOpenEventually(runsCountLatch);

        assertTrue(future.isDone());
        assertTrue(future.isCancelled());
    }

    @Test
    public void schedule_withNegativeDelay() throws Exception {
        int delay = -2;
        double expectedResult = 25.0;

        HazelcastInstance[] instances = createClusterWithCount(2);
        IScheduledExecutorService executorService = getScheduledExecutor(instances, "s");
        IScheduledFuture<Double> future = executorService.schedule(new PlainCallableTask(), delay, SECONDS);

        double result = future.get();

        assertEquals(expectedResult, result, 0);
        assertTrue(future.isDone());
        assertFalse(future.isCancelled());
    }

    @Test(expected = DuplicateTaskException.class)
    public void schedule_duplicate() {
        int delay = 1;
        String taskName = "Test";

        HazelcastInstance[] instances = createClusterWithCount(2);
        IScheduledExecutorService executorService = getScheduledExecutor(instances, "s");
        executorService.schedule(named(taskName, new PlainCallableTask()), delay, SECONDS);

        executorService.schedule(named(taskName, new PlainCallableTask()), delay, SECONDS);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void schedule_thenCancelInterrupted() {
        int delay = 1;
        String taskName = "Test";

        HazelcastInstance[] instances = createClusterWithCount(2);
        IScheduledExecutorService executorService = getScheduledExecutor(instances, "s");
        IScheduledFuture<Double> first = executorService.schedule(named(taskName, new PlainCallableTask()), delay, MINUTES);

        first.cancel(true);
    }

    @Test(expected = CancellationException.class)
    public void schedule_thenCancelAndGet() throws Exception {
        int delay = 1;
        String taskName = "Test";

        HazelcastInstance[] instances = createClusterWithCount(2);
        IScheduledExecutorService executorService = getScheduledExecutor(instances, "s");
        IScheduledFuture<Double> first = executorService.schedule(named(taskName, new PlainCallableTask()), delay, MINUTES);

        first.cancel(false);
        first.get();
    }

    @Test(expected = TimeoutException.class)
    public void schedule_thenGetWithTimeout() throws Exception {
        int delay = 5;
        String taskName = "Test";

        HazelcastInstance[] instances = createClusterWithCount(2);
        IScheduledExecutorService executorService = getScheduledExecutor(instances, "s");
        IScheduledFuture<Double> first = executorService.schedule(named(taskName, new PlainCallableTask()), delay, MINUTES);

        first.get(2, TimeUnit.SECONDS);
    }

    @Test
    public void schedule_getDelay() {
        int delay = 20;
        String taskName = "Test";

        HazelcastInstance[] instances = createClusterWithCount(2);
        IScheduledExecutorService executorService = getScheduledExecutor(instances, "s");
        IScheduledFuture<Double> first = executorService.schedule(named(taskName, new PlainCallableTask()), delay, MINUTES);

        assertEquals(19, first.getDelay(MINUTES));
    }

    @Test
    public void scheduleOnKeyOwner_getDelay() {
        int delay = 20;
        String taskName = "Test";

        HazelcastInstance[] instances = createClusterWithCount(2);
        Object key = generateKeyOwnedBy(instances[1]);

        IScheduledExecutorService executorService = getScheduledExecutor(instances, "s");
        IScheduledFuture<Double> first = executorService.scheduleOnKeyOwner(
                named(taskName, new PlainCallableTask()), key, delay, MINUTES);

        assertEquals(19, first.getDelay(MINUTES));
    }

    @Test
    public void scheduleOnMember_getDelay() {
        int delay = 20;
        String taskName = "Test";

        HazelcastInstance[] instances = createClusterWithCount(2);
        Member localMember = instances[0].getCluster().getLocalMember();

        IScheduledExecutorService executorService = getScheduledExecutor(instances, "s");
        IScheduledFuture<Double> first = executorService.scheduleOnMember(
                named(taskName, new PlainCallableTask()), localMember, delay, MINUTES);

        assertEquals(19, first.getDelay(MINUTES));
    }

    @Test
    public void schedule_andCancel() {
        HazelcastInstance[] instances = createClusterWithCount(2);

        ICountDownLatch latch = instances[0].getCPSubsystem().getCountDownLatch("latch");
        latch.trySetCount(1);

        IScheduledExecutorService executorService = getScheduledExecutor(instances, "s");
        IScheduledFuture future = executorService.scheduleAtFixedRate(new ICountdownLatchRunnableTask("latch"), 1, 1, SECONDS);

        sleepSeconds(5);

        assertFalse(future.isCancelled());
        assertFalse(future.isDone());

        future.cancel(false);

        assertTrue(future.isCancelled());
        assertTrue(future.isDone());
    }

    @Test
    public void schedule_andCancel_onMember() {
        HazelcastInstance[] instances = createClusterWithCount(2);
        Member localMember = instances[0].getCluster().getLocalMember();

        ICountDownLatch latch = instances[0].getCPSubsystem().getCountDownLatch("latch");
        latch.trySetCount(1);

        IScheduledExecutorService executorService = getScheduledExecutor(instances, "s");
        IScheduledFuture future = executorService.scheduleOnMemberAtFixedRate(
                new ICountdownLatchRunnableTask("latch"), localMember, 1, 1, SECONDS);

        sleepSeconds(5);

        assertFalse(future.isCancelled());
        assertFalse(future.isDone());

        future.cancel(false);

        assertTrue(future.isCancelled());
        assertTrue(future.isDone());
    }

    @Test
    public void cancelledAndDone_durable() {
        HazelcastInstance[] instances = createClusterWithCount(3);
        Object key = generateKeyOwnedBy(instances[1]);

        ICountDownLatch latch = instances[0].getCPSubsystem().getCountDownLatch("latch");
        latch.trySetCount(1);

        IScheduledExecutorService executorService = getScheduledExecutor(instances, "s");
        IScheduledFuture future = executorService.scheduleOnKeyOwnerAtFixedRate(
                new ICountdownLatchRunnableTask("latch"), key, 0, 1, SECONDS);

        assertOpenEventually(latch);

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
    public void schedule_compareTo() {
        HazelcastInstance[] instances = createClusterWithCount(2);

        IScheduledExecutorService executorService = getScheduledExecutor(instances, "s");
        IScheduledFuture<Double> first = executorService.schedule(new PlainCallableTask(), 1, MINUTES);
        IScheduledFuture<Double> second = executorService.schedule(new PlainCallableTask(), 2, MINUTES);

        assertEquals(-1, first.compareTo(second));
    }

    @Test(expected = StaleTaskException.class)
    public void schedule_thenDisposeThenGet() throws Exception {
        int delay = 1;
        String taskName = "Test";

        HazelcastInstance[] instances = createClusterWithCount(2);
        IScheduledExecutorService executorService = getScheduledExecutor(instances, "s");
        IScheduledFuture<Double> first = executorService.schedule(named(taskName, new PlainCallableTask()), delay, SECONDS);

        first.dispose();
        first.get();
    }

    @Test(expected = StaleTaskException.class)
    public void schedule_thenDisposeThenGet_onMember() throws Exception {
        int delay = 1;
        String taskName = "Test";

        HazelcastInstance[] instances = createClusterWithCount(2);
        Member localMember = instances[0].getCluster().getLocalMember();
        IScheduledExecutorService executorService = getScheduledExecutor(instances, "s");
        IScheduledFuture<Double> first = executorService.scheduleOnMember(
                named(taskName, new PlainCallableTask()), localMember, delay, SECONDS);

        first.dispose();
        first.get();
    }

    @Test(expected = RejectedExecutionException.class)
    public void schedule_whenShutdown() {
        int delay = 1;

        HazelcastInstance[] instances = createClusterWithCount(2);
        IScheduledExecutorService executorService = getScheduledExecutor(instances, "s");
        executorService.schedule(new PlainCallableTask(), delay, SECONDS);
        executorService.shutdown();

        executorService.schedule(new PlainCallableTask(), delay, SECONDS);
    }

    public void schedule_testPartitionLostEvent(int replicaLostCount) {
        int delay = 1;

        HazelcastInstance[] instances = createClusterWithCount(1);
        IScheduledExecutorService executorService = getScheduledExecutor(instances, "s");
        final IScheduledFuture future = executorService.schedule(new PlainCallableTask(), delay, SECONDS);

        // Used to make sure both futures (on the same handler) get the event.
        // Catching possible equal/hashcode issues in the Map
        final IScheduledFuture futureCopyInstance = (IScheduledFuture) ((List) executorService.getAllScheduledFutures()
                .values().toArray()[0]).get(0);

        ScheduledTaskHandler handler = future.getHandler();

        int partitionOwner = handler.getPartitionId();
        IPartitionLostEvent internalEvent = new PartitionLostEventImpl(partitionOwner, replicaLostCount, null);
        ((InternalPartitionServiceImpl) getNodeEngineImpl(instances[0]).getPartitionService()).onPartitionLost(internalEvent);

        assertTrueEventually(() -> {
            try {
                future.get();
                fail();
            } catch (IllegalStateException ex) {
                try {
                    futureCopyInstance.get();
                    fail();
                } catch (IllegalStateException ex2) {
                    assertEquals(format("Partition %d, holding this scheduled task was lost along with all backups.",
                            future.getHandler().getPartitionId()), ex.getMessage());
                    assertEquals(format("Partition %d, holding this scheduled task was lost along with all backups.",
                            future.getHandler().getPartitionId()), ex2.getMessage());
                }
            }
        });
    }

    @Test
    public void schedule_testPartitionLostEvent_withMaxBackupCount() {
        schedule_testPartitionLostEvent(MAX_BACKUP_COUNT);
    }

    @Test
    public void schedule_testPartitionLostEvent_withDurabilityCount() {
        schedule_testPartitionLostEvent(1);
    }

    @Test
    public void scheduleOnMember_testMemberLostEvent() {
        int delay = 1;

        HazelcastInstance[] instances = createClusterWithCount(2);
        Member member = instances[1].getCluster().getLocalMember();

        IScheduledExecutorService executorService = getScheduledExecutor(instances, "s");
        final IScheduledFuture future = executorService.scheduleOnMember(new PlainCallableTask(), member, delay, SECONDS);

        instances[1].getLifecycleService().terminate();

        assertTrueEventually(() -> {
            try {
                future.get(0, SECONDS);
                fail();
            } catch (IllegalStateException ex) {
                System.err.println(ex.getMessage());
                assertEquals(format("Member with address: %s,  holding this scheduled task is not part of this cluster.",
                        future.getHandler().getAddress()), ex.getMessage());
            } catch (TimeoutException ex) {
                ignore(ex);
            }
        });
    }

    @Test
    public void schedule_getHandlerDisposeThenRecreateFutureAndGet() throws Exception {
        int delay = 1;
        String taskName = "Test";

        HazelcastInstance[] instances = createClusterWithCount(2);
        IScheduledExecutorService executorService = getScheduledExecutor(instances, "s");
        IScheduledFuture<Double> first = executorService.schedule(named(taskName, new PlainCallableTask()), delay, SECONDS);

        ScheduledTaskHandler handler = first.getHandler();
        first.dispose();

        expected.expect(ExecutionException.class);
        expected.expectCause(new RootCauseMatcher(StaleTaskException.class));
        executorService.getScheduledFuture(handler).get();
    }

    @Test
    public void schedule_partitionAware() {
        int delay = 1;

        HazelcastInstance[] instances = createClusterWithCount(2);
        IScheduledExecutorService executorService = getScheduledExecutor(instances, "s");
        Callable<Double> task = new PlainPartitionAwareCallableTask();
        IScheduledFuture<Double> first = executorService.schedule(task, delay, SECONDS);

        ScheduledTaskHandler handler = first.getHandler();
        int expectedPartition = getPartitionIdFromPartitionAwareTask(instances[0], (PartitionAware) task);

        assertEquals(expectedPartition, handler.getPartitionId());
    }

    @Test
    public void schedule_partitionAware_runnable() {
        int delay = 1;
        String completionLatchName = "completionLatch";

        HazelcastInstance[] instances = createClusterWithCount(2);
        ICountDownLatch completionLatch = instances[0].getCPSubsystem().getCountDownLatch(completionLatchName);
        completionLatch.trySetCount(1);

        IScheduledExecutorService executorService = getScheduledExecutor(instances, "s");
        Runnable task = new PlainPartitionAwareRunnableTask(completionLatchName);
        IScheduledFuture first = executorService.schedule(task, delay, SECONDS);

        assertOpenEventually(completionLatch);

        ScheduledTaskHandler handler = first.getHandler();
        int expectedPartition = getPartitionIdFromPartitionAwareTask(instances[0], (PartitionAware) task);
        assertEquals(expectedPartition, handler.getPartitionId());
    }

    @Test
    public void schedule_withStatefulRunnable() {
        HazelcastInstance[] instances = createClusterWithCount(4);
        IScheduledExecutorService executorService = getScheduledExecutor(instances, "s");

        ICountDownLatch latch = instances[0].getCPSubsystem().getCountDownLatch("latch");
        latch.trySetCount(1);

        executorService.schedule(new StatefulRunnableTask("latch", "runC", "loadC"), 2, SECONDS);

        assertOpenEventually(latch);
    }

    @Test
    public void schedule_withNamedInstanceAware_whenLocalRun() {
        HazelcastInstance[] instances = createClusterWithCount(1);
        ICountDownLatch latch = instances[0].getCPSubsystem().getCountDownLatch("latch");
        latch.trySetCount(1);

        IScheduledExecutorService s = getScheduledExecutor(instances, "s");
        s.schedule(TaskUtils.named("blah", new PlainInstanceAwareRunnableTask("latch")), 1, TimeUnit.SECONDS);

        assertOpenEventually(latch);
        assertEquals(0, latch.getCount());
    }

    @Test
    public void schedule_withNamedInstanceAware_whenRemoteRun() {
        HazelcastInstance[] instances = createClusterWithCount(2);
        ICountDownLatch latch = instances[0].getCPSubsystem().getCountDownLatch("latch");
        latch.trySetCount(1);

        MemberImpl member = getNodeEngineImpl(instances[1]).getLocalMember();
        IScheduledExecutorService s = getScheduledExecutor(instances, "s");
        s.scheduleOnMember(TaskUtils.named("blah", new PlainInstanceAwareRunnableTask("latch")), member, 1, TimeUnit.SECONDS);

        assertOpenEventually(latch);
        assertEquals(0, latch.getCount());
    }

    @Test
    public void scheduleWithRepetition() {
        HazelcastInstance[] instances = createClusterWithCount(2);

        IScheduledExecutorService s = getScheduledExecutor(instances, "s");

        ICountDownLatch latch = instances[0].getCPSubsystem().getCountDownLatch("latch");
        latch.trySetCount(3);

        IScheduledFuture future = s.scheduleAtFixedRate(new ICountdownLatchRunnableTask("latch"), 0, 1, SECONDS);

        assertOpenEventually(latch);
        future.cancel(false);

        assertEquals(0, latch.getCount());
    }

    @Test
    public void scheduleOnMember() throws Exception {
        int delay = 1;

        HazelcastInstance[] instances = createClusterWithCount(2);
        IScheduledExecutorService executorService = getScheduledExecutor(instances, "s");

        MemberImpl member = getNodeEngineImpl(instances[0]).getLocalMember();
        IScheduledFuture<Double> future = executorService.scheduleOnMember(new PlainCallableTask(), member, delay, SECONDS);

        assertTrue(future.getHandler().isAssignedToMember());
        assertEquals(25.0, future.get(), 0);
    }

    @Test
    public void scheduleOnMemberWithRepetition() {
        HazelcastInstance[] instances = createClusterWithCount(4);
        IScheduledExecutorService s = getScheduledExecutor(instances, "s");

        ICountDownLatch latch = instances[0].getCPSubsystem().getCountDownLatch("latch");
        latch.trySetCount(4);

        Map<Member, IScheduledFuture<Object>> futures = s.scheduleOnAllMembersAtFixedRate(
                new ICountdownLatchRunnableTask("latch"), 0, 3, SECONDS);

        assertOpenEventually(latch);

        assertEquals(0, latch.getCount());
        assertEquals(4, futures.size());
    }

    @Test
    public void scheduleOnKeyOwner_thenGet() throws Exception {
        HazelcastInstance[] instances = createClusterWithCount(2);
        IScheduledExecutorService executorService = getScheduledExecutor(instances, "s");
        String key = generateKeyOwnedBy(instances[1]);

        IScheduledFuture<Double> future = executorService.scheduleOnKeyOwner(new PlainCallableTask(), key, 2, SECONDS);

        assertEquals(25.0, future.get(), 0.0);
    }

    @Test
    public void scheduleOnKeyOwner_withNotPeriodicRunnable() throws Exception {
        HazelcastInstance[] instances = createClusterWithCount(2);

        String key = generateKeyOwnedBy(instances[0]);
        IScheduledExecutorService s = getScheduledExecutor(instances, "s");

        ICountDownLatch latch = instances[0].getCPSubsystem().getCountDownLatch("latch");
        latch.trySetCount(1);

        s.scheduleOnKeyOwner(new ICountdownLatchRunnableTask("latch"), key, 2, SECONDS).get();
        assertEquals(0, latch.getCount());
    }

    @Test
    public void scheduleOnKeyOwner_withNotPeriodicRunnableDurable() throws Exception {
        HazelcastInstance[] instances = createClusterWithCount(2);

        String key = generateKeyOwnedBy(instances[1]);
        IScheduledExecutorService s = getScheduledExecutor(instances, "s");

        ICountDownLatch latch = instances[0].getCPSubsystem().getCountDownLatch("latch");
        latch.trySetCount(1);

        IScheduledFuture future = s.scheduleOnKeyOwner(new ICountdownLatchRunnableTask("latch"), key, 2, SECONDS);

        instances[1].getLifecycleService().shutdown();
        future.get();
        assertEquals(0, latch.getCount());
    }

    @Test
    public void scheduleOnKeyOwner_withCallable() throws Exception {
        int delay = 1;
        String key = "TestKey";

        HazelcastInstance[] instances = createClusterWithCount(2);
        IScheduledExecutorService executorService = getScheduledExecutor(instances, "s");
        Callable<Double> task = new PlainPartitionAwareCallableTask();
        IScheduledFuture<Double> first = executorService.scheduleOnKeyOwner(task, key, delay, SECONDS);

        ScheduledTaskHandler handler = first.getHandler();
        int expectedPartition = instances[0].getPartitionService()
                .getPartition(key)
                .getPartitionId();
        assertEquals(expectedPartition, handler.getPartitionId());
        assertEquals(25, first.get(), 0);
    }

    @Test
    public void scheduleOnKeyOwnerWithRepetition() {
        String key = "TestKey";

        HazelcastInstance[] instances = createClusterWithCount(2);
        IScheduledExecutorService executorService = getScheduledExecutor(instances, "s");

        ICountDownLatch latch = instances[0].getCPSubsystem().getCountDownLatch("latch");
        latch.trySetCount(5);

        IScheduledFuture future = executorService.scheduleOnKeyOwnerAtFixedRate(
                new ICountdownLatchRunnableTask("latch"), key, 0, 1, SECONDS);

        ScheduledTaskHandler handler = future.getHandler();
        int expectedPartition = instances[0].getPartitionService()
                .getPartition(key)
                .getPartitionId();

        assertEquals(expectedPartition, handler.getPartitionId());

        assertOpenEventually(latch);
        assertEquals(0, latch.getCount());
    }

    @Test
    public void getScheduled() {
        int delay = 1;
        String taskName = "Test";

        HazelcastInstance[] instances = createClusterWithCount(2);
        IScheduledExecutorService executorService = getScheduledExecutor(instances, "s");
        IScheduledFuture<Double> first = executorService.schedule(named(taskName, new PlainCallableTask()), delay, SECONDS);

        ScheduledTaskHandler handler = first.getHandler();
        IScheduledFuture<Double> copy = executorService.getScheduledFuture(handler);

        assertEquals(first.getHandler(), copy.getHandler());
    }

    @Test
    public void scheduleOnAllMembers_getAllScheduled() throws Exception {
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
    public void scheduleRandomPartitions_getAllScheduled() throws Exception {
        HazelcastInstance[] instances = createClusterWithCount(2);
        IScheduledExecutorService s = getScheduledExecutor(instances, "s");

        int expectedTotal = 11;
        IScheduledFuture[] futures = new IScheduledFuture[expectedTotal];
        for (int i = 0; i < expectedTotal; i++) {
            futures[i] = s.schedule(new PlainCallableTask(i), 0, SECONDS);
        }

        assertEquals(expectedTotal, countScheduledTasksOn(s), 0);

        // dispose 1 task
        futures[0].dispose();

        // recount
        assertEquals(expectedTotal - 1, countScheduledTasksOn(s), 0);

        // verify all tasks
        for (int i = 1; i < expectedTotal; i++) {
            assertEquals(25.0 + i, futures[i].get());
        }
    }

    @Test
    public void getErroneous() throws Exception {
        int delay = 2;
        String taskName = "Test";
        String completionLatchName = "completionLatch";

        HazelcastInstance[] instances = createClusterWithCount(2);

        String key = generateKeyOwnedBy(instances[1]);
        IScheduledExecutorService executorService = getScheduledExecutor(instances, "s");
        ICountDownLatch latch = instances[1].getCPSubsystem().getCountDownLatch(completionLatchName);
        latch.trySetCount(1);

        IScheduledFuture<Double> future = executorService.scheduleOnKeyOwner(
                named(taskName, new ErroneousCallableTask(completionLatchName)), key, delay, SECONDS);

        assertOpenEventually(latch);
        expected.expect(ExecutionException.class);
        expected.expectCause(new RootCauseMatcher(IllegalStateException.class, "Erroneous task"));
        future.get();
    }

    @Test
    public void getErroneous_durable() throws Exception {
        int delay = 2;
        String taskName = "Test";
        String completionLatchName = "completionLatch";

        HazelcastInstance[] instances = createClusterWithCount(2);

        String key = generateKeyOwnedBy(instances[1]);
        IScheduledExecutorService executorService = getScheduledExecutor(instances, "s");
        ICountDownLatch latch = instances[1].getCPSubsystem().getCountDownLatch(completionLatchName);
        latch.trySetCount(1);

        IScheduledFuture<Double> future = executorService.scheduleOnKeyOwner(
                named(taskName, new ErroneousCallableTask(completionLatchName)), key, delay, SECONDS);

        assertOpenEventually(latch);
        instances[1].getLifecycleService().shutdown();
        Thread.sleep(2000);

        expected.expect(ExecutionException.class);
        expected.expectCause(new RootCauseMatcher(IllegalStateException.class, "Erroneous task"));
        future.get();
    }

    @Test
    public void managedContext_whenLocalExecution() {
        HazelcastInstance instance = createHazelcastInstance();
        IScheduledExecutorService s = instance.getScheduledExecutorService("s");
        s.schedule(new PlainCallableTask(), 0, SECONDS);
    }
}
