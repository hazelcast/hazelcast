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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.ICountDownLatch;
import com.hazelcast.core.Member;
import com.hazelcast.core.PartitionAware;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.spi.partition.IPartitionLostEvent;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
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

import static com.hazelcast.scheduledexecutor.TaskHelper.named;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Created by Thomas Kountis.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ScheduledExecutorServiceTest extends HazelcastTestSupport {

    private TestHazelcastInstanceFactory factory;

    @After
    public void teardown() {
        if (factory != null) {
            factory.shutdownAll();
        }
    }

    @Test
    public void handlerTaskAndSchedulerNames()
            throws ExecutionException, InterruptedException {

        int delay = 0;
        String schedulerName = "s";
        String taskName = "Test";

        HazelcastInstance[] instances = createClusterWithCount(1);
        IScheduledExecutorService executorService = instances[0].getScheduledExecutorService(schedulerName);
        IScheduledFuture<Double> future = executorService.schedule(
                named(taskName, new PlainCallableTask()), delay, TimeUnit.SECONDS);

        future.get();

        ScheduledTaskHandler handler = future.getHandler();
        assertEquals(schedulerName, handler.getSchedulerName());
        assertEquals(taskName, handler.getTaskName());
    }

    @Test
    public void stats()
            throws ExecutionException, InterruptedException {
        double delay = 2.0;
        double delayGracePeriod = 2.0; //Could fail sporadically, if so will remove

        HazelcastInstance[] instances = createClusterWithCount(1);
        IScheduledExecutorService executorService = instances[0].getScheduledExecutorService("s");
        IScheduledFuture<Double> future = executorService.schedule(
                named("Test", new PlainCallableTask()), (int) delay, TimeUnit.SECONDS);


        future.get();
        ScheduledTaskStatistics stats = future.getStats();
        double durationFromCreationToScheduleIn = TimeUnit.SECONDS.convert(
                stats.getLastRunStart() - stats.getCreatedAt(), TimeUnit.NANOSECONDS);

        assertEquals(1, stats.getTotalRuns());
        assertEquals(delay, durationFromCreationToScheduleIn, delayGracePeriod);
        assertNotNull(stats.getCreatedAt());
        assertNotNull(stats.getFirstRunStart());
        assertNotNull(stats.getLastIdleTime(TimeUnit.SECONDS));
        assertNotNull(stats.getLastRunDuration(TimeUnit.SECONDS));
        assertNotNull(stats.getLastRunStart());
        assertNotNull(stats.getLastRunDuration(TimeUnit.SECONDS));
        assertNotNull(stats.getTotalIdleTime(TimeUnit.SECONDS));
        assertNotNull(stats.getTotalRunTime(TimeUnit.SECONDS));
        assertNotNull(stats.getTotalRuns());
    }

    @Test
    public void schedule_withCallable()
            throws ExecutionException, InterruptedException {

        int delay = 5;
        String taskName = "Test";
        double expectedResult = 25.0;

        HazelcastInstance[] instances = createClusterWithCount(1);
        IScheduledExecutorService executorService = instances[0].getScheduledExecutorService("s");
        IScheduledFuture<Double> future = executorService.schedule(
                named(taskName, new PlainCallableTask()), delay, TimeUnit.SECONDS);

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

        HazelcastInstance[] instances = createClusterWithCount(1);
        IScheduledExecutorService executorService = instances[0].getScheduledExecutorService("s");
        IScheduledFuture<Double> first = executorService.schedule(
                named(taskName, new PlainCallableTask()), delay, TimeUnit.SECONDS);

        IScheduledFuture<Double> second = executorService.schedule(
                named(taskName, new PlainCallableTask()), delay, TimeUnit.SECONDS);
    }

    @Test(expected = CancellationException.class)
    public void schedule_thenCancelAndGet()
            throws ExecutionException, InterruptedException {
        int delay = 1;
        String taskName = "Test";

        HazelcastInstance[] instances = createClusterWithCount(1);
        IScheduledExecutorService executorService = instances[0].getScheduledExecutorService("s");
        IScheduledFuture<Double> first = executorService.schedule(
                named(taskName, new PlainCallableTask()), delay, TimeUnit.SECONDS);

        first.cancel(true);
        first.get();
    }

    @Test()
    public void schedule_getDelay()
            throws ExecutionException, InterruptedException {
        int delay = 20;
        String taskName = "Test";

        HazelcastInstance[] instances = createClusterWithCount(1);
        IScheduledExecutorService executorService = instances[0].getScheduledExecutorService("s");
        IScheduledFuture<Double> first = executorService.schedule(
                named(taskName, new PlainCallableTask()), delay, TimeUnit.MINUTES);

        assertEquals(19, first.getDelay(TimeUnit.MINUTES));
    }

    @Test(expected = StaleTaskException.class)
    public void schedule_thenDisposeThenGet()
            throws ExecutionException, InterruptedException {
        int delay = 1;
        String taskName = "Test";

        HazelcastInstance[] instances = createClusterWithCount(1);
        IScheduledExecutorService executorService = instances[0].getScheduledExecutorService("s");
        IScheduledFuture<Double> first = executorService.schedule(
                named(taskName, new PlainCallableTask()), delay, TimeUnit.SECONDS);

        first.dispose();
        first.get();
    }

    @Test(expected = RejectedExecutionException.class)
    public void schedule_whenShutdown()
            throws ExecutionException, InterruptedException {
        int delay = 1;

        HazelcastInstance[] instances = createClusterWithCount(1);
        IScheduledExecutorService executorService = instances[0].getScheduledExecutorService("s");
        executorService.schedule(new PlainCallableTask(), delay, TimeUnit.SECONDS);
        executorService.shutdown();

        executorService.schedule(new PlainCallableTask(), delay, TimeUnit.SECONDS);
    }
    @Test()
    public void schedule_whenPartitionLost()
            throws ExecutionException, InterruptedException {
        int delay = 1;

        HazelcastInstance[] instances = createClusterWithCount(1);
        IScheduledExecutorService executorService = instances[0].getScheduledExecutorService("s");
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

        HazelcastInstance[] instances = createClusterWithCount(1);
        IScheduledExecutorService executorService = instances[0].getScheduledExecutorService("s");
        IScheduledFuture<Double> first = executorService.schedule(
                named(taskName, new PlainCallableTask()), delay, TimeUnit.SECONDS);

        ScheduledTaskHandler handler = first.getHandler();
        first.dispose();

        executorService.getScheduled(handler).get();
    }

    @Test()
    public void schedule_partitionAware()
            throws ExecutionException, InterruptedException {
        int delay = 1;

        HazelcastInstance[] instances = createClusterWithCount(1);
        IScheduledExecutorService executorService = instances[0].getScheduledExecutorService("s");
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
        IScheduledExecutorService executorService = instances[0].getScheduledExecutorService("s");

        ICountDownLatch latch = instances[0].getCountDownLatch("latch");
        latch.trySetCount(1);

        executorService.schedule(
                new StatefullRunnableTask("latch", instances[1]), 2, TimeUnit.SECONDS);

        latch.await(10, TimeUnit.SECONDS);
    }

    @Test
    public void scheduleWithRepetition()
            throws ExecutionException, InterruptedException {
        HazelcastInstance[] instances = createClusterWithCount(1);

        IScheduledExecutorService s = instances[0].getScheduledExecutorService("s");

        ICountDownLatch latch = instances[0].getCountDownLatch("latch");
        latch.trySetCount(3);

        IScheduledFuture future = s.scheduleWithRepetition(new ICountdownLatchRunnableTask("latch", instances[0]),
                0, 1, TimeUnit.SECONDS);

        latch.await(10, TimeUnit.SECONDS);
        future.cancel(true);

        assertEquals(0, latch.getCount());
    }

    @Test
    public void scheduleOnMember_withRunnable()
            throws ExecutionException, InterruptedException {
        int delay = 1;

        HazelcastInstance[] instances = createClusterWithCount(2);
        IScheduledExecutorService executorService = instances[0].getScheduledExecutorService("s");

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
        IScheduledExecutorService s = instances[0].getScheduledExecutorService("s");

        ICountDownLatch latch = instances[0].getCountDownLatch("latch");
        latch.trySetCount(4);

        Map<Member, IScheduledFuture<?>> futures = s
                .scheduleOnAllMembersWithRepetition(new ICountdownLatchRunnableTask("latch", instances[0]),
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
        IScheduledExecutorService s = instances[0].getScheduledExecutorService("s");

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
        IScheduledExecutorService s = instances[0].getScheduledExecutorService("s");

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

        HazelcastInstance[] instances = createClusterWithCount(1);
        IScheduledExecutorService executorService = instances[0].getScheduledExecutorService("s");
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

        HazelcastInstance[] instances = createClusterWithCount(1);
        IScheduledExecutorService executorService = instances[0].getScheduledExecutorService("s");

        ICountDownLatch latch = instances[0].getCountDownLatch("latch");
        latch.trySetCount(5);

        IScheduledFuture future = executorService.scheduleOnKeyOwnerWithRepetition(
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

        HazelcastInstance[] instances = createClusterWithCount(1);
        IScheduledExecutorService executorService = instances[0].getScheduledExecutorService("s");
        IScheduledFuture<Double> first = executorService.schedule(
                named(taskName, new PlainCallableTask()), delay, TimeUnit.SECONDS);

        ScheduledTaskHandler handler = first.getHandler();
        IScheduledFuture<Double> copy = executorService.getScheduled(handler);

        assertEquals(first, copy);
    }

    @Test
    public void getAllScheduled()
            throws ExecutionException, InterruptedException {

        HazelcastInstance[] instances = createClusterWithCount(3);
        IScheduledExecutorService s = instances[0].getScheduledExecutorService("s");
        s.scheduleOnAllMembers(new PlainCallableTask(), 0, TimeUnit.SECONDS);

        Set<Member> members = instances[0].getCluster().getMembers();
        Map<Member, List<IScheduledFuture<Double>>> allScheduled = s.getAllScheduled();

        assertEquals(members.size(), allScheduled.size());

        for (Member member : members) {
            assertEquals(1, allScheduled.get(member).size());
            assertEquals(25.0, allScheduled.get(member).get(0).get(), 0);
        }
    }

    public HazelcastInstance[] createClusterWithCount(int count) {
        HazelcastInstance[] instances = new HazelcastInstance[count];

        factory = createHazelcastInstanceFactory(count);
        for (int i = 0; i < count; i++) {
            instances[i] = factory.newHazelcastInstance();
        }

        return instances;
    }

    static class StatefullRunnableTask
            implements Runnable, Serializable,
                       HazelcastInstanceAware, StatefulTask<String, Integer> {

        final String latchName;

        int status;

        transient HazelcastInstance instance;

        StatefullRunnableTask(String latchName, HazelcastInstance instance) {
            this.latchName = latchName;
            this.instance = instance;
        }

        @Override
        public void run() {
            status = 66 * 77;
        }

        @Override
        public void loadState(Map<String, Integer> state) {
            if (!state.isEmpty()) {
                assertEquals(66 * 77, (int) state.get("status"));
                instance.getCountDownLatch(latchName).countDown();
            }
        }

        @Override
        public void saveState(Map<String, Integer> state) {
            state.put("status", status);
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            this.instance = hazelcastInstance;
        }
    }

    static class ICountdownLatchRunnableTask implements Runnable, Serializable, HazelcastInstanceAware {

        final String latchName;

        transient HazelcastInstance instance;

        ICountdownLatchRunnableTask(String latchName, HazelcastInstance instance) {
            this.latchName = latchName;
            this.instance = instance;
        }

        @Override
        public void run() {
            instance.getCountDownLatch(latchName).countDown();
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
