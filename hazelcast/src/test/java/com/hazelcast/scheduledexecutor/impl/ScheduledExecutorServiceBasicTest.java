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

import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.config.Config;
import com.hazelcast.config.ScheduledExecutorConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.IAtomicLong;
import com.hazelcast.cp.ICountDownLatch;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.collectors.MetricsCollector;
import com.hazelcast.internal.partition.IPartitionLostEvent;
import com.hazelcast.internal.partition.PartitionLostEventImpl;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.internal.util.RootCauseMatcher;
import com.hazelcast.internal.util.executor.ManagedExecutorService;
import com.hazelcast.map.IMap;
import com.hazelcast.partition.PartitionAware;
import com.hazelcast.scheduledexecutor.DuplicateTaskException;
import com.hazelcast.scheduledexecutor.IScheduledExecutorService;
import com.hazelcast.scheduledexecutor.IScheduledFuture;
import com.hazelcast.scheduledexecutor.ScheduledTaskHandler;
import com.hazelcast.scheduledexecutor.ScheduledTaskStatistics;
import com.hazelcast.scheduledexecutor.StaleTaskException;
import com.hazelcast.scheduledexecutor.TaskUtils;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.Accessors;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.internal.metrics.MetricDescriptorConstants.EXECUTOR_METRIC_CANCELLED;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.EXECUTOR_METRIC_COMPLETED;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.EXECUTOR_METRIC_CREATION_TIME;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.EXECUTOR_METRIC_PENDING;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.EXECUTOR_METRIC_STARTED;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.EXECUTOR_METRIC_TOTAL_EXECUTION_TIME;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.EXECUTOR_METRIC_TOTAL_START_LATENCY;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.SCHEDULED_EXECUTOR_PREFIX;
import static com.hazelcast.internal.partition.IPartition.MAX_BACKUP_COUNT;
import static com.hazelcast.internal.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.scheduledexecutor.TaskUtils.autoDisposable;
import static com.hazelcast.scheduledexecutor.TaskUtils.named;
import static com.hazelcast.spi.properties.ClusterProperty.PARTITION_COUNT;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
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

    private static final String ANY_EXECUTOR_NAME = "s";

    @Rule
    public ExpectedException expected = ExpectedException.none();

    @Test
    public void config() {
        String schedulerName = ANY_EXECUTOR_NAME;

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
        IScheduledExecutorService service = instances[0].getScheduledExecutorService(ANY_EXECUTOR_NAME);

        final IScheduledFuture f = service.scheduleAtFixedRate(
                new ErroneousRunnableTask(), 1, 1, TimeUnit.SECONDS);

        assertTrueEventually(() -> assertTrue(f.isDone()));

        assertEquals(1L, f.getStats().getTotalRuns());
        expected.expect(ExecutionException.class);
        expected.expectCause(new RootCauseMatcher(IllegalStateException.class, "Erroneous task"));

        f.get();
    }

    @Test
    public void capacity_whenNoLimit_perNode() {
        String schedulerName = ANY_EXECUTOR_NAME;

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
    public void capacity_whenNoLimit_perPartition() {
        String schedulerName = ANY_EXECUTOR_NAME;

        ScheduledExecutorConfig sec = new ScheduledExecutorConfig()
                .setName(schedulerName)
                .setDurability(1)
                .setPoolSize(1)
                .setCapacity(0)
                .setCapacityPolicy(ScheduledExecutorConfig.CapacityPolicy.PER_PARTITION);

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
        String schedulerName = ANY_EXECUTOR_NAME;

        HazelcastInstance[] instances = createClusterWithCount(1, null);
        IScheduledExecutorService service = instances[0].getScheduledExecutorService(schedulerName);
        List<IScheduledFuture> futures = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            futures.add(service.schedule(new PlainCallableTask(), 0, TimeUnit.SECONDS));
        }

        assertCapacityReached(service, null, "Maximum capacity (100) of tasks reached "
                + "for this member and scheduled executor (" + schedulerName + ").");

        // Dispose all
        for (IScheduledFuture future : futures) {
            future.dispose();
        }

        // Re-schedule to verify capacity
        for (int i = 0; i < 100; i++) {
            service.schedule(new PlainCallableTask(), 0, TimeUnit.SECONDS);
        }

        assertCapacityReached(service, null, "Maximum capacity (100) of tasks reached "
                + "for this member and scheduled executor (" + schedulerName + ").");
    }

    @Test
    public void capacity_whenDefault_perPartition() {
        String schedulerName = ANY_EXECUTOR_NAME;
        ScheduledExecutorConfig sec = new ScheduledExecutorConfig()
                .setName(schedulerName)
                .setCapacityPolicy(ScheduledExecutorConfig.CapacityPolicy.PER_PARTITION);

        Config config = new Config().addScheduledExecutorConfig(sec);

        HazelcastInstance[] instances = createClusterWithCount(1, config);
        IScheduledExecutorService service = instances[0].getScheduledExecutorService(schedulerName);
        String key = "hitSamePartitionToCheckCapacity";
        int keyOwner = getNodeEngineImpl(instances[0]).getPartitionService().getPartitionId(key);

        List<IScheduledFuture> futures = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            futures.add(service.scheduleOnKeyOwner(new PlainCallableTask(), key, 0, TimeUnit.SECONDS));
        }

        assertCapacityReached(service, key, "Maximum capacity (100) of tasks reached "
                + "for partition (" + keyOwner + ") and scheduled executor (" + schedulerName + ").");

        // Dispose all
        for (IScheduledFuture future : futures) {
            future.dispose();
        }

        // Re-schedule to verify capacity
        for (int i = 0; i < 100; i++) {
            service.scheduleOnKeyOwner(new PlainCallableTask(), key, 0, TimeUnit.SECONDS);
        }

        assertCapacityReached(service, key, "Maximum capacity (100) of tasks reached "
                + "for partition (" + keyOwner + ") and scheduled executor (" + schedulerName + ").");
    }

    @Test
    public void capacity_whenPositiveLimit() {
        String schedulerName = ANY_EXECUTOR_NAME;

        ScheduledExecutorConfig sec = new ScheduledExecutorConfig()
                .setName(schedulerName)
                .setDurability(1)
                .setPoolSize(1)
                .setCapacity(10)
                .setCapacityPolicy(ScheduledExecutorConfig.CapacityPolicy.PER_PARTITION);

        Config config = new Config().addScheduledExecutorConfig(sec);

        HazelcastInstance[] instances = createClusterWithCount(1, config);
        IScheduledExecutorService service = instances[0].getScheduledExecutorService(schedulerName);
        String key = "hitSamePartitionToCheckCapacity";
        int keyOwner = getNodeEngineImpl(instances[0]).getPartitionService().getPartitionId(key);

        List<IScheduledFuture> futures = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            futures.add(service.scheduleOnKeyOwner(new PlainCallableTask(), key, 0, TimeUnit.SECONDS));
        }

        assertCapacityReached(service, key, "Maximum capacity (10) of tasks reached "
                + "for partition (" + keyOwner + ") and scheduled executor (" + schedulerName + ").");

        // Dispose all
        for (IScheduledFuture future : futures) {
            future.dispose();
        }

        // Re-schedule to verify capacity
        for (int i = 0; i < 10; i++) {
            service.scheduleOnKeyOwner(new PlainCallableTask(), key, 0, TimeUnit.SECONDS);
        }

        assertCapacityReached(service, key, "Maximum capacity (10) of tasks reached "
                + "for partition (" + keyOwner + ") and scheduled executor (" + schedulerName + ").");

    }

    @Test
    public void capacity_whenPositiveLimit_andMigration()
            throws ExecutionException, InterruptedException {
        String schedulerName = ANY_EXECUTOR_NAME;

        ScheduledExecutorConfig sec = new ScheduledExecutorConfig()
                .setName(schedulerName)
                .setDurability(1)
                .setPoolSize(1)
                .setCapacity(1)
                .setCapacityPolicy(ScheduledExecutorConfig.CapacityPolicy.PER_PARTITION);

        Config config = new Config().addScheduledExecutorConfig(sec);

        HazelcastInstance[] instances = createClusterWithCount(2, config);
        IScheduledExecutorService service = instances[0].getScheduledExecutorService(schedulerName);
        String key = generateKeyOwnedBy(instances[0]);
        int keyOwner = getNodeEngineImpl(instances[0]).getPartitionService().getPartitionId(key);

        IScheduledFuture future = service.scheduleOnKeyOwner(new PlainCallableTask(), key, 0, TimeUnit.SECONDS);
        future.get();

        assertCapacityReached(service, key, "Maximum capacity (1) of tasks reached "
                + "for partition (" + keyOwner + ") and scheduled executor (" + schedulerName + ").");

        instances[0].getLifecycleService().shutdown();
        waitAllForSafeState(instances[1]);
        // Re-assign service & future
        service = instances[1].getScheduledExecutorService(schedulerName);
        future = service.getScheduledFuture(future.getHandler());

        assertCapacityReached(service, key, "Maximum capacity (1) of tasks reached "
                + "for partition (" + keyOwner + ") and scheduled executor (" + schedulerName + ").");

        future.dispose();
        service.scheduleOnKeyOwner(new PlainCallableTask(), key, 0, TimeUnit.SECONDS);

        assertCapacityReached(service, key, "Maximum capacity (1) of tasks reached "
                + "for partition (" + keyOwner + ") and scheduled executor (" + schedulerName + ").");
    }

    /**
     * Make sure disposal of SUSPENDED tasks doesn't release permits
     */
    @Test
    public void capacity_whenPositiveLimit_afterDisposing_andReplicaPartitionPromotion()
            throws ExecutionException, InterruptedException {
        String schedulerName = ANY_EXECUTOR_NAME;

        ScheduledExecutorConfig sec = new ScheduledExecutorConfig()
                .setName(schedulerName)
                .setDurability(1)
                .setPoolSize(1)
                .setCapacity(1)
                .setCapacityPolicy(ScheduledExecutorConfig.CapacityPolicy.PER_PARTITION);

        Config config = new Config().addScheduledExecutorConfig(sec);

        HazelcastInstance[] instances = createClusterWithCount(2, config);
        IScheduledExecutorService service = instances[0].getScheduledExecutorService(schedulerName);
        String key = generateKeyOwnedBy(instances[0]);
        int keyOwner = getNodeEngineImpl(instances[0]).getPartitionService().getPartitionId(key);

        IScheduledFuture future = service.scheduleOnKeyOwner(new PlainCallableTask(), key, 0, TimeUnit.SECONDS);
        future.get();

        assertCapacityReached(service, key, "Maximum capacity (1) of tasks reached "
                + "for partition (" + keyOwner + ") and scheduled executor (" + schedulerName + ").");

        instances[0].getLifecycleService().shutdown();
        waitAllForSafeState(instances[1]);
        // Re-assign service & future
        service = instances[1].getScheduledExecutorService(schedulerName);
        future = service.getScheduledFuture(future.getHandler());

        assertCapacityReached(service, key, "Maximum capacity (1) of tasks reached "
                + "for partition (" + keyOwner + ") and scheduled executor (" + schedulerName + ").");

        future.dispose();
        service.scheduleOnKeyOwner(new PlainCallableTask(), key, 0, TimeUnit.SECONDS);

        assertCapacityReached(service, key, "Maximum capacity (1) of tasks reached "
                + "for partition (" + keyOwner + ") and scheduled executor (" + schedulerName + ").");
    }

    @Test
    public void capacity_whenPositiveLimit_perNode_afterDisposing_andReplicaPartitionPromotion()
            throws ExecutionException, InterruptedException {
        String schedulerName = ANY_EXECUTOR_NAME;

        ScheduledExecutorConfig sec = new ScheduledExecutorConfig()
                .setName(schedulerName)
                .setDurability(1)
                .setPoolSize(1)
                .setCapacity(1)
                .setCapacityPolicy(ScheduledExecutorConfig.CapacityPolicy.PER_NODE);

        Config config = new Config().addScheduledExecutorConfig(sec);

        HazelcastInstance[] instances = createClusterWithCount(2, config);
        IScheduledExecutorService service = instances[0].getScheduledExecutorService(schedulerName);
        String key = generateKeyOwnedBy(instances[0]);

        IScheduledFuture future = service.scheduleOnKeyOwner(new PlainCallableTask(), key, 0, TimeUnit.SECONDS);
        future.get();

        assertCapacityReached(service, key, "Maximum capacity (1) of tasks reached for this "
                + "member and scheduled executor (" + schedulerName + ").");

        instances[0].getLifecycleService().shutdown();
        waitAllForSafeState(instances[1]);
        // Re-assign service & future
        service = instances[1].getScheduledExecutorService(schedulerName);
        future = service.getScheduledFuture(future.getHandler());

        assertCapacityReached(service, key, "Maximum capacity (1) of tasks reached for this "
                + "member and scheduled executor (" + schedulerName + ").");

        future.dispose();
        service.scheduleOnKeyOwner(new PlainCallableTask(), key, 0, TimeUnit.SECONDS);

        assertCapacityReached(service, key, "Maximum capacity (1) of tasks reached for this "
                + "member and scheduled executor (" + schedulerName + ").");
    }

    @Test
    public void capacity_whenPositiveLimit_completedTask_andFirstPromotionFails()
            throws ExecutionException, InterruptedException {
        String schedulerName = ANY_EXECUTOR_NAME;

        ScheduledExecutorConfig sec = new ScheduledExecutorConfig()
                .setName(schedulerName)
                .setDurability(1)
                .setPoolSize(1)
                .setCapacity(1)
                .setCapacityPolicy(ScheduledExecutorConfig.CapacityPolicy.PER_PARTITION);

        Config config = new Config().addScheduledExecutorConfig(sec);
        config.setProperty(PARTITION_COUNT.getName(), "3");

        TestHazelcastInstanceFactory hazelcastInstanceFactory = createHazelcastInstanceFactory();
        HazelcastInstance[] instances = hazelcastInstanceFactory.newInstances(config, 3);
        IScheduledExecutorService service = instances[0].getScheduledExecutorService(schedulerName);
        String key = generateKeyOwnedBy(instances[0]);
        int keyOwner = getNodeEngineImpl(instances[0]).getPartitionService().getPartitionId(key);

        IScheduledFuture future = service.scheduleOnKeyOwner(new PlainCallableTask(), key, 0, TimeUnit.SECONDS);
        future.get();

        assertCapacityReached(service, key, "Maximum capacity (1) of tasks reached "
                + "for partition (" + keyOwner + ") and scheduled executor (" + schedulerName + ").");

        // Fail promotion
        DistributedScheduledExecutorService.FAIL_MIGRATIONS.set(true);
        instances[0].getLifecycleService().terminate();

        waitAllForSafeState(instances);

        service = instances[1].getScheduledExecutorService(schedulerName);
        assertCapacityReached(service, key, "Maximum capacity (1) of tasks reached "
                + "for partition (" + keyOwner + ") and scheduled executor (" + schedulerName + ").");
    }

    @Test
    public void capacity_whenPositiveLimit_pendingTask_andFirstPromotionFails() {
        String schedulerName = ANY_EXECUTOR_NAME;

        ScheduledExecutorConfig sec = new ScheduledExecutorConfig()
                .setName(schedulerName)
                .setDurability(1)
                .setPoolSize(1)
                .setCapacity(1)
                .setCapacityPolicy(ScheduledExecutorConfig.CapacityPolicy.PER_PARTITION);

        Config config = new Config().addScheduledExecutorConfig(sec);
        config.setProperty(PARTITION_COUNT.getName(), "3");

        TestHazelcastInstanceFactory hazelcastInstanceFactory = createHazelcastInstanceFactory();
        HazelcastInstance[] instances = hazelcastInstanceFactory.newInstances(config, 3);
        IScheduledExecutorService service = instances[0].getScheduledExecutorService(schedulerName);
        String key = generateKeyOwnedBy(instances[0]);
        int keyOwner = getNodeEngineImpl(instances[0]).getPartitionService().getPartitionId(key);

        IScheduledFuture future = service.scheduleOnKeyOwner(new PlainCallableTask(), key, 1, TimeUnit.HOURS);
        future.getStats();

        assertCapacityReached(service, key, "Maximum capacity (1) of tasks reached "
                + "for partition (" + keyOwner + ") and scheduled executor (" + schedulerName + ").");

        // Fail promotion
        DistributedScheduledExecutorService.FAIL_MIGRATIONS.set(true);
        instances[0].getLifecycleService().terminate();

        waitAllForSafeState(instances);

        service = instances[1].getScheduledExecutorService(schedulerName);
        assertCapacityReached(service, key, "Maximum capacity (1) of tasks reached "
                + "for partition (" + keyOwner + ") and scheduled executor (" + schedulerName + ").");
    }

    @Test
    public void capacity_whenPositiveLimit_onMember_andMigration()
            throws ExecutionException, InterruptedException {
        String schedulerName = ANY_EXECUTOR_NAME;

        ScheduledExecutorConfig sec = new ScheduledExecutorConfig()
                .setName(schedulerName)
                .setDurability(1)
                .setPoolSize(1)
                .setCapacity(3)
                .setCapacityPolicy(ScheduledExecutorConfig.CapacityPolicy.PER_NODE);

        Config config = new Config().addScheduledExecutorConfig(sec);

        HazelcastInstance[] instances = createClusterWithCount(2, config);
        IScheduledExecutorService serviceA = instances[0].getScheduledExecutorService(schedulerName);
        IScheduledExecutorService serviceB = instances[1].getScheduledExecutorService(schedulerName);

        // Fill-up both nodes
        String key = generateKeyOwnedBy(instances[0]);
        IScheduledFuture futureAA = serviceA.scheduleOnKeyOwner(new PlainCallableTask(), key, 0, TimeUnit.SECONDS);
        IScheduledFuture futureAB = serviceA.scheduleOnKeyOwner(new PlainCallableTask(), key, 0, TimeUnit.SECONDS);
        IScheduledFuture futureAC = serviceA.scheduleOnKeyOwner(new PlainCallableTask(), key, 0, TimeUnit.SECONDS);
        futureAA.get();
        futureAB.get();
        futureAC.get();

        key = generateKeyOwnedBy(instances[1]);
        IScheduledFuture futureBA = serviceB.scheduleOnKeyOwner(new PlainCallableTask(), key, 0, TimeUnit.SECONDS);
        IScheduledFuture futureBB = serviceB.scheduleOnKeyOwner(new PlainCallableTask(), key, 0, TimeUnit.SECONDS);
        futureBA.get();
        futureBB.get();

        // At this point both Node A has 5 tasks each.
        // Accounting for both primary copies & replicas
        // Node B has still availability for 1 more primary task

        // Scheduling on different partition on Node A should be rejected
        key = generateKeyOwnedBy(instances[0]);
        assertCapacityReached(serviceA, key, "Maximum capacity (3) of tasks reached "
                + "for this member and scheduled executor (" + schedulerName + ").");

        instances[0].getLifecycleService().shutdown();
        waitAllForSafeState(instances[1]);

        // At this point Node B has 5 primary tasks.
        // No new tasks should be allowed
        // Re-fetch future from Node B
        futureAA = serviceB.getScheduledFuture(futureAA.getHandler());
        futureAB = serviceB.getScheduledFuture(futureAB.getHandler());
        futureAC = serviceB.getScheduledFuture(futureAC.getHandler());

        assertCapacityReached(serviceB, null, "Maximum capacity (3) of tasks reached "
                + "for this member and scheduled executor (" + schedulerName + ").");

        // Disposing node's A tasks will be enough to schedule ONE more,
        // since Node B was already holding 2 tasks before the migration
        futureAA.dispose();
        assertCapacityReached(serviceB, null, "Maximum capacity (3) of tasks reached "
                + "for this member and scheduled executor (" + schedulerName + ").");
        futureAB.dispose();
        assertCapacityReached(serviceB, null, "Maximum capacity (3) of tasks reached "
                + "for this member and scheduled executor (" + schedulerName + ").");
        futureAC.dispose();

        // Now we should be able to schedule again ONE more
        IScheduledFuture futureBC = serviceB.scheduleOnKeyOwner(new PlainCallableTask(), key, 0, TimeUnit.SECONDS);
        assertCapacityReached(serviceB, null, "Maximum capacity (3) of tasks reached "
                + "for this member and scheduled executor (" + schedulerName + ").");
        futureBA.dispose();
        futureBB.dispose();
        futureBC.dispose();

        // Clean slate - can schedule 3 more
        serviceB.scheduleOnKeyOwner(new PlainCallableTask(), key, 0, TimeUnit.SECONDS);
        serviceB.scheduleOnKeyOwner(new PlainCallableTask(), key, 0, TimeUnit.SECONDS);
        serviceB.scheduleOnKeyOwner(new PlainCallableTask(), key, 0, TimeUnit.SECONDS);
        assertCapacityReached(serviceB, null, "Maximum capacity (3) of tasks reached "
                + "for this member and scheduled executor (" + schedulerName + ").");
    }

    @Test
    public void capacity_whenAutoDisposable_Callable() throws Exception {
        String schedulerName = ANY_EXECUTOR_NAME;
        int capacity = 10;

        ScheduledExecutorConfig sec = new ScheduledExecutorConfig()
                .setName(schedulerName)
                .setDurability(1)
                .setPoolSize(1)
                .setCapacity(capacity);

        Config config = new Config().addScheduledExecutorConfig(sec);

        HazelcastInstance[] instances = createClusterWithCount(1, config);
        IScheduledExecutorService service = instances[0].getScheduledExecutorService(schedulerName);
        String keyOwner = "hitSamePartitionToCheckCapacity";

        List<IScheduledFuture<Double>> futures = new ArrayList<>();
        for (int i = 0; i < capacity; i++) {
            Callable<Double> command = autoDisposable(new PlainCallableTask());
            IScheduledFuture<Double> future = service.scheduleOnKeyOwner(command, keyOwner, 0, SECONDS);
            futures.add(future);
        }

        futures.forEach(this::assertTaskHasBeenDestroyedEventually);

        for (int i = 0; i < capacity; i++) {
            service.scheduleOnKeyOwner(autoDisposable(new PlainCallableTask()), keyOwner, 0, TimeUnit.SECONDS);
        }

        // no exceptions thrown
    }

    @Test
    public void capacity_whenAutoDisposable_Runnable() throws Exception {
        String schedulerName = ANY_EXECUTOR_NAME;
        int capacity = 10;

        ScheduledExecutorConfig sec = new ScheduledExecutorConfig()
                .setName(schedulerName)
                .setDurability(1)
                .setPoolSize(1)
                .setCapacity(capacity);

        Config config = new Config().addScheduledExecutorConfig(sec);

        HazelcastInstance[] instances = createClusterWithCount(1, config);
        IScheduledExecutorService service = instances[0].getScheduledExecutorService(schedulerName);
        String keyOwner = "hitSamePartitionToCheckCapacity";

        List<IScheduledFuture<Double>> futures = new ArrayList<>();
        for (int i = 0; i < capacity; i++) {
            Runnable command = autoDisposable(new PlainRunnableTask());
            IScheduledFuture<Double> future = service.scheduleOnKeyOwner(command, keyOwner, 0, SECONDS);
            futures.add(future);
        }

        futures.forEach(this::assertTaskHasBeenDestroyedEventually);

        for (int i = 0; i < capacity; i++) {
            service.scheduleOnKeyOwner(autoDisposable(new PlainRunnableTask()), keyOwner, 0, TimeUnit.SECONDS);
        }

        // no exceptions thrown
    }


    protected void assertCapacityReached(IScheduledExecutorService service, String key, String expectedError) {
        try {
            if (key == null) {
                service.schedule(new PlainCallableTask(), 0, TimeUnit.SECONDS);
            } else {
                service.scheduleOnKeyOwner(new PlainCallableTask(), key, 0, TimeUnit.SECONDS);
            }
            fail("Should have been rejected.");
        } catch (RejectedExecutionException ex) {
            assertEquals("Got wrong RejectedExecutionException", expectedError
                            + " Reminder, that tasks must be disposed if not needed.",
                    ex.getMessage());
        }
    }

    @Test
    public void capacity_onMember_whenPositiveLimit() {
        String schedulerName = ANY_EXECUTOR_NAME;

        ScheduledExecutorConfig sec = new ScheduledExecutorConfig()
                .setName(schedulerName)
                .setDurability(1)
                .setPoolSize(1)
                .setCapacity(10);

        Config config = new Config().addScheduledExecutorConfig(sec);

        HazelcastInstance[] instances = createClusterWithCount(1, config);
        IScheduledExecutorService service = instances[0].getScheduledExecutorService(schedulerName);
        Member member = instances[0].getCluster().getLocalMember();

        List<IScheduledFuture> futures = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            futures.add(service.scheduleOnMember(new PlainCallableTask(), member, 0, TimeUnit.SECONDS));
        }

        try {
            service.scheduleOnMember(new PlainCallableTask(), member, 0, TimeUnit.SECONDS);
            fail("Should have been rejected.");
        } catch (RejectedExecutionException ex) {
            assertEquals("Got wrong RejectedExecutionException",
                    "Maximum capacity (10) of tasks reached for this member and scheduled executor (" + schedulerName + "). "
                            + "Reminder, that tasks must be disposed if not needed.", ex.getMessage());
        }

        // Dispose all
        for (IScheduledFuture future : futures) {
            future.dispose();
        }

        // Re-schedule to verify capacity
        for (int i = 0; i < 10; i++) {
            service.scheduleOnMember(new PlainCallableTask(), member, 0, TimeUnit.SECONDS);
        }

        try {
            service.scheduleOnMember(new PlainCallableTask(), member, 0, TimeUnit.SECONDS);
            fail("Should have been rejected.");
        } catch (RejectedExecutionException ex) {
            assertEquals("Got wrong RejectedExecutionException",
                    "Maximum capacity (10) of tasks reached for this member and scheduled executor (" + schedulerName + "). "
                            + "Reminder, that tasks must be disposed if not needed.", ex.getMessage());
        }
    }

    @Test
    public void capacity_onMember_whenPositiveLimit_perPartition_shouldNotReject() {
        String schedulerName = ANY_EXECUTOR_NAME;

        ScheduledExecutorConfig sec = new ScheduledExecutorConfig()
                .setName(schedulerName)
                .setDurability(1)
                .setPoolSize(1)
                .setCapacity(10)
                .setCapacityPolicy(ScheduledExecutorConfig.CapacityPolicy.PER_PARTITION);

        Config config = new Config().addScheduledExecutorConfig(sec);

        HazelcastInstance[] instances = createClusterWithCount(1, config);
        IScheduledExecutorService service = instances[0].getScheduledExecutorService(schedulerName);
        Member member = instances[0].getCluster().getLocalMember();

        for (int i = 0; i < 10; i++) {
            service.scheduleOnMember(new PlainCallableTask(), member, 0, TimeUnit.SECONDS);
        }

        // Should pass - PER_PARTITION policy disables MEMBER OWNED capacity checking
        service.scheduleOnMember(new PlainCallableTask(), member, 0, TimeUnit.SECONDS);
    }

    @Test
    public void handlerTaskAndSchedulerNames_withCallable() throws Exception {
        int delay = 0;
        String schedulerName = ANY_EXECUTOR_NAME;
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
        String schedulerName = ANY_EXECUTOR_NAME;
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
        IScheduledExecutorService executorService = getScheduledExecutor(instances, ANY_EXECUTOR_NAME);
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
        IScheduledExecutorService executorService = getScheduledExecutor(instances, ANY_EXECUTOR_NAME);
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
        IScheduledExecutorService executorService = getScheduledExecutor(instances, ANY_EXECUTOR_NAME);
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

        IScheduledExecutorService executorService = getScheduledExecutor(instances, ANY_EXECUTOR_NAME);
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
        map.put("foo", 1);

        Object key = generateKeyOwnedBy(instances[0]);
        ICountDownLatch startedLatch = instances[1].getCPSubsystem().getCountDownLatch("startedLatch");
        ICountDownLatch finishedLatch = instances[1].getCPSubsystem().getCountDownLatch("finishedLatch");
        ICountDownLatch waitAfterStartLatch = instances[1].getCPSubsystem().getCountDownLatch("waitAfterStartLatch");
        startedLatch.trySetCount(1);
        finishedLatch.trySetCount(1);
        waitAfterStartLatch.trySetCount(1);

        IAtomicLong runEntryCounter = instances[1].getCPSubsystem().getAtomicLong("runEntryCounterName");

        IScheduledExecutorService executorService = getScheduledExecutor(instances, ANY_EXECUTOR_NAME);
        executorService.scheduleOnKeyOwner(new ICountdownLatchMapIncrementCallableTask("map",
                "runEntryCounterName", "startedLatch", "finishedLatch", "waitAfterStartLatch"), key, 0, SECONDS);

        assertOpenEventually(startedLatch);
        instances[0].getLifecycleService().shutdown();

        waitAfterStartLatch.countDown();
        assertOpenEventually(finishedLatch);

        assertEquals(2, (int) map.get("foo"));

        assertEquals(2, runEntryCounter.get());
    }

    @Test
    public void schedule_withLongSleepingCallable_cancelledAndGet() {
        int delay = 0;

        HazelcastInstance[] instances = createClusterWithCount(2);
        ICountDownLatch initCountLatch = instances[0].getCPSubsystem().getCountDownLatch("initCountLatchName");
        initCountLatch.trySetCount(1);
        ICountDownLatch waitCountLatch = instances[0].getCPSubsystem().getCountDownLatch("waitCountLatchName");
        waitCountLatch.trySetCount(1);
        ICountDownLatch doneCountLatch = instances[0].getCPSubsystem().getCountDownLatch("doneCountLatchName");
        doneCountLatch.trySetCount(1);

        IScheduledExecutorService executorService = getScheduledExecutor(instances, ANY_EXECUTOR_NAME);
        IScheduledFuture<Double> future = executorService.schedule(
                new ICountdownLatchCallableTask(initCountLatch.getName(), waitCountLatch.getName(), doneCountLatch.getName()),
                delay, SECONDS);

        assertOpenEventually(initCountLatch);

        assertTrue(future.cancel(false));
        waitCountLatch.countDown();

        assertOpenEventually(doneCountLatch);

        assertTrue(future.isDone());
        assertTrue(future.isCancelled());
    }

    @Test
    public void schedule_withNegativeDelay() throws Exception {
        int delay = -2;
        double expectedResult = 25.0;

        HazelcastInstance[] instances = createClusterWithCount(2);
        IScheduledExecutorService executorService = getScheduledExecutor(instances, ANY_EXECUTOR_NAME);
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
        IScheduledExecutorService executorService = getScheduledExecutor(instances, ANY_EXECUTOR_NAME);
        executorService.schedule(named(taskName, new PlainCallableTask()), delay, SECONDS);

        executorService.schedule(named(taskName, new PlainCallableTask()), delay, SECONDS);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void schedule_thenCancelInterrupted() {
        int delay = 1;
        String taskName = "Test";

        HazelcastInstance[] instances = createClusterWithCount(2);
        IScheduledExecutorService executorService = getScheduledExecutor(instances, ANY_EXECUTOR_NAME);
        IScheduledFuture<Double> first = executorService.schedule(named(taskName, new PlainCallableTask()), delay, MINUTES);

        first.cancel(true);
    }

    @Test(expected = CancellationException.class)
    public void schedule_thenCancelAndGet() throws Exception {
        int delay = 1;
        String taskName = "Test";

        HazelcastInstance[] instances = createClusterWithCount(2);
        IScheduledExecutorService executorService = getScheduledExecutor(instances, ANY_EXECUTOR_NAME);
        IScheduledFuture<Double> first = executorService.schedule(named(taskName, new PlainCallableTask()), delay, MINUTES);

        first.cancel(false);
        first.get();
    }

    @Test
    public void schedule_whenAutoDisposable_thenGet() throws Exception {
        HazelcastInstance[] instances = createClusterWithCount(2);
        IScheduledExecutorService executorService = getScheduledExecutor(instances, ANY_EXECUTOR_NAME);

        IScheduledFuture<Double> future = executorService.schedule(autoDisposable(new PlainCallableTask()), 1, SECONDS);

        assertTaskHasBeenDestroyedEventually(future);
    }


    @Test
    public void scheduleOnMember_whenAutoDisposable_thenGet() throws Exception {
        HazelcastInstance[] instances = createClusterWithCount(2);
        Member localMember = instances[0].getCluster().getLocalMember();
        IScheduledExecutorService executorService = getScheduledExecutor(instances, ANY_EXECUTOR_NAME);

        IScheduledFuture<Double> future = executorService.scheduleOnMember(
                autoDisposable(new PlainCallableTask()), localMember, 1, SECONDS);

        assertTaskHasBeenDestroyedEventually(future);
    }

    @Test
    public void scheduleOnKeyOwner_whenAutoDisposable_thenGet() throws Exception {
        HazelcastInstance[] instances = createClusterWithCount(2);
        IScheduledExecutorService executorService = getScheduledExecutor(instances, ANY_EXECUTOR_NAME);
        String key = generateKeyOwnedBy(instances[1]);

        IScheduledFuture<Double> future = executorService.scheduleOnKeyOwner(autoDisposable(new PlainCallableTask()), key, 1, SECONDS);

        assertTaskHasBeenDestroyedEventually(future);
    }

    private void assertTaskHasBeenDestroyedEventually(IScheduledFuture<Double> future) {
        assertTrueEventually(() -> assertThrows(StaleTaskException.class, () -> {
            try {
                future.get();
            } catch (InterruptedException ignored) {
                // ignored
            } catch (ExecutionException ex) {
                sneakyThrow(ex.getCause());
            }
        }), 2);
    }

    @Test(expected = TimeoutException.class)
    public void schedule_thenGetWithTimeout() throws Exception {
        int delay = 5;
        String taskName = "Test";

        HazelcastInstance[] instances = createClusterWithCount(2);
        IScheduledExecutorService executorService = getScheduledExecutor(instances, ANY_EXECUTOR_NAME);
        IScheduledFuture<Double> first = executorService.schedule(named(taskName, new PlainCallableTask()), delay, MINUTES);

        first.get(2, TimeUnit.SECONDS);
    }

    @Test
    public void schedule_getDelay() {
        int delay = 20;
        String taskName = "Test";

        HazelcastInstance[] instances = createClusterWithCount(2);
        IScheduledExecutorService executorService = getScheduledExecutor(instances, ANY_EXECUTOR_NAME);
        IScheduledFuture<Double> first = executorService.schedule(named(taskName, new PlainCallableTask()), delay, MINUTES);

        assertEquals(19, first.getDelay(MINUTES));
    }

    @Test
    public void scheduleOnKeyOwner_getDelay() {
        int delay = 20;
        String taskName = "Test";

        HazelcastInstance[] instances = createClusterWithCount(2);
        Object key = generateKeyOwnedBy(instances[1]);

        IScheduledExecutorService executorService = getScheduledExecutor(instances, ANY_EXECUTOR_NAME);
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

        IScheduledExecutorService executorService = getScheduledExecutor(instances, ANY_EXECUTOR_NAME);
        IScheduledFuture<Double> first = executorService.scheduleOnMember(
                named(taskName, new PlainCallableTask()), localMember, delay, MINUTES);

        assertEquals(19, first.getDelay(MINUTES));
    }

    @Test
    public void schedule_andCancel() {
        HazelcastInstance[] instances = createClusterWithCount(2);

        ICountDownLatch latch = instances[0].getCPSubsystem().getCountDownLatch("latch");
        latch.trySetCount(1);

        IScheduledExecutorService executorService = getScheduledExecutor(instances, ANY_EXECUTOR_NAME);
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

        IScheduledExecutorService executorService = getScheduledExecutor(instances, ANY_EXECUTOR_NAME);
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

        IScheduledExecutorService executorService = getScheduledExecutor(instances, ANY_EXECUTOR_NAME);
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

        IScheduledExecutorService executorService = getScheduledExecutor(instances, ANY_EXECUTOR_NAME);
        IScheduledFuture<Double> first = executorService.schedule(new PlainCallableTask(), 1, MINUTES);
        IScheduledFuture<Double> second = executorService.schedule(new PlainCallableTask(), 2, MINUTES);

        assertEquals(-1, first.compareTo(second));
    }

    @Test(expected = StaleTaskException.class)
    public void schedule_thenDisposeThenGet() throws Exception {
        int delay = 1;
        String taskName = "Test";

        HazelcastInstance[] instances = createClusterWithCount(2);
        IScheduledExecutorService executorService = getScheduledExecutor(instances, ANY_EXECUTOR_NAME);
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
        IScheduledExecutorService executorService = getScheduledExecutor(instances, ANY_EXECUTOR_NAME);
        IScheduledFuture<Double> first = executorService.scheduleOnMember(
                named(taskName, new PlainCallableTask()), localMember, delay, SECONDS);

        first.dispose();
        first.get();
    }

    @Test(expected = RejectedExecutionException.class)
    public void schedule_whenShutdown() {
        int delay = 1;

        HazelcastInstance[] instances = createClusterWithCount(2);
        IScheduledExecutorService executorService = getScheduledExecutor(instances, ANY_EXECUTOR_NAME);
        executorService.schedule(new PlainCallableTask(), delay, SECONDS);
        executorService.shutdown();

        executorService.schedule(new PlainCallableTask(), delay, SECONDS);
    }

    public void schedule_testPartitionLostEvent(int replicaLostCount) {
        int delay = 1;

        HazelcastInstance[] instances = createClusterWithCount(1);
        IScheduledExecutorService executorService = getScheduledExecutor(instances, ANY_EXECUTOR_NAME);
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

        IScheduledExecutorService executorService = getScheduledExecutor(instances, ANY_EXECUTOR_NAME);
        final IScheduledFuture future = executorService.scheduleOnMember(new PlainCallableTask(), member, delay, SECONDS);

        instances[1].getLifecycleService().terminate();

        assertTrueEventually(() -> {
            try {
                future.get(0, SECONDS);
                fail();
            } catch (IllegalStateException ex) {
                System.err.println(ex.getMessage());
                assertEquals(format("Member with address: %s,  holding this scheduled task is not part of this cluster.",
                        future.getHandler().getUuid()), ex.getMessage());
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
        IScheduledExecutorService executorService = getScheduledExecutor(instances, ANY_EXECUTOR_NAME);
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
        IScheduledExecutorService executorService = getScheduledExecutor(instances, ANY_EXECUTOR_NAME);
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

        IScheduledExecutorService executorService = getScheduledExecutor(instances, ANY_EXECUTOR_NAME);
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
        IScheduledExecutorService executorService = getScheduledExecutor(instances, ANY_EXECUTOR_NAME);

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

        IScheduledExecutorService s = getScheduledExecutor(instances, ANY_EXECUTOR_NAME);
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
        IScheduledExecutorService s = getScheduledExecutor(instances, ANY_EXECUTOR_NAME);
        s.scheduleOnMember(TaskUtils.named("blah", new PlainInstanceAwareRunnableTask("latch")), member, 1, TimeUnit.SECONDS);

        assertOpenEventually(latch);
        assertEquals(0, latch.getCount());
    }

    @Test
    public void scheduleWithRepetition() {
        HazelcastInstance[] instances = createClusterWithCount(2);

        IScheduledExecutorService s = getScheduledExecutor(instances, ANY_EXECUTOR_NAME);

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
        IScheduledExecutorService executorService = getScheduledExecutor(instances, ANY_EXECUTOR_NAME);

        MemberImpl member = getNodeEngineImpl(instances[0]).getLocalMember();
        IScheduledFuture<Double> future = executorService.scheduleOnMember(new PlainCallableTask(), member, delay, SECONDS);

        assertTrue(future.getHandler().isAssignedToMember());
        assertEquals(25.0, future.get(), 0);
    }

    @Test
    public void scheduleOnMemberWithRepetition() {
        HazelcastInstance[] instances = createClusterWithCount(4);
        IScheduledExecutorService s = getScheduledExecutor(instances, ANY_EXECUTOR_NAME);

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
        IScheduledExecutorService executorService = getScheduledExecutor(instances, ANY_EXECUTOR_NAME);
        String key = generateKeyOwnedBy(instances[1]);

        IScheduledFuture<Double> future = executorService.scheduleOnKeyOwner(new PlainCallableTask(), key, 2, SECONDS);

        assertEquals(25.0, future.get(), 0.0);
    }

    @Test
    public void scheduleOnKeyOwner_withNotPeriodicRunnable() throws Exception {
        HazelcastInstance[] instances = createClusterWithCount(2);

        String key = generateKeyOwnedBy(instances[0]);
        IScheduledExecutorService s = getScheduledExecutor(instances, ANY_EXECUTOR_NAME);

        ICountDownLatch latch = instances[0].getCPSubsystem().getCountDownLatch("latch");
        latch.trySetCount(1);

        s.scheduleOnKeyOwner(new ICountdownLatchRunnableTask("latch"), key, 2, SECONDS).get();
        assertEquals(0, latch.getCount());
    }

    @Test
    public void scheduleOnKeyOwner_withNotPeriodicRunnableDurable() throws Exception {
        HazelcastInstance[] instances = createClusterWithCount(2);

        String key = generateKeyOwnedBy(instances[1]);
        IScheduledExecutorService s = getScheduledExecutor(instances, ANY_EXECUTOR_NAME);

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
        IScheduledExecutorService executorService = getScheduledExecutor(instances, ANY_EXECUTOR_NAME);
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
        IScheduledExecutorService executorService = getScheduledExecutor(instances, ANY_EXECUTOR_NAME);

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
    public void getScheduled_whenTaskDecoratedWithNamedTask() {
        int delay = 1;
        String taskName = "Test";
        PlainCallableTask task = new PlainCallableTask();

        HazelcastInstance[] instances = createClusterWithCount(2);
        IScheduledExecutorService executorService = getScheduledExecutor(instances, ANY_EXECUTOR_NAME);
        IScheduledFuture<Double> first = executorService.schedule(named(taskName, task), delay, SECONDS);

        ScheduledTaskHandler handler = first.getHandler();
        IScheduledFuture<Double> copy = executorService.getScheduledFuture(handler);

        assertEquals(first.getHandler(), copy.getHandler());
        assertEquals(copy.getHandler().getTaskName(), taskName);
    }

    @Test
    public void getScheduled_whenTaskImplementingNamedTask() {
        int delay = 1;
        String taskName = NamedCallable.NAME;
        NamedCallable task = new NamedCallable();

        HazelcastInstance[] instances = createClusterWithCount(2);
        IScheduledExecutorService executorService = getScheduledExecutor(instances, ANY_EXECUTOR_NAME);
        IScheduledFuture<Boolean> first = executorService.schedule(task, delay, SECONDS);

        ScheduledTaskHandler handler = first.getHandler();
        IScheduledFuture<Double> copy = executorService.getScheduledFuture(handler);

        assertEquals(first.getHandler(), copy.getHandler());
        assertEquals(copy.getHandler().getTaskName(), taskName);
    }

    @Test
    public void scheduleOnAllMembers_getAllScheduled() throws Exception {
        HazelcastInstance[] instances = createClusterWithCount(3);
        IScheduledExecutorService s = getScheduledExecutor(instances, ANY_EXECUTOR_NAME);
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
        IScheduledExecutorService s = getScheduledExecutor(instances, ANY_EXECUTOR_NAME);

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
        IScheduledExecutorService executorService = getScheduledExecutor(instances, ANY_EXECUTOR_NAME);
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
        IScheduledExecutorService executorService = getScheduledExecutor(instances, ANY_EXECUTOR_NAME);
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
        IScheduledExecutorService s = instance.getScheduledExecutorService(ANY_EXECUTOR_NAME);
        s.schedule(new PlainCallableTask(), 0, SECONDS);
    }

    @Test
    public void scheduled_executor_collects_statistics_when_stats_enabled() throws Exception {
        // run task
        Config config = smallInstanceConfig();
        config.getScheduledExecutorConfig(ANY_EXECUTOR_NAME)
                .setStatisticsEnabled(true);

        HazelcastInstance[] instances = createClusterWithCount(2, config);
        IScheduledExecutorService s = getScheduledExecutor(instances, ANY_EXECUTOR_NAME);
        Map<Member, IScheduledFuture<Double>> futureMap = s.scheduleOnAllMembers(new OneSecondSleepingTask(), 0, SECONDS);
        Set<Map.Entry<Member, IScheduledFuture<Double>>> entries = futureMap.entrySet();
        for (Map.Entry<Member, IScheduledFuture<Double>> entry : entries) {
            entry.getValue().get();
        }

        assertTrueEventually(() -> {
            // collect metrics
            Map<String, List<Long>> metrics = collectMetrics(SCHEDULED_EXECUTOR_PREFIX, instances);

            // check results
            assertMetricsCollected(metrics, 1000, 0,
                1, 1, 0, 1, 0);
        });
    }

    @Test
    public void scheduled_executor_does_not_collect_statistics_when_stats_disabled() throws Exception {
        // run task
        Config config = smallInstanceConfig();
        config.getScheduledExecutorConfig(ANY_EXECUTOR_NAME)
                .setStatisticsEnabled(false);

        HazelcastInstance[] instances = createClusterWithCount(2, config);
        IScheduledExecutorService s = getScheduledExecutor(instances, ANY_EXECUTOR_NAME);
        Map<Member, IScheduledFuture<Double>> futureMap = s.scheduleOnAllMembers(new OneSecondSleepingTask(), 0, SECONDS);
        Set<Map.Entry<Member, IScheduledFuture<Double>>> entries = futureMap.entrySet();
        for (Map.Entry<Member, IScheduledFuture<Double>> entry : entries) {
            entry.getValue().get();
        }

        assertTrueAllTheTime(() -> {
            // collect metrics
            Map<String, List<Long>> metrics = collectMetrics(SCHEDULED_EXECUTOR_PREFIX, instances);

            // check results
            assertTrue("No metrics collection expected but " + metrics, metrics.isEmpty());
        }, 5);
    }

    @Test
    public void scheduledAtFixedRate_generates_statistics_when_stats_enabled() {
        // run task
        Config config = smallInstanceConfig();
        config.getScheduledExecutorConfig(ANY_EXECUTOR_NAME)
                .setStatisticsEnabled(true);

        long now = System.currentTimeMillis();
        HazelcastInstance[] instances = createClusterWithCount(1, config);
        IScheduledExecutorService s = getScheduledExecutor(instances, ANY_EXECUTOR_NAME);
        CountDownLatch progress = new CountDownLatch(3);
        Semaphore suspend = new Semaphore(0);
        s.scheduleAtFixedRate(new CountableRunTask(progress, suspend), 1, 1, SECONDS);

        assertOpenEventually(progress);

        try {
            assertTrueEventually(() -> {
                // collect metrics
                Map<String, List<Long>> metrics = collectMetrics(SCHEDULED_EXECUTOR_PREFIX, instances);

                // check results
                assertMetricsCollected(metrics, 0, 0,
                    3, 2, 0, now, 0);
            });
        } finally {
            suspend.release();
        }
    }

    @Test
    public void scheduledAtFixedRate_does_not_generate_statistics_when_stats_disabled() {
        // run task
        Config config = smallInstanceConfig();
        config.getScheduledExecutorConfig(ANY_EXECUTOR_NAME)
                .setStatisticsEnabled(false);

        long now = System.currentTimeMillis();
        HazelcastInstance[] instances = createClusterWithCount(1, config);
        IScheduledExecutorService s = getScheduledExecutor(instances, ANY_EXECUTOR_NAME);
        CountDownLatch progress = new CountDownLatch(3);
        Semaphore suspend = new Semaphore(0);
        s.scheduleAtFixedRate(new CountableRunTask(progress, suspend), 1, 1, SECONDS);

        assertOpenEventually(progress);

        assertTrueAllTheTime(() -> {
            // collect metrics
            Map<String, List<Long>> metrics = collectMetrics(SCHEDULED_EXECUTOR_PREFIX, instances);

            // check results
            assertTrue("No metrics collection expected but " + metrics, metrics.isEmpty());
        }, 5);
    }


    public static Map<String, List<Long>> collectMetrics(String prefix, HazelcastInstance... instances) {
        Map<String, List<Long>> metricsMap = new HashMap<>();

        for (HazelcastInstance instance : instances) {
            Accessors.getMetricsRegistry(instance).collect(new MetricsCollector() {
                @Override
                public void collectLong(MetricDescriptor descriptor, long value) {
                    if (prefix.equals(descriptor.prefix())) {
                        metricsMap.compute(descriptor.metric(), (metricName, values) -> {
                            if (values == null) {
                                values = new ArrayList<>();
                            }
                            values.add(value);
                            return values;
                        });
                    }
                }

                @Override
                public void collectDouble(MetricDescriptor descriptor, double value) {
                }

                @Override
                public void collectException(MetricDescriptor descriptor, Exception e) {
                }

                @Override
                public void collectNoValue(MetricDescriptor descriptor) {
                }
            });
        }
        return metricsMap;
    }

    public static void assertMetricsCollected(Map<String, List<Long>> metricsMap,
                                              long expectedTotalExecutionTime,
                                              long expectedPending,
                                              long expectedStarted,
                                              long expectedCompleted,
                                              long expectedCancelled,
                                              long expectedCreationTime,
                                              long expectedTotalStartLatency) {

        List<Long> totalExecutionTimes = metricsMap.get(EXECUTOR_METRIC_TOTAL_EXECUTION_TIME);
        for (long totalExecutionTime : totalExecutionTimes) {
            assertGreaterOrEquals(EXECUTOR_METRIC_TOTAL_EXECUTION_TIME + "::" + metricsMap,
                    totalExecutionTime, expectedTotalExecutionTime);
        }

        List<Long> pendingCount = metricsMap.get(EXECUTOR_METRIC_PENDING);
        for (long pending : pendingCount) {
            assertEquals(EXECUTOR_METRIC_PENDING, expectedPending, pending);
        }

        List<Long> startedCount = metricsMap.get(EXECUTOR_METRIC_STARTED);
        for (long started : startedCount) {
            assertEquals(EXECUTOR_METRIC_STARTED, expectedStarted, started);
        }

        List<Long> completedCount = metricsMap.get(EXECUTOR_METRIC_COMPLETED);
        for (long completed : completedCount) {
            assertEquals(EXECUTOR_METRIC_COMPLETED, expectedCompleted, completed);
        }

        List<Long> cancelledCount = metricsMap.get(EXECUTOR_METRIC_CANCELLED);
        for (long cancelled : cancelledCount) {
            assertEquals(EXECUTOR_METRIC_CANCELLED, expectedCancelled, cancelled);
        }

        List<Long> creationTimes = metricsMap.get(EXECUTOR_METRIC_CREATION_TIME);
        for (long creationTime : creationTimes) {
            assertGreaterOrEquals(EXECUTOR_METRIC_CREATION_TIME,
                    creationTime, expectedCreationTime);
        }

        List<Long> totalStartLatencies = metricsMap.get(EXECUTOR_METRIC_TOTAL_START_LATENCY);
        for (long totalStartLatency : totalStartLatencies) {
            assertGreaterOrEquals(EXECUTOR_METRIC_TOTAL_START_LATENCY,
                    totalStartLatency, expectedTotalStartLatency);
        }
    }
}
