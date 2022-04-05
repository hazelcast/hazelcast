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

package com.hazelcast.durableexecutor;

import com.hazelcast.cluster.Member;
import com.hazelcast.config.Config;
import com.hazelcast.config.DurableExecutorConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.cp.IAtomicLong;
import com.hazelcast.cp.ICountDownLatch;
import com.hazelcast.durableexecutor.impl.DistributedDurableExecutorService;
import com.hazelcast.durableexecutor.impl.DurableExecutorContainer;
import com.hazelcast.executor.ExecutorServiceTestSupport;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.partition.PartitionAware;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static com.hazelcast.durableexecutor.impl.DurableExecutorServiceHelper.getDurableExecutorContainer;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.DURABLE_EXECUTOR_PREFIX;
import static com.hazelcast.scheduledexecutor.impl.ScheduledExecutorServiceBasicTest.assertMetricsCollected;
import static com.hazelcast.scheduledexecutor.impl.ScheduledExecutorServiceBasicTest.collectMetrics;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DurableExecutorServiceTest extends ExecutorServiceTestSupport {

    private static final int NODE_COUNT = 3;
    private static final int TASK_COUNT = 1000;

    @Test(expected = UnsupportedOperationException.class)
    public void testInvokeAll() throws Exception {
        HazelcastInstance instance = createHazelcastInstance(smallInstanceConfig());
        DurableExecutorService service = instance.getDurableExecutorService(randomString());
        List<BasicTestCallable> callables = Collections.emptyList();
        service.invokeAll(callables);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testInvokeAll_WithTimeout() throws Exception {
        HazelcastInstance instance = createHazelcastInstance(smallInstanceConfig());
        DurableExecutorService service = instance.getDurableExecutorService(randomString());
        List<BasicTestCallable> callables = Collections.emptyList();
        service.invokeAll(callables, 1, TimeUnit.SECONDS);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testInvokeAny() throws Exception {
        HazelcastInstance instance = createHazelcastInstance(smallInstanceConfig());
        DurableExecutorService service = instance.getDurableExecutorService(randomString());
        List<BasicTestCallable> callables = Collections.emptyList();
        service.invokeAny(callables);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testInvokeAny_WithTimeout() throws Exception {
        HazelcastInstance instance = createHazelcastInstance(smallInstanceConfig());
        DurableExecutorService service = instance.getDurableExecutorService(randomString());
        List<BasicTestCallable> callables = Collections.emptyList();
        service.invokeAny(callables, 1, TimeUnit.SECONDS);
    }

    @Test
    public void testDestroyCleansAllContainers() throws Exception {
        String executorName = randomMapName();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(NODE_COUNT);
        HazelcastInstance[] instances = factory.newInstances(smallInstanceConfig());
        DurableExecutorService service = instances[0].getDurableExecutorService(executorName);
        InternalPartitionService partitionService = getNodeEngineImpl(instances[0]).getPartitionService();

        List<Future<String>> futures = new ArrayList<>(TASK_COUNT);
        for (int i = 0; i < TASK_COUNT; i++) {
            futures.add(service.submit(new DummyCallable()));
        }
        for (Future<String> future : futures) {
            future.get();
        }

        // wait for all backup operations to complete
        assertTrueEventually(() -> {
            for (HazelcastInstance instance : instances) {
                NodeEngineImpl ne = getNodeEngineImpl(instance);
                DistributedDurableExecutorService internalService = ne.getService(DistributedDurableExecutorService.SERVICE_NAME);
                for (int partitionId = 0; partitionId < partitionService.getPartitionCount(); partitionId++) {
                    DurableExecutorContainer container = getDurableExecutorContainer(internalService, partitionId, executorName);
                    if (container != null) {
                        assertEquals(0, container.getRingBuffer().getTaskSize());
                    }
                }
            }
        });

        service.destroy();
        assertTrueEventually(() -> {
            for (HazelcastInstance instance : instances) {
                NodeEngineImpl ne = getNodeEngineImpl(instance);
                DistributedDurableExecutorService internalService = ne.getService(DistributedDurableExecutorService.SERVICE_NAME);

                boolean allEmpty = true;
                StringBuilder failMessage = new StringBuilder();
                for (int partitionId = 0; partitionId < partitionService.getPartitionCount(); partitionId++) {
                    DurableExecutorContainer container = getDurableExecutorContainer(internalService, partitionId, executorName);
                    if (container != null) {
                        failMessage.append(String.format("Partition %d owned by %s on %s\n",
                                partitionId, partitionService.getPartition(partitionId).getOwnerOrNull(), instance));
                        allEmpty = false;
                    }
                }
                assertTrue(String.format("Some partitions have non-null containers for executor %s:\n%s",
                        executorName,
                        failMessage.toString()), allEmpty);
            }
        }, 30);
    }

    @Test
    public void testAwaitTermination() throws Exception {
        HazelcastInstance instance = createHazelcastInstance(smallInstanceConfig());
        DurableExecutorService service = instance.getDurableExecutorService(randomString());
        assertFalse(service.awaitTermination(1, TimeUnit.SECONDS));
    }

    @Test
    public void testFullRingBuffer() throws Exception {
        String name = randomString();
        String key = randomString();
        Config config = smallInstanceConfig();
        config.getDurableExecutorConfig(name).setCapacity(1);
        HazelcastInstance instance = createHazelcastInstance(config);
        DurableExecutorService service = instance.getDurableExecutorService(name);
        service.submitToKeyOwner(new SleepingTask(100), key);
        DurableExecutorServiceFuture<String> future = service.submitToKeyOwner(new BasicTestCallable(), key);
        try {
            future.get();
            fail();
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof RejectedExecutionException);
        }
    }

    @Test
    public void test_registerCallback_beforeFutureIsCompletedOnOtherNode() throws Exception {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance1 = factory.newHazelcastInstance(smallInstanceConfig());
        HazelcastInstance instance2 = factory.newHazelcastInstance(smallInstanceConfig());

        assertTrue(instance1.getCPSubsystem().getCountDownLatch("latch").trySetCount(1));

        String name = randomString();
        DurableExecutorService executorService = instance2.getDurableExecutorService(name);
        ICountDownLatchAwaitCallable task = new ICountDownLatchAwaitCallable("latch");
        String key = generateKeyOwnedBy(instance1);
        DurableExecutorServiceFuture<Boolean> future = executorService.submitToKeyOwner(task, key);

        CountingDownExecutionCallback<Boolean> callback = new CountingDownExecutionCallback<>(1);
        future.whenCompleteAsync(callback);
        instance1.getCPSubsystem().getCountDownLatch("latch").countDown();

        assertTrue(future.get());
        assertOpenEventually(callback.getLatch());
    }

    @Test
    public void test_registerCallback_afterFutureIsCompletedOnOtherNode() throws Exception {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance1 = factory.newHazelcastInstance(smallInstanceConfig());
        HazelcastInstance instance2 = factory.newHazelcastInstance(smallInstanceConfig());
        String name = randomString();
        DurableExecutorService executorService = instance2.getDurableExecutorService(name);
        BasicTestCallable task = new BasicTestCallable();
        String key = generateKeyOwnedBy(instance1);
        DurableExecutorServiceFuture<String> future = executorService.submitToKeyOwner(task, key);
        assertEquals(BasicTestCallable.RESULT, future.get());

        CountingDownExecutionCallback<String> callback = new CountingDownExecutionCallback<>(1);
        future.whenCompleteAsync(callback);

        assertOpenEventually(callback.getLatch(), 10);
    }

    @Test
    public void test_registerCallback_multipleTimes_futureIsCompletedOnOtherNode() throws Exception {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance1 = factory.newHazelcastInstance(smallInstanceConfig());
        HazelcastInstance instance2 = factory.newHazelcastInstance(smallInstanceConfig());

        assertTrue(instance1.getCPSubsystem().getCountDownLatch("latch").trySetCount(1));

        String name = randomString();
        DurableExecutorService executorService = instance2.getDurableExecutorService(name);
        ICountDownLatchAwaitCallable task = new ICountDownLatchAwaitCallable("latch");
        String key = generateKeyOwnedBy(instance1);
        DurableExecutorServiceFuture<Boolean> future = executorService.submitToKeyOwner(task, key);

        CountDownLatch latch = new CountDownLatch(2);
        CountingDownExecutionCallback<Boolean> callback = new CountingDownExecutionCallback<>(latch);
        future.whenCompleteAsync(callback);
        future.whenCompleteAsync(callback);
        instance1.getCPSubsystem().getCountDownLatch("latch").countDown();

        assertTrue(future.get());
        assertOpenEventually(latch, 10);
    }

    @Test
    public void testSubmitFailingCallableException_withExecutionCallback() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        HazelcastInstance instance = factory.newHazelcastInstance(smallInstanceConfig());
        DurableExecutorService service = instance.getDurableExecutorService(randomString());

        CountingDownExecutionCallback<String> callback = new CountingDownExecutionCallback<>(1);
        service.submit(new FailingTestTask()).whenCompleteAsync(callback);

        assertOpenEventually(callback.getLatch());
        assertTrue(callback.getResult() instanceof Throwable);
    }

    /* ############ submit runnable ############ */

    @Test
    public void testManagedContextAndLocal() throws Exception {
        Config config = smallInstanceConfig();
        config.addDurableExecutorConfig(new DurableExecutorConfig("test").setPoolSize(1));
        final AtomicBoolean initialized = new AtomicBoolean();
        config.setManagedContext(obj -> {
            if (obj instanceof RunnableWithManagedContext) {
                initialized.set(true);
            }
            return obj;
        });

        HazelcastInstance instance = createHazelcastInstance(config);
        DurableExecutorService executor = instance.getDurableExecutorService("test");

        RunnableWithManagedContext task = new RunnableWithManagedContext();
        executor.submit(task).get();
        assertTrue("The task should have been initialized by the ManagedContext", initialized.get());
    }

    @Test
    public void testExecuteOnKeyOwner() throws Exception {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance1 = factory.newHazelcastInstance(smallInstanceConfig());
        HazelcastInstance instance2 = factory.newHazelcastInstance(smallInstanceConfig());
        String key = generateKeyOwnedBy(instance2);
        String instanceName = instance2.getName();
        ICountDownLatch latch = instance2.getCPSubsystem().getCountDownLatch(instanceName);
        latch.trySetCount(1);

        DurableExecutorService durableExecutorService = instance1.getDurableExecutorService(randomString());
        durableExecutorService.executeOnKeyOwner(new InstanceAsserterRunnable(instanceName), key);

        latch.await(30, TimeUnit.SECONDS);
    }

    @Test
    public void hazelcastInstanceAwareAndLocal() throws Exception {
        Config config = smallInstanceConfig();
        config.addDurableExecutorConfig(new DurableExecutorConfig("test").setPoolSize(1));
        HazelcastInstance instance = createHazelcastInstance(config);
        DurableExecutorService executor = instance.getDurableExecutorService("test");

        HazelcastInstanceAwareRunnable task = new HazelcastInstanceAwareRunnable();
        // if setHazelcastInstance() not called we expect a RuntimeException
        executor.submit(task).get();
    }

    @Test
    public void testExecuteMultipleNode() throws Exception {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(NODE_COUNT);
        HazelcastInstance[] instances = factory.newInstances(smallInstanceConfig());
        for (int i = 0; i < NODE_COUNT; i++) {
            DurableExecutorService service = instances[i].getDurableExecutorService("testExecuteMultipleNode");
            int rand = new Random().nextInt(100);
            Future<Integer> future = service.submit(new IncrementAtomicLongRunnable("count"), rand);
            assertEquals(Integer.valueOf(rand), future.get(10, TimeUnit.SECONDS));
        }

        IAtomicLong count = instances[0].getCPSubsystem().getAtomicLong("count");
        assertEquals(NODE_COUNT, count.get());
    }

    @Test
    public void testSubmitToKeyOwnerRunnable() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(NODE_COUNT);
        HazelcastInstance[] instances = factory.newInstances(smallInstanceConfig());
        final AtomicInteger nullResponseCount = new AtomicInteger(0);
        final CountDownLatch responseLatch = new CountDownLatch(NODE_COUNT);
        Consumer<Object> callback = response -> {
            if (response == null) {
                nullResponseCount.incrementAndGet();
            }
            responseLatch.countDown();
        };
        for (int i = 0; i < NODE_COUNT; i++) {
            HazelcastInstance instance = instances[i];
            DurableExecutorService service = instance.getDurableExecutorService("testSubmitToKeyOwnerRunnable");
            Member localMember = instance.getCluster().getLocalMember();
            UUID uuid = localMember.getUuid();
            Runnable runnable = new IncrementAtomicLongIfMemberUUIDNotMatchRunnable(uuid, "testSubmitToKeyOwnerRunnable");
            int key = findNextKeyForMember(instance, localMember);
            service.submitToKeyOwner(runnable, key).thenAccept(callback);
        }
        assertOpenEventually(responseLatch);
        assertEquals(0, instances[0].getCPSubsystem().getAtomicLong("testSubmitToKeyOwnerRunnable").get());
        assertEquals(NODE_COUNT, nullResponseCount.get());
    }

    /**
     * Submit a null task has to raise a NullPointerException.
     */
    @Test(expected = NullPointerException.class)
    @SuppressWarnings("ConstantConditions")
    public void submitNullTask() {
        DurableExecutorService executor = createSingleNodeDurableExecutorService("submitNullTask");
        executor.submit((Callable<?>) null);
    }

    /**
     * Run a basic task.
     */
    @Test
    public void testBasicTask() throws Exception {
        Callable<String> task = new BasicTestCallable();
        DurableExecutorService executor = createSingleNodeDurableExecutorService("testBasicTask");
        Future<String> future = executor.submit(task);
        assertEquals(future.get(), BasicTestCallable.RESULT);
    }

    @Test
    public void testSubmitMultipleNode() throws Exception {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(NODE_COUNT);
        HazelcastInstance[] instances = factory.newInstances(smallInstanceConfig());
        for (int i = 0; i < NODE_COUNT; i++) {
            DurableExecutorService service = instances[i].getDurableExecutorService("testSubmitMultipleNode");
            Future<Long> future = service.submit(new IncrementAtomicLongCallable("testSubmitMultipleNode"));
            assertEquals(i + 1, (long) future.get());
        }
    }

    /* ############ submit callable ############ */

    @Test
    public void testSubmitToKeyOwnerCallable() throws Exception {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(NODE_COUNT);
        HazelcastInstance[] instances = factory.newInstances(smallInstanceConfig());

        List<Future<Boolean>> futures = new ArrayList<>();
        for (int i = 0; i < NODE_COUNT; i++) {
            HazelcastInstance instance = instances[i];
            DurableExecutorService service = instance.getDurableExecutorService("testSubmitToKeyOwnerCallable");

            Member localMember = instance.getCluster().getLocalMember();
            int key = findNextKeyForMember(instance, localMember);
            Future<Boolean> future = service.submitToKeyOwner(new MemberUUIDCheckCallable(localMember.getUuid()), key);
            futures.add(future);
        }

        for (Future<Boolean> future : futures) {
            assertTrue(future.get(10, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testSubmitToKeyOwnerCallable_withCallback() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(NODE_COUNT);
        HazelcastInstance[] instances = factory.newInstances(smallInstanceConfig());
        BooleanSuccessResponseCountingCallback callback = new BooleanSuccessResponseCountingCallback(NODE_COUNT);

        for (int i = 0; i < NODE_COUNT; i++) {
            HazelcastInstance instance = instances[i];
            DurableExecutorService service = instance.getDurableExecutorService("testSubmitToKeyOwnerCallable");
            Member localMember = instance.getCluster().getLocalMember();
            int key = findNextKeyForMember(instance, localMember);
            service.submitToKeyOwner(new MemberUUIDCheckCallable(localMember.getUuid()), key).thenAccept(callback);
        }

        assertOpenEventually(callback.getResponseLatch());
        assertEquals(NODE_COUNT, callback.getSuccessResponseCount());
    }

    @Test
    public void testIsDoneMethod() throws Exception {
        Callable<String> task = new BasicTestCallable();
        DurableExecutorService executor = createSingleNodeDurableExecutorService("isDoneMethod");
        Future<String> future = executor.submit(task);
        assertResult(future, BasicTestCallable.RESULT);
    }

    /**
     * Repeatedly runs tasks and check for isDone() status after get().
     * Test for the issue 129.
     */
    @Test
    public void testIsDoneMethodAfterGet() throws Exception {
        DurableExecutorService executor = createSingleNodeDurableExecutorService("isDoneMethodAfterGet");
        for (int i = 0; i < TASK_COUNT; i++) {
            Callable<String> task1 = new BasicTestCallable();
            Callable<String> task2 = new BasicTestCallable();
            Future<String> future1 = executor.submit(task1);
            Future<String> future2 = executor.submit(task2);
            assertResult(future2, BasicTestCallable.RESULT);
            assertResult(future1, BasicTestCallable.RESULT);
        }
    }

    @Test
    public void testMultipleFutureGetInvocations() throws Exception {
        Callable<String> task = new BasicTestCallable();
        DurableExecutorService executor = createSingleNodeDurableExecutorService("isTwoGetFromFuture");
        Future<String> future = executor.submit(task);
        assertResult(future, BasicTestCallable.RESULT);
        assertResult(future, BasicTestCallable.RESULT);
        assertResult(future, BasicTestCallable.RESULT);
        assertResult(future, BasicTestCallable.RESULT);
    }

    /* ############ future ############ */

    private void assertResult(Future<?> future, Object expected) throws Exception {
        assertEquals(future.get(), expected);
        assertTrue(future.isDone());
    }

    @Test
    public void testIssue292() {
        CountingDownExecutionCallback<Member> callback = new CountingDownExecutionCallback<>(1);
        createSingleNodeDurableExecutorService("testIssue292").submit(new MemberCheck()).whenCompleteAsync(callback);
        assertOpenEventually(callback.getLatch());
        assertTrue(callback.getResult() instanceof Member);
    }

    /**
     * Execute a task that is executing something else inside (nested execution).
     */
    @Test
    public void testNestedExecution() {
        Callable<String> task = new NestedExecutorTask();
        DurableExecutorService executor = createSingleNodeDurableExecutorService("testNestedExecution");
        Future<?> future = executor.submit(task);
        assertCompletesEventually(future);
    }

    /**
     * Shutdown-related method behaviour when the cluster is running.
     */
    @Test
    public void testShutdownBehaviour() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance1 = factory.newHazelcastInstance(smallInstanceConfig());
        factory.newHazelcastInstance(smallInstanceConfig());
        DurableExecutorService executor = instance1.getDurableExecutorService("testShutdownBehaviour");
        // fresh instance, is not shutting down
        assertFalse(executor.isShutdown());
        assertFalse(executor.isTerminated());
        executor.shutdown();
        assertTrue(executor.isShutdown());
        assertTrue(executor.isTerminated());
        // shutdownNow() should return an empty list and be ignored
        List<Runnable> pending = executor.shutdownNow();
        assertTrue(pending.isEmpty());
        assertTrue(executor.isShutdown());
        assertTrue(executor.isTerminated());
        // awaitTermination() should return immediately false
        try {
            boolean terminated = executor.awaitTermination(60L, TimeUnit.SECONDS);
            assertFalse(terminated);
        } catch (InterruptedException ie) {
            fail("InterruptedException");
        }
        assertTrue(executor.isShutdown());
        assertTrue(executor.isTerminated());
    }

    /**
     * Shutting down the cluster should act as the ExecutorService shutdown.
     */
    @Test(expected = RejectedExecutionException.class)
    public void testClusterShutdown() {
        ExecutorService executor = createSingleNodeDurableExecutorService("testClusterShutdown");
        shutdownNodeFactory();
        sleepSeconds(2);

        assertNotNull(executor);
        assertTrue(executor.isShutdown());
        assertTrue(executor.isTerminated());

        // new tasks must be rejected
        Callable<String> task = new BasicTestCallable();
        executor.submit(task);
    }

    @Test
    public void testStatsIssue2039() {
        Config config = smallInstanceConfig();
        String name = "testStatsIssue2039";
        config.addDurableExecutorConfig(new DurableExecutorConfig(name).setPoolSize(1).setCapacity(1));
        HazelcastInstance instance = createHazelcastInstance(config);
        DurableExecutorService executorService = instance.getDurableExecutorService(name);

        executorService.execute(new SleepLatchRunnable());
        assertOpenEventually(SleepLatchRunnable.startLatch, 30);

        Future<?> rejected = executorService.submit(new EmptyRunnable());

        try {
            rejected.get(1, TimeUnit.MINUTES);
        } catch (Exception e) {
            boolean isRejected = e.getCause() instanceof RejectedExecutionException;
            if (!isRejected) {
                fail(e.toString());
            }
        } finally {
            SleepLatchRunnable.sleepLatch.countDown();
        }

        // FIXME as soon as executorService.getLocalExecutorStats() is implemented
        //LocalExecutorStats stats = executorService.getLocalExecutorStats();
        //assertEquals(2, stats.getStartedTaskCount());
        //assertEquals(0, stats.getPendingTaskCount());
    }

    @Test
    public void testLongRunningCallable() throws Exception {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);

        Config config = smallInstanceConfig();
        long callTimeoutMillis = 3000;
        config.setProperty(ClusterProperty.OPERATION_CALL_TIMEOUT_MILLIS.getName(), String.valueOf(callTimeoutMillis));

        HazelcastInstance hz1 = factory.newHazelcastInstance(config);
        HazelcastInstance hz2 = factory.newHazelcastInstance(config);
        String key = generateKeyOwnedBy(hz2);

        DurableExecutorService executor = hz1.getDurableExecutorService("test");
        Future<Boolean> future = executor.submitToKeyOwner(new SleepingTask(MILLISECONDS.toSeconds(callTimeoutMillis) * 3), key);

        Boolean result = future.get(1, TimeUnit.MINUTES);
        assertTrue(result);
    }

    @Test
    public void durable_executor_collects_statistics_when_stats_enabled()
            throws ExecutionException, InterruptedException {

        // run task
        String executorName = "durable_executor";

        Config config = smallInstanceConfig();
        config.getDurableExecutorConfig(executorName)
                .setStatisticsEnabled(true);

        HazelcastInstance instance = createHazelcastInstance(config);

        DurableExecutorService executor = instance.getDurableExecutorService(executorName);
        executor.submit(new OneSecondSleepingTask()).get();

        // collect metrics
        Map<String, List<Long>> metrics = collectMetrics(DURABLE_EXECUTOR_PREFIX, instance);

        // check results
        assertMetricsCollected(metrics, 1000, 0,
                1, 1, 0, 1, 0);
    }

    @Test
    public void durable_executor_does_not_collect_statistics_when_stats_disabled()
            throws ExecutionException, InterruptedException {

        // run task
        String executorName = "durable_executor";

        Config config = smallInstanceConfig();
        config.getDurableExecutorConfig(executorName)
                .setStatisticsEnabled(false);

        HazelcastInstance instance = createHazelcastInstance(config);

        DurableExecutorService executor = instance.getDurableExecutorService(executorName);
        executor.submit(new OneSecondSleepingTask()).get();

        // collect metrics
        Map<String, List<Long>> metrics = collectMetrics(DURABLE_EXECUTOR_PREFIX, instance);

        // check results
        assertTrue("No metrics collection expected but " + metrics, metrics.isEmpty());
    }

    private static class InstanceAsserterRunnable implements Runnable, HazelcastInstanceAware, Serializable {

        transient HazelcastInstance instance;

        String instanceName;

        InstanceAsserterRunnable(String instanceName) {
            this.instanceName = instanceName;
        }

        @Override
        public void run() {
            if (instanceName.equals(instance.getName())) {
                instance.getCPSubsystem().getCountDownLatch(instanceName).countDown();
            }
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            instance = hazelcastInstance;
        }
    }

    private static class RunnableWithManagedContext implements Runnable, Serializable {

        @Override
        public void run() {
        }
    }

    static class HazelcastInstanceAwareRunnable implements Runnable, HazelcastInstanceAware, Serializable {
        private transient boolean initializeCalled = false;

        @Override
        public void run() {
            if (!initializeCalled) {
                throw new RuntimeException("The setHazelcastInstance should have been called");
            }
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            initializeCalled = true;
        }
    }

    static class ICountDownLatchAwaitCallable implements Callable<Boolean>, HazelcastInstanceAware, Serializable {

        private final String name;

        private HazelcastInstance instance;

        ICountDownLatchAwaitCallable(String name) {
            this.name = name;
        }

        @Override
        public Boolean call()
                throws Exception {
            return instance.getCPSubsystem().getCountDownLatch(name).await(100, TimeUnit.SECONDS);
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance instance) {
            this.instance = instance;
        }
    }

    static class SleepLatchRunnable implements Runnable, Serializable, PartitionAware<String> {

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

        @Override
        public String getPartitionKey() {
            return "key";
        }
    }

    static class EmptyRunnable implements Runnable, PartitionAware<String>, Serializable {

        @Override
        public void run() {
        }

        @Override
        public String getPartitionKey() {
            return "key";
        }
    }


    static class OneSecondSleepingTask implements Runnable, Serializable {

        OneSecondSleepingTask() {
        }

        @Override
        public void run() {
            sleepSeconds(1);
        }

    }
}
