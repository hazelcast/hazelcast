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

package com.hazelcast.executor;

import com.hazelcast.cluster.Member;
import com.hazelcast.config.Config;
import com.hazelcast.config.ExecutorConfig;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.MultiExecutionCallback;
import com.hazelcast.cp.IAtomicLong;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.partition.PartitionAware;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.executionservice.ExecutionService;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.config.ExecutorConfig.DEFAULT_POOL_SIZE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ExecutorServiceTest extends ExecutorServiceTestSupport {

    private static final int NODE_COUNT = 3;
    private static final int TASK_COUNT = 1000;


    // we need to make sure that if a deserialization exception is encounter, the exception isn't lost but always
    // is send to the caller.
    @Test
    public void whenDeserializationFails_thenExceptionPropagatedToCaller() throws Exception {
        HazelcastInstance hz = createHazelcastInstance(smallInstanceConfig());
        IExecutorService executor = hz.getExecutorService("executor");

        Future<String> future = executor.submit(new CallableWithDeserializationError<>());
        try {
            future.get();
            fail();
        } catch (ExecutionException e) {
            assertInstanceOf(HazelcastSerializationException.class, e.getCause());
        }
    }

    public static class CallableWithDeserializationError<T> implements Callable<T>, DataSerializable {
        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            in.readInt();
        }

        @Override
        public T call() throws Exception {
            return null;
        }
    }

    /* ############ andThen(Callback) ############ */

    @Test
    public void testPreregisteredExecutionCallbackCompletableFuture() {
        ExecutionService es = getExecutionService(createHazelcastInstance(smallInstanceConfig()));
        CountDownLatch callableLatch = new CountDownLatch(1);
        ExecutorService executorService = Executors.newSingleThreadExecutor();

        try {
            Future<String> future = executorService.submit(new CountDownLatchAwaitingCallable(callableLatch));
            CountingDownExecutionCallback<String> callback = new CountingDownExecutionCallback<>(1);
            InternalCompletableFuture<String> completableFuture = es.asCompletableFuture(future);
            completableFuture.whenCompleteAsync(callback);

            callableLatch.countDown();
            assertOpenEventually(callback.getLatch());
            assertEquals(CountDownLatchAwaitingCallable.RESULT, callback.getResult());
        } finally {
            executorService.shutdown();
        }
    }

    @Test
    public void testMultiPreregisteredExecutionCallbackCompletableFuture() {
        ExecutionService es = getExecutionService(createHazelcastInstance(smallInstanceConfig()));

        CountDownLatch callableLatch = new CountDownLatch(1);
        CountDownLatch callbackLatch = new CountDownLatch(2);
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        try {
            Future<String> future = executorService.submit(new CountDownLatchAwaitingCallable(callableLatch));
            CountingDownExecutionCallback<String> callback1 = new CountingDownExecutionCallback<>(callbackLatch);
            CountingDownExecutionCallback<String> callback2 = new CountingDownExecutionCallback<>(callbackLatch);

            CompletableFuture<String> completableFuture = es.asCompletableFuture(future);
            completableFuture.whenCompleteAsync(callback1);
            completableFuture.whenCompleteAsync(callback2);

            callableLatch.countDown();
            assertOpenEventually(callbackLatch);
            assertEquals(CountDownLatchAwaitingCallable.RESULT, callback1.getResult());
            assertEquals(CountDownLatchAwaitingCallable.RESULT, callback2.getResult());
        } finally {
            executorService.shutdown();
        }
    }

    @Test
    public void testPostRegisteredExecutionCallbackCompletableFuture() throws Exception {
        ExecutionService es = getExecutionService(createHazelcastInstance(smallInstanceConfig()));

        CountDownLatch callableLatch = new CountDownLatch(1);
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        try {
            Future<String> future = executorService.submit(new CountDownLatchAwaitingCallable(callableLatch));
            CompletableFuture<String> completableFuture = es.asCompletableFuture(future);
            callableLatch.countDown();
            future.get();

            CountingDownExecutionCallback<String> callback = new CountingDownExecutionCallback<>(1);
            completableFuture.whenCompleteAsync(callback);

            try {
                assertOpenEventually(callback.getLatch());
                assertEquals(CountDownLatchAwaitingCallable.RESULT, callback.getResult());
            } catch (AssertionError error) {
                System.out.println(callback.getLatch().getCount());
                System.out.println(callback.getResult());
                throw error;
            }
        } finally {
            executorService.shutdown();
        }
    }

    @Test
    public void testMultiPostRegisteredExecutionCallbackCompletableFuture() throws Exception {
        ExecutionService es = getExecutionService(createHazelcastInstance(smallInstanceConfig()));

        CountDownLatch callableLatch = new CountDownLatch(1);
        CountDownLatch callbackLatch = new CountDownLatch(2);
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        try {
            Future<String> future = executorService.submit(new CountDownLatchAwaitingCallable(callableLatch));
            CompletableFuture<String> completableFuture = es.asCompletableFuture(future);
            callableLatch.countDown();
            future.get();

            CountingDownExecutionCallback<String> callback1 = new CountingDownExecutionCallback<>(callbackLatch);
            completableFuture.whenCompleteAsync(callback1);
            CountingDownExecutionCallback<String> callback2 = new CountingDownExecutionCallback<>(callbackLatch);
            completableFuture.whenCompleteAsync(callback2);

            assertOpenEventually(callbackLatch);
            assertEquals(CountDownLatchAwaitingCallable.RESULT, callback1.getResult());
            assertEquals(CountDownLatchAwaitingCallable.RESULT, callback2.getResult());
        } finally {
            executorService.shutdown();
        }
    }

    @Test
    public void test_registerCallback_beforeFutureIsCompletedOnOtherNode() throws Exception {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance1 = factory.newHazelcastInstance(smallInstanceConfig());
        HazelcastInstance instance2 = factory.newHazelcastInstance(smallInstanceConfig());

        assertTrue(instance1.getCPSubsystem().getCountDownLatch("latch").trySetCount(1));

        String name = randomString();
        IExecutorService executorService = instance2.getExecutorService(name);
        ICountDownLatchAwaitCallable task = new ICountDownLatchAwaitCallable("latch");
        String key = generateKeyOwnedBy(instance1);
        CompletableFuture<Boolean> future = (CompletableFuture<Boolean>) executorService.submitToKeyOwner(task, key);
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
        IExecutorService executorService = instance2.getExecutorService(name);
        BasicTestCallable task = new BasicTestCallable();
        String key = generateKeyOwnedBy(instance1);
        CompletableFuture<String> future = (CompletableFuture<String>) executorService.submitToKeyOwner(task, key);
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
        IExecutorService executorService = instance2.getExecutorService(name);
        ICountDownLatchAwaitCallable task = new ICountDownLatchAwaitCallable("latch");
        String key = generateKeyOwnedBy(instance1);
        CompletableFuture<Boolean> future = (CompletableFuture<Boolean>) executorService.submitToKeyOwner(task, key);
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
        IExecutorService service = instance.getExecutorService(randomString());
        CountingDownExecutionCallback<String> callback = new CountingDownExecutionCallback<>(1);

        service.submit(new FailingTestTask(), callback);

        assertOpenEventually(callback.getLatch());
        assertTrue(callback.getResult() instanceof Throwable);
    }

    /* ############ submit(Runnable) ############ */

    @Test(expected = RejectedExecutionException.class)
    public void testEmptyMemberSelector() {
        HazelcastInstance instance = createHazelcastInstance(smallInstanceConfig());
        String name = randomString();
        IExecutorService executorService = instance.getExecutorService(name);
        HazelcastInstanceAwareRunnable task = new HazelcastInstanceAwareRunnable();
        executorService.execute(task, member -> false);
    }

    @Test
    public void testManagedContextAndLocal() throws Exception {
        Config config = smallInstanceConfig();
        config.addExecutorConfig(new ExecutorConfig("test", 1));
        final AtomicBoolean initialized = new AtomicBoolean();
        config.setManagedContext(obj -> {
            if (obj instanceof RunnableWithManagedContext) {
                initialized.set(true);
            }
            return obj;
        });

        HazelcastInstance instance = createHazelcastInstance(config);
        IExecutorService executor = instance.getExecutorService("test");

        RunnableWithManagedContext task = new RunnableWithManagedContext();
        executor.submit(task).get();
        assertTrue("The task should have been initialized by the ManagedContext", initialized.get());
    }

    static class RunnableWithManagedContext implements Runnable, Serializable {

        @Override
        public void run() {
        }
    }

    @Test
    public void hazelcastInstanceAwareAndLocal() throws Exception {
        Config config = smallInstanceConfig();
        config.addExecutorConfig(new ExecutorConfig("test", 1));
        HazelcastInstance instance = createHazelcastInstance(config);
        IExecutorService executor = instance.getExecutorService("test");

        HazelcastInstanceAwareRunnable task = new HazelcastInstanceAwareRunnable();
        // if 'setHazelcastInstance' not called we expect a RuntimeException
        executor.submit(task).get();
    }

    static class HazelcastInstanceAwareRunnable implements Runnable, HazelcastInstanceAware, Serializable {

        private transient boolean initializeCalled;

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

    @Test
    public void testExecuteMultipleNode() throws Exception {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(NODE_COUNT);
        HazelcastInstance[] instances = factory.newInstances(smallInstanceConfig());
        for (int i = 0; i < NODE_COUNT; i++) {
            IExecutorService service = instances[i].getExecutorService("testExecuteMultipleNode");
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
        ExecutionCallback<Object> callback = new ExecutionCallback<Object>() {
            public void onResponse(Object response) {
                if (response == null) {
                    nullResponseCount.incrementAndGet();
                }
                responseLatch.countDown();
            }

            public void onFailure(Throwable t) {
            }
        };
        for (int i = 0; i < NODE_COUNT; i++) {
            HazelcastInstance instance = instances[i];
            IExecutorService service = instance.getExecutorService("testSubmitToKeyOwnerRunnable");
            Member localMember = instance.getCluster().getLocalMember();
            int key = findNextKeyForMember(instance, localMember);
            service.submitToKeyOwner(
                    new IncrementAtomicLongIfMemberUUIDNotMatchRunnable(localMember.getUuid(), "testSubmitToKeyOwnerRunnable"),
                    key, callback);
        }
        assertOpenEventually(responseLatch);
        assertEquals(0, instances[0].getCPSubsystem().getAtomicLong("testSubmitToKeyOwnerRunnable").get());
        assertEquals(NODE_COUNT, nullResponseCount.get());
    }

    @Test
    public void testSubmitToMemberRunnable() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(NODE_COUNT);
        HazelcastInstance[] instances = factory.newInstances(smallInstanceConfig());
        final AtomicInteger nullResponseCount = new AtomicInteger(0);
        final CountDownLatch responseLatch = new CountDownLatch(NODE_COUNT);
        ExecutionCallback<Object> callback = new ExecutionCallback<Object>() {
            public void onResponse(Object response) {
                if (response == null) {
                    nullResponseCount.incrementAndGet();
                }
                responseLatch.countDown();
            }

            public void onFailure(Throwable t) {
            }
        };

        for (int i = 0; i < NODE_COUNT; i++) {
            HazelcastInstance instance = instances[i];
            IExecutorService service = instance.getExecutorService("testSubmitToMemberRunnable");
            Member localMember = instance.getCluster().getLocalMember();
            service.submitToMember(
                    new IncrementAtomicLongIfMemberUUIDNotMatchRunnable(localMember.getUuid(), "testSubmitToMemberRunnable"),
                    localMember, callback);
        }
        assertOpenEventually(responseLatch);
        assertEquals(0, instances[0].getCPSubsystem().getAtomicLong("testSubmitToMemberRunnable").get());
        assertEquals(NODE_COUNT, nullResponseCount.get());
    }

    @Test
    public void testSubmitToMembersRunnable() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(NODE_COUNT);
        HazelcastInstance[] instances = factory.newInstances(smallInstanceConfig());
        ResponseCountingMultiExecutionCallback callback = new ResponseCountingMultiExecutionCallback(NODE_COUNT);
        int sum = 0;
        Set<Member> membersSet = instances[0].getCluster().getMembers();
        Member[] members = membersSet.toArray(new Member[0]);
        Random random = new Random();
        for (int i = 0; i < NODE_COUNT; i++) {
            IExecutorService service = instances[i].getExecutorService("testSubmitToMembersRunnable");
            int n = random.nextInt(NODE_COUNT) + 1;
            sum += n;
            Member[] m = new Member[n];
            System.arraycopy(members, 0, m, 0, n);
            service.submitToMembers(new IncrementAtomicLongRunnable("testSubmitToMembersRunnable"), Arrays.asList(m), callback);
        }

        assertOpenEventually(callback.getLatch());
        IAtomicLong result = instances[0].getCPSubsystem().getAtomicLong("testSubmitToMembersRunnable");
        assertEquals(sum, result.get());
        assertEquals(sum, callback.getCount());
    }

    @Test
    public void testSubmitToAllMembersRunnable() throws Exception {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(NODE_COUNT);
        HazelcastInstance[] instances = factory.newInstances(smallInstanceConfig());
        final AtomicInteger nullResponseCount = new AtomicInteger(0);
        final CountDownLatch responseLatch = new CountDownLatch(NODE_COUNT * NODE_COUNT);
        MultiExecutionCallback callback = new MultiExecutionCallback() {
            public void onResponse(Member member, Object value) {
                if (value == null) {
                    nullResponseCount.incrementAndGet();
                }
                responseLatch.countDown();
            }

            public void onComplete(Map<Member, Object> values) {
            }
        };

        for (int i = 0; i < NODE_COUNT; i++) {
            IExecutorService service = instances[i].getExecutorService("testSubmitToAllMembersRunnable");
            service.submitToAllMembers(new IncrementAtomicLongRunnable("testSubmitToAllMembersRunnable"), callback);
        }

        assertTrue(responseLatch.await(30, TimeUnit.SECONDS));
        IAtomicLong result = instances[0].getCPSubsystem().getAtomicLong("testSubmitToAllMembersRunnable");
        assertEquals(NODE_COUNT * NODE_COUNT, result.get());
        assertEquals(NODE_COUNT * NODE_COUNT, nullResponseCount.get());
    }

    /* ############ submit(Callable) ############ */

    /**
     * Submit a null task must raise a NullPointerException
     */
    @SuppressWarnings("ConstantConditions")
    @Test(expected = NullPointerException.class)
    public void submitNullTask() {
        ExecutorService executor = createSingleNodeExecutorService("submitNullTask");
        executor.submit((Callable<?>) null);
    }

    /**
     * Run a basic task
     */
    @Test
    public void testBasicTask() throws Exception {
        Callable<String> task = new BasicTestCallable();
        ExecutorService executor = createSingleNodeExecutorService("testBasicTask");
        Future<String> future = executor.submit(task);
        assertEquals(future.get(), BasicTestCallable.RESULT);
    }

    @Test
    public void testSubmitMultipleNode() throws Exception {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(NODE_COUNT);
        HazelcastInstance[] instances = factory.newInstances(smallInstanceConfig());
        for (int i = 0; i < NODE_COUNT; i++) {
            IExecutorService service = instances[i].getExecutorService("testSubmitMultipleNode");
            Future<Long> future = service.submit(new IncrementAtomicLongCallable("testSubmitMultipleNode"));
            assertEquals((i + 1), (long) future.get());
        }
    }

    @Test
    public void testSubmitToKeyOwnerCallable() throws Exception {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(NODE_COUNT);
        HazelcastInstance[] instances = factory.newInstances(smallInstanceConfig());

        List<Future<Boolean>> futures = new ArrayList<>();
        for (int i = 0; i < NODE_COUNT; i++) {
            HazelcastInstance instance = instances[i];
            IExecutorService service = instance.getExecutorService("testSubmitToKeyOwnerCallable");

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
            IExecutorService service = instance.getExecutorService("testSubmitToKeyOwnerCallable");
            Member localMember = instance.getCluster().getLocalMember();
            int key = findNextKeyForMember(instance, localMember);
            service.submitToKeyOwner(new MemberUUIDCheckCallable(localMember.getUuid()), key, callback);
        }

        assertOpenEventually(callback.getResponseLatch());
        assertEquals(NODE_COUNT, callback.getSuccessResponseCount());
    }

    @Test
    public void testSubmitToMemberCallable() throws Exception {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(NODE_COUNT);
        HazelcastInstance[] instances = factory.newInstances(smallInstanceConfig());

        List<Future<Boolean>> futures = new ArrayList<>();

        for (int i = 0; i < NODE_COUNT; i++) {
            HazelcastInstance instance = instances[i];
            IExecutorService service = instance.getExecutorService("testSubmitToMemberCallable");

            UUID memberUuid = instance.getCluster().getLocalMember().getUuid();
            Future<Boolean> future = service.submitToMember(
                    new MemberUUIDCheckCallable(memberUuid), instance.getCluster().getLocalMember());
            futures.add(future);
        }

        for (Future<Boolean> future : futures) {
            assertTrue(future.get(10, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testSubmitToMemberCallable_withCallback() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(NODE_COUNT);
        HazelcastInstance[] instances = factory.newInstances(smallInstanceConfig());
        BooleanSuccessResponseCountingCallback callback = new BooleanSuccessResponseCountingCallback(NODE_COUNT);
        for (int i = 0; i < NODE_COUNT; i++) {
            HazelcastInstance instance = instances[i];
            IExecutorService service = instance.getExecutorService("testSubmitToMemberCallable");

            UUID memberUuid = instance.getCluster().getLocalMember().getUuid();
            service.submitToMember(new MemberUUIDCheckCallable(memberUuid), instance.getCluster().getLocalMember(), callback);
        }

        assertOpenEventually(callback.getResponseLatch());
        assertEquals(NODE_COUNT, callback.getSuccessResponseCount());
    }

    @Test
    public void testSubmitToMembersCallable() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(NODE_COUNT);
        HazelcastInstance[] instances = factory.newInstances(smallInstanceConfig());
        final AtomicInteger count = new AtomicInteger(0);
        final CountDownLatch latch = new CountDownLatch(NODE_COUNT);
        MultiExecutionCallback callback = new MultiExecutionCallback() {
            public void onResponse(Member member, Object value) {
                count.incrementAndGet();
            }

            public void onComplete(Map<Member, Object> values) {
                latch.countDown();
            }
        };
        int sum = 0;
        Set<Member> membersSet = instances[0].getCluster().getMembers();
        Member[] members = membersSet.toArray(new Member[0]);
        Random random = new Random();
        String name = "testSubmitToMembersCallable";

        for (int i = 0; i < NODE_COUNT; i++) {
            IExecutorService service = instances[i].getExecutorService(name);
            int n = random.nextInt(NODE_COUNT) + 1;
            sum += n;
            Member[] m = new Member[n];
            System.arraycopy(members, 0, m, 0, n);
            service.submitToMembers(new IncrementAtomicLongCallable(name), Arrays.asList(m), callback);
        }

        assertOpenEventually(latch);
        IAtomicLong result = instances[0].getCPSubsystem().getAtomicLong(name);
        assertEquals(sum, result.get());
        assertEquals(sum, count.get());
    }

    @Test
    public void testSubmitToAllMembersCallable() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(NODE_COUNT);
        HazelcastInstance[] instances = factory.newInstances(smallInstanceConfig());
        final AtomicInteger count = new AtomicInteger(0);
        final CountDownLatch countDownLatch = new CountDownLatch(NODE_COUNT * NODE_COUNT);
        MultiExecutionCallback callback = new MultiExecutionCallback() {
            public void onResponse(Member member, Object value) {
                count.incrementAndGet();
                countDownLatch.countDown();
            }

            public void onComplete(Map<Member, Object> values) {
            }
        };
        for (int i = 0; i < NODE_COUNT; i++) {
            IExecutorService service = instances[i].getExecutorService("testSubmitToAllMembersCallable");
            service.submitToAllMembers(new IncrementAtomicLongCallable("testSubmitToAllMembersCallable"), callback);
        }
        assertOpenEventually(countDownLatch);
        IAtomicLong result = instances[0].getCPSubsystem().getAtomicLong("testSubmitToAllMembersCallable");
        assertEquals(NODE_COUNT * NODE_COUNT, result.get());
        assertEquals(NODE_COUNT * NODE_COUNT, count.get());
    }

    /* ############ cancellation ############ */

    @Test
    public void testCancellationAwareTask() throws Exception {
        SleepingTask task = new SleepingTask(10);
        ExecutorService executor = createSingleNodeExecutorService("testCancellationAwareTask");
        Future<?> future = executor.submit(task);

        try {
            future.get(2, TimeUnit.SECONDS);
            fail("Should throw TimeoutException!");
        } catch (TimeoutException expected) {
        }
        assertFalse(future.isDone());
        assertTrue(future.cancel(true));
        assertTrue(future.isCancelled());
        assertTrue(future.isDone());

        try {
            future.get();
            fail("Should not complete the task successfully");
        } catch (CancellationException expected) {
        } catch (Exception e) {
            fail("Unexpected exception " + e);
        }
    }

    @Test
    public void testCancellationAwareTask2() {
        Callable<?> task1 = new SleepingTask(Integer.MAX_VALUE);
        ExecutorService executor = createSingleNodeExecutorService("testCancellationAwareTask", 1);
        Future<?> future1 = executor.submit(task1);
        try {
            future1.get(2, TimeUnit.SECONDS);
            fail("SleepingTask should not return response");
        } catch (TimeoutException ignored) {

        } catch (Exception e) {
            if (e.getCause() instanceof RejectedExecutionException) {
                fail("SleepingTask is rejected!");
            }
        }
        assertFalse(future1.isDone());

        Callable<?> task2 = new BasicTestCallable();
        Future<?> future2 = executor.submit(task2);

        assertFalse(future2.isDone());
        assertTrue(future2.cancel(true));
        assertTrue(future2.isCancelled());
        assertTrue(future2.isDone());

        try {
            future2.get();
            fail("Should not complete the task successfully");
        } catch (CancellationException expected) {
        } catch (Exception e) {
            fail("Unexpected exception " + e);
        }
    }

    /* ############ future ############ */

    /**
     * Test the method isDone()
     */
    @Test
    public void testIsDoneMethod() throws Exception {
        Callable<String> task = new BasicTestCallable();
        IExecutorService executor = createSingleNodeExecutorService("isDoneMethod");
        Future<String> future = executor.submit(task);
        assertResult(future, BasicTestCallable.RESULT);
    }

    /**
     * Test for the issue 129.
     * Repeatedly runs tasks and check for isDone() status after get().
     */
    @Test
    public void testIsDoneMethod2() throws Exception {
        ExecutorService executor = createSingleNodeExecutorService("isDoneMethod2");
        for (int i = 0; i < TASK_COUNT; i++) {
            Callable<String> task1 = new BasicTestCallable();
            Callable<String> task2 = new BasicTestCallable();
            Future<String> future1 = executor.submit(task1);
            Future<String> future2 = executor.submit(task2);
            assertResult(future2, BasicTestCallable.RESULT);
            assertResult(future1, BasicTestCallable.RESULT);
        }
    }

    /**
     * Test multiple Future.get() invocation
     */
    @Test
    public void testMultipleFutureGets() throws Exception {
        Callable<String> task = new BasicTestCallable();
        ExecutorService executor = createSingleNodeExecutorService("isTwoGetFromFuture");
        Future<String> future = executor.submit(task);
        assertResult(future, BasicTestCallable.RESULT);
        assertResult(future, BasicTestCallable.RESULT);
        assertResult(future, BasicTestCallable.RESULT);
        assertResult(future, BasicTestCallable.RESULT);
    }

    private void assertResult(Future<?> future, Object expected) throws Exception {
        assertEquals(future.get(), expected);
        assertTrue(future.isDone());
    }

    @Test
    public void testIssue292() {
        CountingDownExecutionCallback<Member> callback = new CountingDownExecutionCallback<>(1);
        createSingleNodeExecutorService("testIssue292").submit(new MemberCheck(), callback);
        assertOpenEventually(callback.getLatch());
        assertTrue(callback.getResult() instanceof Member);
    }

    /**
     * Execute a task that is executing something else inside.
     * Nested Execution.
     */
    @Test
    public void testNestedExecution() {
        Callable<String> task = new NestedExecutorTask();
        ExecutorService executor = createSingleNodeExecutorService("testNestedExecution");
        Future<?> future = executor.submit(task);
        assertCompletesEventually(future);
    }

    /**
     * invokeAll tests
     */
    @Test
    public void testInvokeAll() throws Exception {
        ExecutorService executor = createSingleNodeExecutorService("testInvokeAll");
        assertFalse(executor.isShutdown());
        // only one task
        ArrayList<Callable<String>> tasks = new ArrayList<>();
        tasks.add(new BasicTestCallable());
        List<Future<String>> futures = executor.invokeAll(tasks);
        assertEquals(futures.size(), 1);
        assertEquals(futures.get(0).get(), BasicTestCallable.RESULT);
        // more tasks
        tasks.clear();
        for (int i = 0; i < TASK_COUNT; i++) {
            tasks.add(new BasicTestCallable());
        }
        futures = executor.invokeAll(tasks);
        assertEquals(futures.size(), TASK_COUNT);
        for (int i = 0; i < TASK_COUNT; i++) {
            assertEquals(futures.get(i).get(), BasicTestCallable.RESULT);
        }
    }

    @Test
    public void testInvokeAllTimeoutCancelled() throws Exception {
        ExecutorService executor = createSingleNodeExecutorService("testInvokeAll");
        assertFalse(executor.isShutdown());
        // only one task
        ArrayList<Callable<Boolean>> tasks = new ArrayList<>();
        tasks.add(new SleepingTask(0));
        List<Future<Boolean>> futures = executor.invokeAll(tasks, 5, TimeUnit.SECONDS);
        assertEquals(futures.size(), 1);
        assertEquals(futures.get(0).get(), Boolean.TRUE);
        // more tasks
        tasks.clear();
        for (int i = 0; i < TASK_COUNT; i++) {
            tasks.add(new SleepingTask(i < 2 ? 0 : 20));
        }
        futures = executor.invokeAll(tasks, 5, TimeUnit.SECONDS);
        assertEquals(futures.size(), TASK_COUNT);
        for (int i = 0; i < TASK_COUNT; i++) {
            if (i < 2) {
                assertEquals(futures.get(i).get(), Boolean.TRUE);
            } else {
                boolean excepted = false;
                try {
                    futures.get(i).get();
                } catch (CancellationException e) {
                    excepted = true;
                }
                assertTrue(excepted);
            }
        }
    }

    @Test
    public void testInvokeAllTimeoutSuccess() throws Exception {
        ExecutorService executor = createSingleNodeExecutorService("testInvokeAll");
        assertFalse(executor.isShutdown());
        // only one task
        ArrayList<Callable<String>> tasks = new ArrayList<>();
        tasks.add(new BasicTestCallable());
        List<Future<String>> futures = executor.invokeAll(tasks, 5, TimeUnit.SECONDS);
        assertEquals(futures.size(), 1);
        assertEquals(futures.get(0).get(), BasicTestCallable.RESULT);
        // more tasks
        tasks.clear();
        for (int i = 0; i < TASK_COUNT; i++) {
            tasks.add(new BasicTestCallable());
        }
        futures = executor.invokeAll(tasks, 15, TimeUnit.SECONDS);
        assertEquals(futures.size(), TASK_COUNT);
        for (int i = 0; i < TASK_COUNT; i++) {
            assertEquals(futures.get(i).get(), BasicTestCallable.RESULT);
        }
    }

    /**
     * Shutdown-related method behaviour when the cluster is running
     */
    @Test
    public void testShutdownBehaviour() {
        ExecutorService executor = createSingleNodeExecutorService("testShutdownBehaviour");
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
     * Shutting down the cluster should act as the ExecutorService shutdown
     */
    @Test(expected = RejectedExecutionException.class)
    public void test_whenClusterShutdown_thenNewTasksShouldBeRejected() {
        ExecutorService executor = createSingleNodeExecutorService("testClusterShutdownTaskRejection");
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
    public void testClusterShutdown_whenMultipleNodes_thenAllExecutorsAreShutdown() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance1 = factory.newHazelcastInstance(smallInstanceConfig());
        HazelcastInstance instance2 = factory.newHazelcastInstance(smallInstanceConfig());

        final ExecutorService es1 = instance1.getExecutorService("testClusterShutdown");
        final ExecutorService es2 = instance1.getExecutorService("testClusterShutdown");

        assertFalse(es1.isTerminated());
        assertFalse(es2.isTerminated());

        // we shutdown the ExecutorService on the first instance
        es1.shutdown();

        // the ExecutorService on the second instance should be shutdown via a ShutdownOperation
        assertTrueEventually(() -> assertTrue(es2.isTerminated()));
    }

    @Test
    public void testStatsIssue2039() throws Exception {
        Config config = smallInstanceConfig();
        String name = "testStatsIssue2039";
        config.addExecutorConfig(new ExecutorConfig(name).setQueueCapacity(1).setPoolSize(1));
        HazelcastInstance instance = createHazelcastInstance(config);
        IExecutorService executorService = instance.getExecutorService(name);

        executorService.execute(new SleepLatchRunnable());

        assertOpenEventually(SleepLatchRunnable.startLatch, 30);
        Future<?> waitingInQueue = executorService.submit(new EmptyRunnable());

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

        waitingInQueue.get(1, TimeUnit.MINUTES);

        LocalExecutorStats stats = executorService.getLocalExecutorStats();
        assertEquals(2, stats.getStartedTaskCount());
        assertEquals(0, stats.getPendingTaskCount());
    }

    @Test
    public void testExecutorServiceStats() throws Exception {
        int i = 10;
        LocalExecutorStats stats = executeTasksForStats("testExecutorServiceStats", i, true);

        assertEquals(i + 1, stats.getStartedTaskCount());
        assertEquals(i, stats.getCompletedTaskCount());
        assertEquals(0, stats.getPendingTaskCount());
        assertEquals(1, stats.getCancelledTaskCount());
    }

    @Test
    public void testExecutorServiceStats_whenStatsAreDisabled() throws Exception {
        LocalExecutorStats stats = executeTasksForStats("testExecutorServiceStats_whenStatsDisabled", 10, false);

        assertEquals(0, stats.getStartedTaskCount());
        assertEquals(0, stats.getCompletedTaskCount());
        assertEquals(0, stats.getPendingTaskCount());
        assertEquals(0, stats.getCancelledTaskCount());
    }

    /**
     * Executes {@code tasksToExecute}+1 tasks, allowing {@code tasksToExecute} to complete and cancelling 1.
     */
    private LocalExecutorStats executeTasksForStats(String executorName, int tasksToExecute, boolean statsEnabled)
            throws Exception {
        IExecutorService executorService = createSingleNodeExecutorService(executorName, DEFAULT_POOL_SIZE, statsEnabled);
        BlockingQueue<Future<?>> taskQueue = new ArrayBlockingQueue<>(tasksToExecute);
        CountDownLatch latch = new CountDownLatch(tasksToExecute);

        for (int i = 0; i < tasksToExecute; i++) {
            Future<?> future = executorService.submit(new EmptyRunnable());
            taskQueue.offer(future);
        }
        // await completion and countdown for each completed task
        for (int i = 0; i < tasksToExecute; i++) {
            Future<?> future = taskQueue.take();
            future.get();
            latch.countDown();
        }
        assertOpenEventually(latch);

        Future<Boolean> f = executorService.submit(new SleepingTask(10));
        Thread.sleep(1000);
        f.cancel(true);
        try {
            f.get();
        } catch (CancellationException ignored) {
        }

        return executorService.getLocalExecutorStats();
    }

    @Test
    public void testLongRunningCallable() throws Exception {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);

        Config config = smallInstanceConfig();
        long callTimeoutMillis = 4000;
        config.setProperty(ClusterProperty.OPERATION_CALL_TIMEOUT_MILLIS.getName(), String.valueOf(callTimeoutMillis));

        HazelcastInstance hz1 = factory.newHazelcastInstance(config);
        HazelcastInstance hz2 = factory.newHazelcastInstance(config);

        IExecutorService executor = hz1.getExecutorService("test");
        Future<Boolean> future = executor.submitToMember(
                new SleepingTask(TimeUnit.MILLISECONDS.toSeconds(callTimeoutMillis) * 3), hz2.getCluster().getLocalMember());

        Boolean result = future.get(1, TimeUnit.MINUTES);
        assertTrue(result);
    }

    static class ICountDownLatchAwaitCallable implements Callable<Boolean>, HazelcastInstanceAware, Serializable {

        private String name;

        private HazelcastInstance instance;

        ICountDownLatchAwaitCallable(String name) {
            this.name = name;
        }

        @Override
        public Boolean call() throws Exception {
            return instance.getCPSubsystem().getCountDownLatch(name).await(100, TimeUnit.SECONDS);
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance instance) {
            this.instance = instance;
        }
    }

    static class SleepLatchRunnable implements Runnable, Serializable {

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
    }

    static class EmptyRunnable implements Runnable, Serializable, PartitionAware<String> {
        @Override
        public void run() {
        }

        @Override
        public String getPartitionKey() {
            return "key";
        }
    }


    @Test(expected = HazelcastSerializationException.class)
    public void testUnserializableResponse_exceptionPropagatesToCaller() throws Throwable {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance1 = factory.newHazelcastInstance(smallInstanceConfig());
        HazelcastInstance instance2 = factory.newHazelcastInstance(smallInstanceConfig());

        IExecutorService service = instance1.getExecutorService("executor");
        TaskWithUnserialazableResponse counterCallable = new TaskWithUnserialazableResponse();
        Future<?> future = service.submitToMember(counterCallable, instance2.getCluster().getLocalMember());
        try {
            future.get();
        } catch (ExecutionException e) {
            throw e.getCause();
        }
    }

    @Test(expected = HazelcastSerializationException.class)
    public void testUnserializableResponse_exceptionPropagatesToCallerCallback() throws Throwable {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance1 = factory.newHazelcastInstance(smallInstanceConfig());
        HazelcastInstance instance2 = factory.newHazelcastInstance(smallInstanceConfig());


        IExecutorService service = instance1.getExecutorService("executor");
        TaskWithUnserialazableResponse counterCallable = new TaskWithUnserialazableResponse();
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        final AtomicReference<Throwable> throwable = new AtomicReference<>();
        service.submitToMember(counterCallable, instance2.getCluster().getLocalMember(),
                new ExecutionCallback<Object>() {
                    @Override
                    public void onResponse(Object response) {

                    }

                    @Override
                    public void onFailure(Throwable t) {
                        throwable.set(t);
                        countDownLatch.countDown();
                    }
                });
        assertOpenEventually(countDownLatch);
        throw throwable.get();
    }

    private static class TaskWithUnserialazableResponse implements Callable<Object>, Serializable {
        @Override
        public Object call() throws Exception {
            return new Object();
        }
    }
}
