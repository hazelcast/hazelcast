/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.Config;
import com.hazelcast.config.ExecutorConfig;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.ManagedContext;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberSelector;
import com.hazelcast.core.MultiExecutionCallback;
import com.hazelcast.core.PartitionAware;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.monitor.LocalExecutorStats;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ExecutorServiceTest extends ExecutorServiceTestSupport {

    public static final int NODE_COUNT = 3;

    public static final int TASK_COUNT = 1000;



    /* ############ andThen ############ */

    @Test
    public void testPreregisteredExecutionCallbackCompletableFuture() throws Exception {
        final ExecutionService es = getExecutionService(createHazelcastInstance());
        final CountDownLatch callableLatch = new CountDownLatch(1);
        final ExecutorService executorService = Executors.newSingleThreadExecutor();

        try {
            final Future<String> future = executorService.submit(new CountDownLatchAwaitingCallable(callableLatch));
            final CountingDownExecutionCallback<String> callback = new CountingDownExecutionCallback<String>(1);
            final ICompletableFuture<String> completableFuture = es.asCompletableFuture(future);
            completableFuture.andThen(callback);

            callableLatch.countDown();
            assertOpenEventually(callback.getLatch());
            assertEquals(CountDownLatchAwaitingCallable.RESULT, callback.getResult());
        } finally {
            executorService.shutdown();
        }
    }

    @Test
    public void testMultiPreregisteredExecutionCallbackCompletableFuture() throws Exception {
        final ExecutionService es = getExecutionService(createHazelcastInstance());

        final CountDownLatch callableLatch = new CountDownLatch(1);
        final CountDownLatch callbackLatch = new CountDownLatch(2);
        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        try {
            final Future<String> future = executorService.submit(new CountDownLatchAwaitingCallable(callableLatch));
            final CountingDownExecutionCallback<String> callback1 = new CountingDownExecutionCallback<String>(callbackLatch);
            final CountingDownExecutionCallback<String> callback2 = new CountingDownExecutionCallback<String>(callbackLatch);

            final ICompletableFuture<String> completableFuture = es.asCompletableFuture(future);
            completableFuture.andThen(callback1);
            completableFuture.andThen(callback2);

            callableLatch.countDown();
            assertOpenEventually(callbackLatch);
            assertEquals(CountDownLatchAwaitingCallable.RESULT, callback1.getResult());
            assertEquals(CountDownLatchAwaitingCallable.RESULT, callback2.getResult());
        } finally {
            executorService.shutdown();
        }
    }

    @Test
    public void testPostregisteredExecutionCallbackCompletableFuture() throws Exception {
        final ExecutionService es = getExecutionService(createHazelcastInstance());

        final CountDownLatch callableLatch = new CountDownLatch(1);
        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        try {
            final Future<String> future = executorService.submit(new CountDownLatchAwaitingCallable(callableLatch));
            final ICompletableFuture<String> completableFuture = es.asCompletableFuture(future);
            callableLatch.countDown();
            future.get();

            final CountingDownExecutionCallback<String> callback = new CountingDownExecutionCallback<String>(1);
            completableFuture.andThen(callback);

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
    public void testMultiPostregisteredExecutionCallbackCompletableFuture() throws Exception {
        final ExecutionService es = getExecutionService(createHazelcastInstance());

        final CountDownLatch callableLatch = new CountDownLatch(1);
        final CountDownLatch callbackLatch = new CountDownLatch(2);
        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        try {
            final Future<String> future = executorService.submit(new CountDownLatchAwaitingCallable(callableLatch));
            final ICompletableFuture<String> completableFuture = es.asCompletableFuture(future);
            callableLatch.countDown();
            future.get();

            final CountingDownExecutionCallback<String> callback1 = new CountingDownExecutionCallback<String>(callbackLatch);
            completableFuture.andThen(callback1);
            final CountingDownExecutionCallback<String> callback2 = new CountingDownExecutionCallback<String>(callbackLatch);
            completableFuture.andThen(callback2);

            assertOpenEventually(callbackLatch);
            assertEquals(CountDownLatchAwaitingCallable.RESULT, callback1.getResult());
            assertEquals(CountDownLatchAwaitingCallable.RESULT, callback2.getResult());
        } finally {
            executorService.shutdown();
        }
    }

    @Test
    public void test_registerCallback_beforeFutureIsCompletedOnOtherNode() throws ExecutionException, InterruptedException {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance1 = factory.newHazelcastInstance();
        HazelcastInstance instance2 = factory.newHazelcastInstance();

        assertTrue(instance1.getCountDownLatch("latch").trySetCount(1));

        String name = randomString();
        IExecutorService executorService = instance2.getExecutorService(name);
        ICountDownLatchAwaitCallable task = new ICountDownLatchAwaitCallable("latch");
        String key = generateKeyOwnedBy(instance1);
        ICompletableFuture<Boolean> future = (ICompletableFuture<Boolean>) executorService.submitToKeyOwner(task, key);
        final CountingDownExecutionCallback<Boolean> callback = new CountingDownExecutionCallback<Boolean>(1);
        future.andThen(callback);
        instance1.getCountDownLatch("latch").countDown();
        assertTrue(future.get());
        assertOpenEventually(callback.getLatch());
    }

    @Test
    public void test_registerCallback_afterFutureIsCompletedOnOtherNode() throws ExecutionException, InterruptedException {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance1 = factory.newHazelcastInstance();
        HazelcastInstance instance2 = factory.newHazelcastInstance();
        String name = randomString();
        IExecutorService executorService = instance2.getExecutorService(name);
        BasicTestCallable task = new BasicTestCallable();
        String key = generateKeyOwnedBy(instance1);
        ICompletableFuture<String> future = (ICompletableFuture<String>) executorService.submitToKeyOwner(task, key);
        assertEquals(BasicTestCallable.RESULT, future.get());;

        final CountingDownExecutionCallback<String> callback = new CountingDownExecutionCallback<String>(1);
        future.andThen(callback);

        assertOpenEventually(callback.getLatch(), 10);
    }

    @Test
    public void test_registerCallback_multipleTimes_futureIsCompletedOnOtherNode() throws ExecutionException, InterruptedException {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance1 = factory.newHazelcastInstance();
        HazelcastInstance instance2 = factory.newHazelcastInstance();

        assertTrue(instance1.getCountDownLatch("latch").trySetCount(1));

        String name = randomString();
        IExecutorService executorService = instance2.getExecutorService(name);
        ICountDownLatchAwaitCallable task = new ICountDownLatchAwaitCallable("latch");
        String key = generateKeyOwnedBy(instance1);
        ICompletableFuture<Boolean> future = (ICompletableFuture<Boolean>) executorService.submitToKeyOwner(task, key);
        final CountDownLatch latch = new CountDownLatch(2);
        final CountingDownExecutionCallback<Boolean> callback = new CountingDownExecutionCallback<Boolean>(latch);
        future.andThen(callback);
        future.andThen(callback);
        instance1.getCountDownLatch("latch").countDown();
        assertTrue(future.get());
        assertOpenEventually(latch, 10);
    }

    @Test
    public void testSubmitFailingCallableException_withExecutionCallback() throws ExecutionException, InterruptedException {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        HazelcastInstance instance = factory.newHazelcastInstance();
        IExecutorService service = instance.getExecutorService(randomString());
        final CountingDownExecutionCallback callback = new CountingDownExecutionCallback(1);
        service.submit(new FailingTestTask(), callback);
        assertOpenEventually(callback.getLatch());
        assertTrue(callback.getResult() instanceof Throwable);
    }



    /* ############ submit runnable ############ */

    @Test(expected = RejectedExecutionException.class)
    public void testEmptyMemberSelector() {
        final HazelcastInstance instance = createHazelcastInstance();
        final String name = randomString();
        final IExecutorService executorService = instance.getExecutorService(name);
        HazelcastInstanceAwareRunnable task = new HazelcastInstanceAwareRunnable();
        executorService.execute(task, new MemberSelector() {
            @Override
            public boolean select(final Member member) {
                return false;
            }
        });
    }

    @Test
    public void testManagedContextAndLocal() throws Exception {
        final Config config = new Config();
        config.addExecutorConfig(new ExecutorConfig("test", 1));
        final AtomicBoolean initialized = new AtomicBoolean();
        config.setManagedContext(new ManagedContext() {
            @Override
            public Object initialize(Object obj) {
                if (obj instanceof RunnableWithManagedContext) {
                    initialized.set(true);
                }
                return obj;
            }
        });

        final HazelcastInstance instance = createHazelcastInstance(config);
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
        final Config config = new Config();
        config.addExecutorConfig(new ExecutorConfig("test", 1));
        final HazelcastInstance instance = createHazelcastInstance(config);
        IExecutorService executor = instance.getExecutorService("test");

        HazelcastInstanceAwareRunnable task = new HazelcastInstanceAwareRunnable();
        // if 'setHazelcastInstance' not called we expect a RuntimeException
        executor.submit(task).get();
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

    @Test
    public void testExecuteMultipleNode()
            throws InterruptedException, ExecutionException, TimeoutException {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(NODE_COUNT);
        final HazelcastInstance[] instances = factory.newInstances(new Config());
        for (int i = 0; i < NODE_COUNT; i++) {
            final IExecutorService service = instances[i].getExecutorService("testExecuteMultipleNode");
            final int rand = new Random().nextInt(100);
            final Future<Integer> future = service.submit(new IncrementAtomicLongRunnable("count"), rand);
            assertEquals(Integer.valueOf(rand), future.get(10, TimeUnit.SECONDS));
        }

        final IAtomicLong count = instances[0].getAtomicLong("count");
        assertEquals(NODE_COUNT, count.get());
    }

    @Test
    public void testSubmitToKeyOwnerRunnable() throws InterruptedException {
        final int k = NODE_COUNT;
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(k);
        final HazelcastInstance[] instances = factory.newInstances(new Config());
        final AtomicInteger nullResponseCount = new AtomicInteger(0);
        final CountDownLatch responseLatch = new CountDownLatch(k);
        final ExecutionCallback callback = new ExecutionCallback() {
            public void onResponse(Object response) {
                if (response == null) {
                    nullResponseCount.incrementAndGet();
                }
                responseLatch.countDown();
            }

            public void onFailure(Throwable t) {
            }
        };
        for (int i = 0; i < k; i++) {
            final HazelcastInstance instance = instances[i];
            final IExecutorService service = instance.getExecutorService("testSubmitToKeyOwnerRunnable");
            final Member localMember = instance.getCluster().getLocalMember();
            final int key = findNextKeyForMember(instance, localMember);
            service.submitToKeyOwner(
                    new IncrementAtomicLongIfMemberUUIDNotMatchRunnable(localMember.getUuid(), "testSubmitToKeyOwnerRunnable"),
                    key, callback);
        }
        assertOpenEventually(responseLatch);
        assertEquals(0, instances[0].getAtomicLong("testSubmitToKeyOwnerRunnable").get());
        assertEquals(k, nullResponseCount.get());
    }

    @Test
    public void testSubmitToMemberRunnable() throws InterruptedException {
        final int k = NODE_COUNT;
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(k);
        final HazelcastInstance[] instances = factory.newInstances(new Config());
        final AtomicInteger nullResponseCount = new AtomicInteger(0);
        final CountDownLatch responseLatch = new CountDownLatch(k);
        final ExecutionCallback callback = new ExecutionCallback() {
            public void onResponse(Object response) {
                if (response == null) {
                    nullResponseCount.incrementAndGet();
                }
                responseLatch.countDown();
            }

            public void onFailure(Throwable t) {
            }
        };

        for (int i = 0; i < k; i++) {
            final HazelcastInstance instance = instances[i];
            final IExecutorService service = instance.getExecutorService("testSubmitToMemberRunnable");
            final Member localMember = instance.getCluster().getLocalMember();
            service.submitToMember(
                    new IncrementAtomicLongIfMemberUUIDNotMatchRunnable(localMember.getUuid(), "testSubmitToMemberRunnable"),
                    localMember, callback);
        }
        assertOpenEventually(responseLatch);
        assertEquals(0, instances[0].getAtomicLong("testSubmitToMemberRunnable").get());
        assertEquals(k, nullResponseCount.get());
    }

    @Test
    public void testSubmitToMembersRunnable() throws InterruptedException {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(NODE_COUNT);
        final HazelcastInstance[] instances = factory.newInstances(new Config());
        final ResponseCountingMultiExecutionCallback callback = new ResponseCountingMultiExecutionCallback(NODE_COUNT);
        int sum = 0;
        final Set<Member> membersSet = instances[0].getCluster().getMembers();
        final Member[] members = membersSet.toArray(new Member[membersSet.size()]);
        final Random random = new Random();
        for (int i = 0; i < NODE_COUNT; i++) {
            final IExecutorService service = instances[i].getExecutorService("testSubmitToMembersRunnable");
            final int n = random.nextInt(NODE_COUNT) + 1;
            sum += n;
            Member[] m = new Member[n];
            System.arraycopy(members, 0, m, 0, n);
            service.submitToMembers(new IncrementAtomicLongRunnable("testSubmitToMembersRunnable"), Arrays.asList(m), callback);
        }

        assertOpenEventually(callback.getLatch());
        final IAtomicLong result = instances[0].getAtomicLong("testSubmitToMembersRunnable");
        assertEquals(sum, result.get());
        assertEquals(sum, callback.getCount());
    }

    @Test
    public void testSubmitToAllMembersRunnable() throws InterruptedException {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(NODE_COUNT);
        final HazelcastInstance[] instances = factory.newInstances(new Config());
        final AtomicInteger nullResponseCount = new AtomicInteger(0);
        final CountDownLatch responseLatch = new CountDownLatch(NODE_COUNT * NODE_COUNT);
        final MultiExecutionCallback callback = new MultiExecutionCallback() {
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
            final IExecutorService service = instances[i].getExecutorService("testSubmitToAllMembersRunnable");
            service.submitToAllMembers(new IncrementAtomicLongRunnable("testSubmitToAllMembersRunnable"), callback);
        }

        assertTrue(responseLatch.await(30, TimeUnit.SECONDS));
        final IAtomicLong result = instances[0].getAtomicLong("testSubmitToAllMembersRunnable");
        assertEquals(NODE_COUNT * NODE_COUNT, result.get());
        assertEquals(NODE_COUNT * NODE_COUNT, nullResponseCount.get());
    }



    /* ############ submit callable ############ */

    /**
     * Submit a null task must raise a NullPointerException
     */
    @Test(expected = NullPointerException.class)
    public void submitNullTask() throws Exception {
        ExecutorService executor = createSingleNodeExecutorService("submitNullTask");
        Callable c = null;
        executor.submit(c);
    }

    /**
     * Run a basic task
     */
    @Test
    public void testBasicTask() throws Exception {
        Callable<String> task = new BasicTestCallable();
        ExecutorService executor = createSingleNodeExecutorService("testBasicTask");
        Future future = executor.submit(task);
        assertEquals(future.get(), BasicTestCallable.RESULT);
    }

    @Test
    public void testSubmitMultipleNode() throws ExecutionException, InterruptedException {
        final int k = NODE_COUNT;
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(k);
        final HazelcastInstance[] instances = factory.newInstances(new Config());
        for (int i = 0; i < k; i++) {
            final IExecutorService service = instances[i].getExecutorService("testSubmitMultipleNode");
            final Future future = service.submit(new IncrementAtomicLongCallable("testSubmitMultipleNode"));
            assertEquals((long) (i + 1), future.get());
        }
    }

    @Test
    public void testSubmitToKeyOwnerCallable() throws Exception {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(NODE_COUNT);
        final HazelcastInstance[] instances = factory.newInstances(new Config());

        final List<Future> futures = new ArrayList<Future>();

        for (int i = 0; i < NODE_COUNT; i++) {
            final HazelcastInstance instance = instances[i];
            final IExecutorService service = instance.getExecutorService("testSubmitToKeyOwnerCallable");

            final Member localMember = instance.getCluster().getLocalMember();
            final int key = findNextKeyForMember(instance, localMember);
            final Future f = service.submitToKeyOwner(new MemberUUIDCheckCallable(localMember.getUuid()), key);
            futures.add(f);
        }

        for (Future f : futures) {
            assertTrue((Boolean) f.get(10, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testSubmitToKeyOwnerCallable_withCallback() throws Exception {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(NODE_COUNT);
        final HazelcastInstance[] instances = factory.newInstances(new Config());
        final BooleanSuccessResponseCountingCallback callback = new BooleanSuccessResponseCountingCallback(NODE_COUNT);

        for (int i = 0; i < NODE_COUNT; i++) {
            final HazelcastInstance instance = instances[i];
            final IExecutorService service = instance.getExecutorService("testSubmitToKeyOwnerCallable");
            final Member localMember = instance.getCluster().getLocalMember();
            final int key = findNextKeyForMember(instance, localMember);
            service.submitToKeyOwner(new MemberUUIDCheckCallable(localMember.getUuid()), key, callback);
        }

        assertOpenEventually(callback.getResponseLatch());
        assertEquals(NODE_COUNT, callback.getSuccessResponseCount());
    }

    @Test
    public void testSubmitToMemberCallable() throws ExecutionException, InterruptedException, TimeoutException {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(NODE_COUNT);
        final HazelcastInstance[] instances = factory.newInstances(new Config());

        final List<Future> futures = new ArrayList<Future>();

        for (int i = 0; i < NODE_COUNT; i++) {
            final HazelcastInstance instance = instances[i];
            final IExecutorService service = instance.getExecutorService("testSubmitToMemberCallable");

            String memberUuid = instance.getCluster().getLocalMember().getUuid();
            final Future f = service.submitToMember(new MemberUUIDCheckCallable(memberUuid),
                    instance.getCluster().getLocalMember());
            futures.add(f);
        }

        for (Future f : futures) {
            assertTrue((Boolean) f.get(10, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testSubmitToMemberCallable_withCallback() throws ExecutionException, InterruptedException, TimeoutException {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(NODE_COUNT);
        final HazelcastInstance[] instances = factory.newInstances(new Config());
        final BooleanSuccessResponseCountingCallback callback = new BooleanSuccessResponseCountingCallback(NODE_COUNT);
        for (int i = 0; i < NODE_COUNT; i++) {
            final HazelcastInstance instance = instances[i];
            final IExecutorService service = instance.getExecutorService("testSubmitToMemberCallable");

            final String memberUuid = instance.getCluster().getLocalMember().getUuid();
            service.submitToMember(new MemberUUIDCheckCallable(memberUuid), instance.getCluster().getLocalMember(), callback);
        }

        assertOpenEventually(callback.getResponseLatch());
        assertEquals(NODE_COUNT, callback.getSuccessResponseCount());
    }

    @Test
    public void testSubmitToMembersCallable() throws InterruptedException {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(NODE_COUNT);
        final HazelcastInstance[] instances = factory.newInstances(new Config());
        final AtomicInteger count = new AtomicInteger(0);
        final CountDownLatch latch = new CountDownLatch(NODE_COUNT);
        final MultiExecutionCallback callback = new MultiExecutionCallback() {
            public void onResponse(Member member, Object value) {
                count.incrementAndGet();
            }

            public void onComplete(Map<Member, Object> values) {
                latch.countDown();
            }
        };
        int sum = 0;
        final Set<Member> membersSet = instances[0].getCluster().getMembers();
        final Member[] members = membersSet.toArray(new Member[membersSet.size()]);
        final Random random = new Random();
        final String name = "testSubmitToMembersCallable";

        for (int i = 0; i < NODE_COUNT; i++) {
            final IExecutorService service = instances[i].getExecutorService(name);
            final int n = random.nextInt(NODE_COUNT) + 1;
            sum += n;
            Member[] m = new Member[n];
            System.arraycopy(members, 0, m, 0, n);
            service.submitToMembers(new IncrementAtomicLongCallable(name), Arrays.asList(m), callback);
        }

        assertOpenEventually(latch);
        final IAtomicLong result = instances[0].getAtomicLong(name);
        assertEquals(sum, result.get());
        assertEquals(sum, count.get());
    }

    @Test
    public void testSubmitToAllMembersCallable() throws InterruptedException {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(NODE_COUNT);
        final HazelcastInstance[] instances = factory.newInstances(new Config());
        final AtomicInteger count = new AtomicInteger(0);
        final CountDownLatch countDownLatch = new CountDownLatch(NODE_COUNT * NODE_COUNT);
        final MultiExecutionCallback callback = new MultiExecutionCallback() {
            public void onResponse(Member member, Object value) {
                count.incrementAndGet();
                countDownLatch.countDown();
            }

            public void onComplete(Map<Member, Object> values) {
            }
        };
        for (int i = 0; i < NODE_COUNT; i++) {
            final IExecutorService service = instances[i].getExecutorService("testSubmitToAllMembersCallable");
            service.submitToAllMembers(new IncrementAtomicLongCallable("testSubmitToAllMembersCallable"), callback);
        }
        assertOpenEventually(countDownLatch);
        final IAtomicLong result = instances[0].getAtomicLong("testSubmitToAllMembersCallable");
        assertEquals(NODE_COUNT * NODE_COUNT, result.get());
        assertEquals(NODE_COUNT * NODE_COUNT, count.get());
    }



    /* ############ cancellation ############ */

    @Test
    public void testCancellationAwareTask() throws ExecutionException, InterruptedException {
        SleepingTask task = new SleepingTask(5);
        ExecutorService executor = createSingleNodeExecutorService("testCancellationAwareTask");
        Future future = executor.submit(task);
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
        Callable task1 = new SleepingTask(Integer.MAX_VALUE);
        ExecutorService executor = createSingleNodeExecutorService("testCancellationAwareTask", 1);
        Future future1 = executor.submit(task1);
        try {
            future1.get(2, TimeUnit.SECONDS);
            fail("SleepingTask should not return response");
        }catch (TimeoutException ignored) {

        } catch (Exception e) {
            if (e.getCause() instanceof RejectedExecutionException) {
                fail("SleepingTask is rejected!");
            }
        }
        assertFalse(future1.isDone());

        Callable task2 = new BasicTestCallable();
        Future future2 = executor.submit(task2);

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
        Future future = executor.submit(task);
        assertResult(future, BasicTestCallable.RESULT);
    }

    /**
     * Test for the issue 129.
     * Repeatedly runs tasks and check for isDone() status after
     * get().
     */
    @Test
    public void testIsDoneMethod2() throws Exception {
        ExecutorService executor = createSingleNodeExecutorService("isDoneMethod2");
        for (int i = 0; i < TASK_COUNT; i++) {
            Callable<String> task1 = new BasicTestCallable();
            Callable<String> task2 = new BasicTestCallable();
            Future future1 = executor.submit(task1);
            Future future2 = executor.submit(task2);
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

    private void assertResult(Future future, Object expected) throws Exception {
        assertEquals(future.get(), expected);
        assertTrue(future.isDone());
    }

    @Test
    public void testIssue292() throws Exception {
        final CountingDownExecutionCallback<Member> callback = new CountingDownExecutionCallback<Member>(1);
        createSingleNodeExecutorService("testIssue292").submit(new MemberCheck(), callback);
        assertOpenEventually(callback.getLatch());
        assertTrue(callback.getResult() instanceof Member);
    }



    /**
     * Execute a task that is executing
     * something else inside. Nested Execution.
     */
    @Test(timeout = 10000)
    public void testNestedExecution() throws Exception {
        Callable<String> task = new NestedExecutorTask();
        ExecutorService executor = createSingleNodeExecutorService("testNestedExecution");
        Future future = executor.submit(task);
        future.get();
    }

    /**
     * invokeAll tests
     */
    @Test
    public void testInvokeAll() throws Exception {
        ExecutorService executor = createSingleNodeExecutorService("testInvokeAll");
        assertFalse(executor.isShutdown());
        // Only one task
        ArrayList<Callable<String>> tasks = new ArrayList<Callable<String>>();
        tasks.add(new BasicTestCallable());
        List<Future<String>> futures = executor.invokeAll(tasks);
        assertEquals(futures.size(), 1);
        assertEquals(futures.get(0).get(), BasicTestCallable.RESULT);
        // More tasks
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
        // Only one task
        ArrayList<Callable<Boolean>> tasks = new ArrayList<Callable<Boolean>>();
        tasks.add(new SleepingTask(0));
        List<Future<Boolean>> futures = executor.invokeAll(tasks, 5, TimeUnit.SECONDS);
        assertEquals(futures.size(), 1);
        assertEquals(futures.get(0).get(), Boolean.TRUE);
        // More tasks
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
        // Only one task
        ArrayList<Callable<String>> tasks = new ArrayList<Callable<String>>();
        tasks.add(new BasicTestCallable());
        List<Future<String>> futures = executor.invokeAll(tasks, 5, TimeUnit.SECONDS);
        assertEquals(futures.size(), 1);
        assertEquals(futures.get(0).get(), BasicTestCallable.RESULT);
        // More tasks
        tasks.clear();
        for (int i = 0; i < TASK_COUNT; i++) {
            tasks.add(new BasicTestCallable());
        }
        futures = executor.invokeAll(tasks, 5, TimeUnit.SECONDS);
        assertEquals(futures.size(), TASK_COUNT);
        for (int i = 0; i < TASK_COUNT; i++) {
            assertEquals(futures.get(i).get(), BasicTestCallable.RESULT);
        }
    }

    /**
     * Shutdown-related method behaviour when the cluster is running
     */
    @Test
    public void testShutdownBehaviour() throws Exception {
        ExecutorService executor = createSingleNodeExecutorService("testShutdownBehaviour");
        // Fresh instance, is not shutting down
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
    public void testClusterShutdown() throws Exception {
        ExecutorService executor = createSingleNodeExecutorService("testClusterShutdown");
        shutdownNodeFactory();
        Thread.sleep(2000);

        assertNotNull(executor);
        assertTrue(executor.isShutdown());
        assertTrue(executor.isTerminated());

        // New tasks must be rejected
        Callable<String> task = new BasicTestCallable();
        executor.submit(task);
    }

    @Test
    public void testStatsIssue2039() throws InterruptedException, ExecutionException, TimeoutException {
        final Config config = new Config();
        final String name = "testStatsIssue2039";
        config.addExecutorConfig(new ExecutorConfig(name).setQueueCapacity(1).setPoolSize(1));
        final HazelcastInstance instance = createHazelcastInstance(config);
        final IExecutorService executorService = instance.getExecutorService(name);


        executorService.execute(new SleepLatchRunnable());

        assertOpenEventually(SleepLatchRunnable.startLatch, 30);
        Future waitingInQueue = executorService.submit(new EmptyRunnable());

        Future rejected = executorService.submit(new EmptyRunnable());

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

        final LocalExecutorStats stats = executorService.getLocalExecutorStats();
        assertEquals(2, stats.getStartedTaskCount());
        assertEquals(0, stats.getPendingTaskCount());
    }

    @Test
    public void testExecutorServiceStats() throws InterruptedException, ExecutionException {
        final IExecutorService executorService = createSingleNodeExecutorService("testExecutorServiceStats");
        final int k = 10;
        LatchRunnable.latch = new CountDownLatch(k);

        for (int i = 0; i < k; i++) {
            executorService.execute(new LatchRunnable());
        }
        assertOpenEventually(LatchRunnable.latch);

        final Future<Boolean> f = executorService.submit(new SleepingTask(10));
        Thread.sleep(1000);
        f.cancel(true);
        try {
            f.get();
        } catch (CancellationException e) {
        }

        final LocalExecutorStats stats = executorService.getLocalExecutorStats();
        assertEquals(k + 1, stats.getStartedTaskCount());
        assertEquals(k, stats.getCompletedTaskCount());
        assertEquals(0, stats.getPendingTaskCount());
        assertEquals(1, stats.getCancelledTaskCount());
    }

    static class LatchRunnable implements Runnable, Serializable {

        static CountDownLatch latch;
        final int executionTime = 200;

        @Override
        public void run() {
            try {
                Thread.sleep(executionTime);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            latch.countDown();
        }
    }


    @Test
    public void testLongRunningCallable() throws ExecutionException, InterruptedException, TimeoutException {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);

        Config config = new Config();
        long callTimeout = 3000;
        config.setProperty(GroupProperties.PROP_OPERATION_CALL_TIMEOUT_MILLIS, String.valueOf(callTimeout));

        HazelcastInstance hz1 = factory.newHazelcastInstance(config);
        HazelcastInstance hz2 = factory.newHazelcastInstance(config);

        IExecutorService executor = hz1.getExecutorService("test");
        Future<Boolean> f = executor
                .submitToMember(new SleepingTask(TimeUnit.MILLISECONDS.toSeconds(callTimeout) * 3),
                        hz2.getCluster().getLocalMember());

        Boolean result = f.get(1, TimeUnit.MINUTES);
        assertTrue(result);
    }



    static class ICountDownLatchAwaitCallable implements Callable<Boolean>, HazelcastInstanceAware, Serializable {

        private final String name;

        private HazelcastInstance instance;

        public ICountDownLatchAwaitCallable(String name) {
            this.name = name;
        }

        @Override
        public Boolean call()
                throws Exception {
            return instance.getCountDownLatch(name).await(100, TimeUnit.SECONDS);
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance instance) {
            this.instance = instance;
        }
    }

    static class SleepLatchRunnable implements Runnable, Serializable {

        static CountDownLatch startLatch;
        static CountDownLatch sleepLatch;

        public SleepLatchRunnable() {
            startLatch = new CountDownLatch(1);
            sleepLatch = new CountDownLatch(1);
        }

        @Override
        public void run() {
            startLatch.countDown();
            assertOpenEventually(sleepLatch);
        }
    }

    static class EmptyRunnable implements Runnable, Serializable, PartitionAware {
        @Override
        public void run() {
        }

        @Override
        public Object getPartitionKey() {
            return "key";
        }
    }

}

