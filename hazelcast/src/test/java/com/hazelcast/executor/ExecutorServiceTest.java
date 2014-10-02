/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.instance.HazelcastInstanceProxy;
import com.hazelcast.monitor.LocalExecutorStats;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class ExecutorServiceTest extends HazelcastTestSupport {

    public static final int simpleTestNodeCount = 3;
    public static final int COUNT = 1000;

    private IExecutorService createSingleNodeExecutorService(String name) {
        return createSingleNodeExecutorService(name, ExecutorConfig.DEFAULT_POOL_SIZE);
    }

    private IExecutorService createSingleNodeExecutorService(String name, int poolSize) {
        final Config config = new Config();
        config.addExecutorConfig(new ExecutorConfig(name, poolSize));
        final HazelcastInstance instance = createHazelcastInstance(config);
        return instance.getExecutorService(name);
    }

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
        config.setManagedContext(new ManagedContext() {
            @Override
            public Object initialize(Object obj) {
                if (obj instanceof RunnableWithManagedContext) {
                    RunnableWithManagedContext task = (RunnableWithManagedContext) obj;
                    task.initializeCalled = true;
                }
                return obj;
            }
        });

        final HazelcastInstance instance = createHazelcastInstance(config);
        IExecutorService executor = instance.getExecutorService("test");

        RunnableWithManagedContext task = new RunnableWithManagedContext();
        executor.submit(task).get();
        assertTrue("The task should have been initialized by the ManagedContext", task.initializeCalled);
    }

    static class RunnableWithManagedContext implements Runnable {
        private volatile boolean initializeCalled = false;

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
        executor.submit(task).get();
        assertTrue("The setHazelcastInstance should have been called", task.initializeCalled);
    }

    static class HazelcastInstanceAwareRunnable implements Runnable, HazelcastInstanceAware {
        private volatile boolean initializeCalled = false;

        @Override
        public void run() {
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            initializeCalled = true;
        }
    }


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
        Callable<String> task = new BasicTestTask();
        ExecutorService executor = createSingleNodeExecutorService("testBasicTask");
        Future future = executor.submit(task);
        assertEquals(future.get(), BasicTestTask.RESULT);
    }

    @Test
    public void testExecuteMultipleNode() throws InterruptedException, ExecutionException {
        final int k = simpleTestNodeCount;
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(k);
        final HazelcastInstance[] instances = factory.newInstances(new Config());
        for (int i = 0; i < k; i++) {
            final IExecutorService service = instances[i].getExecutorService("testExecuteMultipleNode");
            final String script = "hazelcast.getAtomicLong('count').incrementAndGet();";
            final int rand = new Random().nextInt(100);
            final Future<Integer> future = service.submit(new ScriptRunnable(script, null), rand);
            assertEquals(Integer.valueOf(rand), future.get());
        }
        final IAtomicLong count = instances[0].getAtomicLong("count");
        assertEquals(k, count.get());
    }

    @Test
    public void testSubmitToKeyOwnerRunnable() throws InterruptedException {
        final int k = simpleTestNodeCount;
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(k);
        final HazelcastInstance[] instances = factory.newInstances(new Config());
        final AtomicInteger count = new AtomicInteger(0);
        final CountDownLatch latch = new CountDownLatch(k);
        final ExecutionCallback callback = new ExecutionCallback() {
            public void onResponse(Object response) {
                if (response == null)
                    count.incrementAndGet();
                latch.countDown();
            }

            public void onFailure(Throwable t) {
            }
        };
        for (int i = 0; i < k; i++) {
            final HazelcastInstance instance = instances[i];
            final IExecutorService service = instance.getExecutorService("testSubmitToKeyOwnerRunnable");
            final String script = "if(!hazelcast.getCluster().getLocalMember().equals(member)) " +
                    "hazelcast.getAtomicLong('testSubmitToKeyOwnerRunnable').incrementAndGet();";
            final HashMap map = new HashMap();
            map.put("member", instance.getCluster().getLocalMember());
            int key = 0;
            while (!instance.getCluster().getLocalMember().equals(instance.getPartitionService().getPartition(++key).getOwner())) {
                Thread.sleep(1);
            }
            service.submitToKeyOwner(new ScriptRunnable(script, map), key, callback);
        }
        assertOpenEventually(latch);
        assertEquals(0, instances[0].getAtomicLong("testSubmitToKeyOwnerRunnable").get());
        assertEquals(k, count.get());
    }

    @Test
    public void testSubmitToMemberRunnable() throws InterruptedException {
        final int k = simpleTestNodeCount;
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(k);
        final HazelcastInstance[] instances = factory.newInstances(new Config());
        final AtomicInteger count = new AtomicInteger(0);
        final CountDownLatch latch = new CountDownLatch(k);
        final ExecutionCallback callback = new ExecutionCallback() {
            public void onResponse(Object response) {
                if (response == null) {
                    count.incrementAndGet();
                }
                latch.countDown();
            }

            public void onFailure(Throwable t) {
            }
        };
        for (int i = 0; i < k; i++) {
            final HazelcastInstance instance = instances[i];
            final IExecutorService service = instance.getExecutorService("testSubmitToMemberRunnable");
            final String script = "if(!hazelcast.getCluster().getLocalMember().equals(member)) " +
                    "hazelcast.getAtomicLong('testSubmitToMemberRunnable').incrementAndGet();";
            final HashMap map = new HashMap();
            map.put("member", instance.getCluster().getLocalMember());
            service.submitToMember(new ScriptRunnable(script, map), instance.getCluster().getLocalMember(), callback);
        }
        assertOpenEventually(latch);
        assertEquals(0, instances[0].getAtomicLong("testSubmitToMemberRunnable").get());
        assertEquals(k, count.get());
    }

    @Test
    public void testSubmitToMembersRunnable() throws InterruptedException {
        final int k = simpleTestNodeCount;
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(k);
        final HazelcastInstance[] instances = factory.newInstances(new Config());
        final AtomicInteger count = new AtomicInteger(0);
        final CountDownLatch latch = new CountDownLatch(k);
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
        for (int i = 0; i < k; i++) {
            final IExecutorService service = instances[i].getExecutorService("testSubmitToMembersRunnable");
            final String script = "hazelcast.getAtomicLong('testSubmitToMembersRunnable').incrementAndGet();";
            final int n = random.nextInt(k) + 1;
            sum += n;
            Member[] m = new Member[n];
            System.arraycopy(members, 0, m, 0, n);
            service.submitToMembers(new ScriptRunnable(script, null), Arrays.asList(m), callback);
        }

        assertTrue(latch.await(30, TimeUnit.SECONDS));
        final IAtomicLong result = instances[0].getAtomicLong("testSubmitToMembersRunnable");
        assertEquals(sum, result.get());
        assertEquals(sum, count.get());
    }

    @Test
    public void testSubmitToAllMembersRunnable() throws InterruptedException {
        final int k = simpleTestNodeCount;
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(k);
        final HazelcastInstance[] instances = factory.newInstances(new Config());
        final AtomicInteger count = new AtomicInteger(0);
        final CountDownLatch latch = new CountDownLatch(k * k);
        final MultiExecutionCallback callback = new MultiExecutionCallback() {
            public void onResponse(Member member, Object value) {
                if (value == null)
                    count.incrementAndGet();
                latch.countDown();
            }

            public void onComplete(Map<Member, Object> values) {
            }
        };
        for (int i = 0; i < k; i++) {
            final IExecutorService service = instances[i].getExecutorService("testSubmitToAllMembersRunnable");
            final String script = "hazelcast.getAtomicLong('testSubmitToAllMembersRunnable').incrementAndGet();";
            service.submitToAllMembers(new ScriptRunnable(script, null), callback);
        }
        assertTrue(latch.await(30, TimeUnit.SECONDS));
        final IAtomicLong result = instances[0].getAtomicLong("testSubmitToAllMembersRunnable");
        assertEquals(k * k, result.get());
        assertEquals(k * k, count.get());
    }

    @Test
    public void testSubmitMultipleNode() throws ExecutionException, InterruptedException {
        final int k = simpleTestNodeCount;
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(k);
        final HazelcastInstance[] instances = factory.newInstances(new Config());
        for (int i = 0; i < k; i++) {
            final IExecutorService service = instances[i].getExecutorService("testSubmitMultipleNode");
            final String script = "hazelcast.getAtomicLong('testSubmitMultipleNode').incrementAndGet();";
            final Future future = service.submit(new ScriptCallable(script, null));
            assertEquals((long) (i + 1), future.get());
        }
    }

    @Test
    public void testSubmitToKeyOwnerCallable() throws Exception {
        final int k = simpleTestNodeCount;
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(k);
        final HazelcastInstance[] instances = factory.newInstances(new Config());
        final AtomicInteger count = new AtomicInteger(0);
        final CountDownLatch latch = new CountDownLatch(k / 2);
        final ExecutionCallback callback = new ExecutionCallback() {
            public void onResponse(Object response) {
                if ((Boolean) response)
                    count.incrementAndGet();
                latch.countDown();
            }

            public void onFailure(Throwable t) {
            }
        };
        for (int i = 0; i < k; i++) {
            final HazelcastInstance instance = instances[i];
            final IExecutorService service = instance.getExecutorService("testSubmitToKeyOwnerCallable");
            final String script = "hazelcast.getCluster().getLocalMember().equals(member)";
            final HashMap map = new HashMap();
            final Member localMember = instance.getCluster().getLocalMember();
            map.put("member", localMember);
            int key = 0;
            while (!localMember.equals(instance.getPartitionService().getPartition(++key).getOwner())) ;
            if (i % 2 == 0) {
                final Future f = service.submitToKeyOwner(new ScriptCallable(script, map), key);
                assertTrue((Boolean) f.get(60, TimeUnit.SECONDS));
            } else {
                service.submitToKeyOwner(new ScriptCallable(script, map), key, callback);
            }
        }
        assertOpenEventually(latch);
        assertEquals(k / 2, count.get());
    }

    @Test
    public void testSubmitToMemberCallable() throws ExecutionException, InterruptedException, TimeoutException {
        final int k = simpleTestNodeCount;
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(k);
        final HazelcastInstance[] instances = factory.newInstances(new Config());
        final AtomicInteger count = new AtomicInteger(0);
        final CountDownLatch latch = new CountDownLatch(k / 2);
        final ExecutionCallback callback = new ExecutionCallback() {
            public void onResponse(Object response) {
                if ((Boolean) response)
                    count.incrementAndGet();
                latch.countDown();
            }

            public void onFailure(Throwable t) {
            }
        };
        for (int i = 0; i < k; i++) {
            final HazelcastInstance instance = instances[i];
            final IExecutorService service = instance.getExecutorService("testSubmitToMemberCallable");
            final String script = "hazelcast.getCluster().getLocalMember().equals(member); ";
            final HashMap map = new HashMap();
            map.put("member", instance.getCluster().getLocalMember());
            if (i % 2 == 0) {
                final Future f = service.submitToMember(new ScriptCallable(script, map), instance.getCluster().getLocalMember());
                assertTrue((Boolean) f.get(5, TimeUnit.SECONDS));
            } else {
                service.submitToMember(new ScriptCallable(script, map), instance.getCluster().getLocalMember(), callback);
            }
        }
        assertTrue(latch.await(30, TimeUnit.SECONDS));
        assertEquals(k / 2, count.get());
    }

    @Test
    public void testSubmitToMembersCallable() throws InterruptedException {
        final int k = simpleTestNodeCount;
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(k);
        final HazelcastInstance[] instances = factory.newInstances(new Config());
        final AtomicInteger count = new AtomicInteger(0);
        final CountDownLatch latch = new CountDownLatch(k);
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
        for (int i = 0; i < k; i++) {
            final IExecutorService service = instances[i].getExecutorService(name);
            final String script = "hazelcast.getAtomicLong('" + name + "').incrementAndGet();";
            final int n = random.nextInt(k) + 1;
            sum += n;
            Member[] m = new Member[n];
            System.arraycopy(members, 0, m, 0, n);
            service.submitToMembers(new ScriptCallable(script, null), Arrays.asList(m), callback);
        }

        assertTrue(latch.await(30, TimeUnit.SECONDS));
        final IAtomicLong result = instances[0].getAtomicLong(name);
        assertEquals(sum, result.get());
        assertEquals(sum, count.get());
    }

    @Test
    public void testSubmitToAllMembersCallable() throws InterruptedException {
        final int k = simpleTestNodeCount;
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(k);
        final HazelcastInstance[] instances = factory.newInstances(new Config());
        final AtomicInteger count = new AtomicInteger(0);
        final CountDownLatch countDownLatch = new CountDownLatch(k * k);
        final MultiExecutionCallback callback = new MultiExecutionCallback() {
            public void onResponse(Member member, Object value) {
                count.incrementAndGet();
                countDownLatch.countDown();
            }

            public void onComplete(Map<Member, Object> values) {
            }
        };
        for (int i = 0; i < k; i++) {
            final IExecutorService service = instances[i].getExecutorService("testSubmitToAllMembersCallable");
            final String script = "hazelcast.getAtomicLong('testSubmitToAllMembersCallable').incrementAndGet();";
            service.submitToAllMembers(new ScriptCallable(script, null), callback);
        }
        countDownLatch.await(30, TimeUnit.SECONDS);
        final IAtomicLong result = instances[0].getAtomicLong("testSubmitToAllMembersCallable");
        assertEquals(k * k, result.get());
        assertEquals(k * k, count.get());
    }

    @Test
    public void testIssue292() throws Exception {
        final BlockingQueue qResponse = new ArrayBlockingQueue(1);
        createSingleNodeExecutorService("testIssue292").submit(new MemberCheck(), new ExecutionCallback<Member>() {
            public void onResponse(Member response) {
                qResponse.offer(response);
            }

            public void onFailure(Throwable t) {
            }
        });
        Object response = qResponse.poll(10, TimeUnit.SECONDS);
        assertNotNull(response);
        assertTrue(response instanceof Member);
    }

    @Test
    public void testCancellationAwareTask() throws ExecutionException, InterruptedException {
        SleepingTask task = new SleepingTask(5000);
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
        Callable task1 = new SleepingTask(5000);
        ExecutorService executor = createSingleNodeExecutorService("testCancellationAwareTask", 1);
        executor.submit(task1);

        Callable task2 = new BasicTestTask();
        Future future = executor.submit(task2);
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

    /**
     * Test the method isDone()
     */
    @Test
    public void testIsDoneMethod() throws Exception {
        Callable<String> task = new BasicTestTask();
        IExecutorService executor = createSingleNodeExecutorService("isDoneMethod");
        Future future = executor.submit(task);
        if (future.isDone()) {
            assertTrue(future.isDone());
        }
        assertEquals(future.get(), BasicTestTask.RESULT);
        assertTrue(future.isDone());
    }

    /**
     * Test for the issue 129.
     * Repeatedly runs tasks and check for isDone() status after
     * get().
     */
    @Test
    public void testIsDoneMethod2() throws Exception {
        ExecutorService executor = createSingleNodeExecutorService("isDoneMethod2");
        for (int i = 0; i < COUNT; i++) {
            Callable<String> task1 = new BasicTestTask();
            Callable<String> task2 = new BasicTestTask();
            Future future1 = executor.submit(task1);
            Future future2 = executor.submit(task2);
            assertEquals(future2.get(), BasicTestTask.RESULT);
            assertTrue(future2.isDone());
            assertEquals(future1.get(), BasicTestTask.RESULT);
            assertTrue(future1.isDone());
        }
    }

    /**
     * Test the Execution Callback
     */
    @Test
    public void testExecutionCallback() throws Exception {
        Callable<String> task = new BasicTestTask();
        IExecutorService executor = createSingleNodeExecutorService("testExecutionCallback");
        final CountDownLatch latch = new CountDownLatch(1);
        final ExecutionCallback executionCallback = new ExecutionCallback() {
            public void onResponse(Object response) {
                latch.countDown();
            }

            public void onFailure(Throwable t) {
            }
        };
        executor.submit(task, executionCallback);

        assertTrue(latch.await(2, TimeUnit.SECONDS));
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
     * Test multiple Future.get() invocation
     */
    @Test
    public void testMultipleFutureGets() throws Exception {
        Callable<String> task = new BasicTestTask();
        ExecutorService executor = createSingleNodeExecutorService("isTwoGetFromFuture");
        Future<String> future = executor.submit(task);
        String s1 = future.get();
        assertEquals(s1, BasicTestTask.RESULT);
        assertTrue(future.isDone());
        String s2 = future.get();
        assertEquals(s2, BasicTestTask.RESULT);
        assertTrue(future.isDone());
        String s3 = future.get();
        assertEquals(s3, BasicTestTask.RESULT);
        assertTrue(future.isDone());
        String s4 = future.get();
        assertEquals(s4, BasicTestTask.RESULT);
        assertTrue(future.isDone());
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
        tasks.add(new BasicTestTask());
        List<Future<String>> futures = executor.invokeAll(tasks);
        assertEquals(futures.size(), 1);
        assertEquals(futures.get(0).get(), BasicTestTask.RESULT);
        // More tasks
        tasks.clear();
        for (int i = 0; i < COUNT; i++) {
            tasks.add(new BasicTestTask());
        }
        futures = executor.invokeAll(tasks);
        assertEquals(futures.size(), COUNT);
        for (int i = 0; i < COUNT; i++) {
            assertEquals(futures.get(i).get(), BasicTestTask.RESULT);
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
        for (int i = 0; i < COUNT; i++) {
            tasks.add(new SleepingTask(i < 2 ? 0 : 20000));
        }
        futures = executor.invokeAll(tasks, 5, TimeUnit.SECONDS);
        assertEquals(futures.size(), COUNT);
        for (int i = 0; i < COUNT; i++) {
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
        tasks.add(new BasicTestTask());
        List<Future<String>> futures = executor.invokeAll(tasks, 5, TimeUnit.SECONDS);
        assertEquals(futures.size(), 1);
        assertEquals(futures.get(0).get(), BasicTestTask.RESULT);
        // More tasks
        tasks.clear();
        for (int i = 0; i < COUNT; i++) {
            tasks.add(new BasicTestTask());
        }
        futures = executor.invokeAll(tasks, 5, TimeUnit.SECONDS);
        assertEquals(futures.size(), COUNT);
        for (int i = 0; i < COUNT; i++) {
            assertEquals(futures.get(i).get(), BasicTestTask.RESULT);
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
        Callable<String> task = new BasicTestTask();
        executor.submit(task);
    }

    @Test
    public void testStatsIssue2039() throws InterruptedException, ExecutionException, TimeoutException {
        final Config config = new Config();
        final String name = "testStatsIssue2039";
        config.addExecutorConfig(new ExecutorConfig(name).setQueueCapacity(1).setPoolSize(1));
        final HazelcastInstance instance = createHazelcastInstance(config);
        final IExecutorService executorService = instance.getExecutorService(name);

        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch sleepLatch = new CountDownLatch(1);

        executorService.execute(new Runnable() {
            @Override
            public void run() {
                startLatch.countDown();
                assertOpenEventually(sleepLatch);
            }
        });

        assertTrue(startLatch.await(30, TimeUnit.SECONDS));
        final Future waitingInQueue = executorService.submit(new Runnable() {
            public void run() {
            }
        });

        final Future rejected = executorService.submit(new Runnable() {
            public void run() {
            }
        });

        try {
            rejected.get(1, TimeUnit.MINUTES);
        } catch (Exception e) {
            boolean isRejected = e.getCause() instanceof RejectedExecutionException;
            if (!isRejected) {
                fail(e.toString());
            }
        } finally {
            sleepLatch.countDown();
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
        final CountDownLatch latch = new CountDownLatch(k);
        final int executionTime = 200;
        for (int i = 0; i < k; i++) {
            executorService.execute(new Runnable() {
                public void run() {
                    try {
                        Thread.sleep(executionTime);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    latch.countDown();
                }
            });

        }
        latch.await(2, TimeUnit.MINUTES);

        final Future<Boolean> f = executorService.submit(new SleepingTask(10000));
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

    @Test
    public void testPreregisteredExecutionCallbackCompletableFuture() throws Exception {
        HazelcastInstanceProxy proxy = (HazelcastInstanceProxy) createHazelcastInstance();
        Field originalField = HazelcastInstanceProxy.class.getDeclaredField("original");
        originalField.setAccessible(true);
        HazelcastInstanceImpl hz = (HazelcastInstanceImpl) originalField.get(proxy);
        NodeEngine nodeEngine = hz.node.nodeEngine;
        ExecutionService es = nodeEngine.getExecutionService();

        final CountDownLatch latch1 = new CountDownLatch(1);
        final CountDownLatch latch2 = new CountDownLatch(1);
        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        try {
            Future future = executorService.submit(new Callable<String>() {
                @Override
                public String call() {
                    try {
                        latch1.await(30, TimeUnit.SECONDS);
                        return "success";
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            });

            final AtomicReference reference = new AtomicReference();
            final ICompletableFuture completableFuture = es.asCompletableFuture(future);
            completableFuture.andThen(new ExecutionCallback() {
                @Override
                public void onResponse(Object response) {
                    reference.set(response);
                    latch2.countDown();
                }

                @Override
                public void onFailure(Throwable t) {
                    reference.set(t);
                    latch2.countDown();
                }
            });

            latch1.countDown();
            latch2.await(30, TimeUnit.SECONDS);
            assertEquals("success", reference.get());

        } finally {
            executorService.shutdown();
        }
    }

    @Test
    public void testMultiPreregisteredExecutionCallbackCompletableFuture() throws Exception {
        HazelcastInstanceProxy proxy = (HazelcastInstanceProxy) createHazelcastInstance();
        Field originalField = HazelcastInstanceProxy.class.getDeclaredField("original");
        originalField.setAccessible(true);
        HazelcastInstanceImpl hz = (HazelcastInstanceImpl) originalField.get(proxy);
        NodeEngine nodeEngine = hz.node.nodeEngine;
        ExecutionService es = nodeEngine.getExecutionService();

        final CountDownLatch latch1 = new CountDownLatch(1);
        final CountDownLatch latch2 = new CountDownLatch(2);
        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        try {
            Future future = executorService.submit(new Callable<String>() {
                @Override
                public String call() {
                    try {
                        latch1.await(30, TimeUnit.SECONDS);
                        return "success";
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            });

            final AtomicReference reference1 = new AtomicReference();
            final AtomicReference reference2 = new AtomicReference();
            final ICompletableFuture completableFuture = es.asCompletableFuture(future);
            completableFuture.andThen(new ExecutionCallback() {
                @Override
                public void onResponse(Object response) {
                    reference1.set(response);
                    latch2.countDown();
                }

                @Override
                public void onFailure(Throwable t) {
                    reference1.set(t);
                    latch2.countDown();
                }
            });
            completableFuture.andThen(new ExecutionCallback() {
                @Override
                public void onResponse(Object response) {
                    reference2.set(response);
                    latch2.countDown();
                }

                @Override
                public void onFailure(Throwable t) {
                    reference2.set(t);
                    latch2.countDown();
                }
            });

            latch1.countDown();
            latch2.await(30, TimeUnit.SECONDS);
            assertEquals("success", reference1.get());
            assertEquals("success", reference2.get());

        } finally {
            executorService.shutdown();
        }
    }

    @Test
    public void testPostregisteredExecutionCallbackCompletableFuture() throws Exception {
        HazelcastInstanceProxy proxy = (HazelcastInstanceProxy) createHazelcastInstance();
        Field originalField = HazelcastInstanceProxy.class.getDeclaredField("original");
        originalField.setAccessible(true);
        HazelcastInstanceImpl hz = (HazelcastInstanceImpl) originalField.get(proxy);
        NodeEngine nodeEngine = hz.node.nodeEngine;
        ExecutionService es = nodeEngine.getExecutionService();

        final CountDownLatch latch1 = new CountDownLatch(1);
        final CountDownLatch latch2 = new CountDownLatch(1);
        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        try {
            Future future = executorService.submit(new Callable<String>() {
                @Override
                public String call() {
                    try {
                        return "success";
                    } finally {
                        latch1.countDown();
                    }
                }
            });

            final ICompletableFuture completableFuture = es.asCompletableFuture(future);
            latch1.await(30, TimeUnit.SECONDS);

            final AtomicReference reference = new AtomicReference();
            completableFuture.andThen(new ExecutionCallback() {
                @Override
                public void onResponse(Object response) {
                    reference.set(response);
                    latch2.countDown();
                }

                @Override
                public void onFailure(Throwable t) {
                    reference.set(t);
                    latch2.countDown();
                }
            });

            latch2.await(30, TimeUnit.SECONDS);
            if (reference.get() instanceof Throwable) {
                ((Throwable) reference.get()).printStackTrace();
            }

            assertEquals("success", reference.get());

        } finally {
            executorService.shutdown();
        }
    }

    @Test
    public void testMultiPostregisteredExecutionCallbackCompletableFuture() throws Exception {
        HazelcastInstanceProxy proxy = (HazelcastInstanceProxy) createHazelcastInstance();
        Field originalField = HazelcastInstanceProxy.class.getDeclaredField("original");
        originalField.setAccessible(true);
        HazelcastInstanceImpl hz = (HazelcastInstanceImpl) originalField.get(proxy);
        NodeEngine nodeEngine = hz.node.nodeEngine;
        ExecutionService es = nodeEngine.getExecutionService();

        final CountDownLatch latch1 = new CountDownLatch(1);
        final CountDownLatch latch2 = new CountDownLatch(2);
        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        try {
            Future future = executorService.submit(new Callable<String>() {
                @Override
                public String call() {
                    try {
                        return "success";
                    } finally {
                        latch1.countDown();
                    }
                }
            });

            latch1.await(30, TimeUnit.SECONDS);

            final AtomicReference reference1 = new AtomicReference();
            final AtomicReference reference2 = new AtomicReference();
            final ICompletableFuture completableFuture = es.asCompletableFuture(future);
            completableFuture.andThen(new ExecutionCallback() {
                @Override
                public void onResponse(Object response) {
                    reference1.set(response);
                    latch2.countDown();
                }

                @Override
                public void onFailure(Throwable t) {
                    reference1.set(t);
                    latch2.countDown();
                }
            });
            completableFuture.andThen(new ExecutionCallback() {
                @Override
                public void onResponse(Object response) {
                    reference2.set(response);
                    latch2.countDown();
                }

                @Override
                public void onFailure(Throwable t) {
                    reference2.set(t);
                    latch2.countDown();
                }
            });

            latch2.await(30, TimeUnit.SECONDS);
            assertEquals("success", reference1.get());
            assertEquals("success", reference2.get());

        } finally {
            executorService.shutdown();
        }
    }

    @Test
    public void testManagedPreregisteredExecutionCallbackCompletableFuture() throws Exception {
        HazelcastInstanceProxy proxy = (HazelcastInstanceProxy) createHazelcastInstance();
        Field originalField = HazelcastInstanceProxy.class.getDeclaredField("original");
        originalField.setAccessible(true);
        HazelcastInstanceImpl hz = (HazelcastInstanceImpl) originalField.get(proxy);
        NodeEngine nodeEngine = hz.node.nodeEngine;
        ExecutionService es = nodeEngine.getExecutionService();

        final CountDownLatch latch1 = new CountDownLatch(1);
        final CountDownLatch latch2 = new CountDownLatch(1);
        Future future = es.submit("default", new Callable<String>() {
            @Override
            public String call() {
                try {
                    latch1.await(30, TimeUnit.SECONDS);
                    return "success";
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });

        final AtomicReference reference = new AtomicReference();
        final ICompletableFuture completableFuture = es.asCompletableFuture(future);
        completableFuture.andThen(new ExecutionCallback() {
            @Override
            public void onResponse(Object response) {
                reference.set(response);
                latch2.countDown();
            }

            @Override
            public void onFailure(Throwable t) {
                reference.set(t);
                latch2.countDown();
            }
        });

        latch1.countDown();
        latch2.await(30, TimeUnit.SECONDS);
        assertEquals("success", reference.get());
    }

    @Test
    public void testManagedMultiPreregisteredExecutionCallbackCompletableFuture() throws Exception {
        HazelcastInstanceProxy proxy = (HazelcastInstanceProxy) createHazelcastInstance();
        Field originalField = HazelcastInstanceProxy.class.getDeclaredField("original");
        originalField.setAccessible(true);
        HazelcastInstanceImpl hz = (HazelcastInstanceImpl) originalField.get(proxy);
        NodeEngine nodeEngine = hz.node.nodeEngine;
        ExecutionService es = nodeEngine.getExecutionService();

        final CountDownLatch latch1 = new CountDownLatch(1);
        final CountDownLatch latch2 = new CountDownLatch(2);
        Future future = es.submit("default", new Callable<String>() {
            @Override
            public String call() {
                try {
                    latch1.await(30, TimeUnit.SECONDS);
                    return "success";
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });

        final AtomicReference reference1 = new AtomicReference();
        final AtomicReference reference2 = new AtomicReference();
        final ICompletableFuture completableFuture = es.asCompletableFuture(future);
        completableFuture.andThen(new ExecutionCallback() {
            @Override
            public void onResponse(Object response) {
                reference1.set(response);
                latch2.countDown();
            }

            @Override
            public void onFailure(Throwable t) {
                reference1.set(t);
                latch2.countDown();
            }
        });
        completableFuture.andThen(new ExecutionCallback() {
            @Override
            public void onResponse(Object response) {
                reference2.set(response);
                latch2.countDown();
            }

            @Override
            public void onFailure(Throwable t) {
                reference2.set(t);
                latch2.countDown();
            }
        });

        latch1.countDown();
        latch2.await(30, TimeUnit.SECONDS);
        assertEquals("success", reference1.get());
        assertEquals("success", reference2.get());
    }

    @Test
    public void testManagedPostregisteredExecutionCallbackCompletableFuture() throws Exception {
        HazelcastInstanceProxy proxy = (HazelcastInstanceProxy) createHazelcastInstance();
        Field originalField = HazelcastInstanceProxy.class.getDeclaredField("original");
        originalField.setAccessible(true);
        HazelcastInstanceImpl hz = (HazelcastInstanceImpl) originalField.get(proxy);
        NodeEngine nodeEngine = hz.node.nodeEngine;
        ExecutionService es = nodeEngine.getExecutionService();

        final CountDownLatch latch1 = new CountDownLatch(1);
        final CountDownLatch latch2 = new CountDownLatch(1);
        Future future = es.submit("default", new Callable<String>() {
            @Override
            public String call() {
                try {
                    return "success";
                } finally {
                    latch1.countDown();
                }
            }
        });

        latch1.await(30, TimeUnit.SECONDS);

        final AtomicReference reference = new AtomicReference();
        final ICompletableFuture completableFuture = es.asCompletableFuture(future);
        completableFuture.andThen(new ExecutionCallback() {
            @Override
            public void onResponse(Object response) {
                reference.set(response);
                latch2.countDown();
            }

            @Override
            public void onFailure(Throwable t) {
                reference.set(t);
                latch2.countDown();
            }
        });

        latch2.await(30, TimeUnit.SECONDS);
        assertEquals("success", reference.get());
    }

    @Test
    public void testManagedMultiPostregisteredExecutionCallbackCompletableFuture() throws Exception {
        HazelcastInstanceProxy proxy = (HazelcastInstanceProxy) createHazelcastInstance();
        Field originalField = HazelcastInstanceProxy.class.getDeclaredField("original");
        originalField.setAccessible(true);
        HazelcastInstanceImpl hz = (HazelcastInstanceImpl) originalField.get(proxy);
        NodeEngine nodeEngine = hz.node.nodeEngine;
        ExecutionService es = nodeEngine.getExecutionService();

        final CountDownLatch latch1 = new CountDownLatch(1);
        final CountDownLatch latch2 = new CountDownLatch(2);
        Future future = es.submit("default", new Callable<String>() {
            @Override
            public String call() {
                try {
                    return "success";
                } finally {
                    latch1.countDown();
                }
            }
        });

        assertOpenEventually(latch1);

        final AtomicReference reference1 = new AtomicReference();
        final AtomicReference reference2 = new AtomicReference();
        final ICompletableFuture completableFuture = es.asCompletableFuture(future);
        completableFuture.andThen(new ExecutionCallback() {
            @Override
            public void onResponse(Object response) {
                reference1.set(response);
                latch2.countDown();
            }

            @Override
            public void onFailure(Throwable t) {
                reference1.set(t);
                latch2.countDown();
            }
        });
        completableFuture.andThen(new ExecutionCallback() {
            @Override
            public void onResponse(Object response) {
                reference2.set(response);
                latch2.countDown();
            }

            @Override
            public void onFailure(Throwable t) {
                reference2.set(t);
                latch2.countDown();
            }
        });

        assertOpenEventually(latch2);
        assertEquals("success", reference1.get());
        assertEquals("success", reference2.get());
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
                .submitToMember(new SleepingTask(callTimeout * 3), hz2.getCluster().getLocalMember());

        Boolean result = f.get(1, TimeUnit.MINUTES);
        assertTrue(result);
    }

    private static class ScriptRunnable implements Runnable, Serializable, HazelcastInstanceAware {
        private final String script;
        private final Map<String, Object> map;
        private transient HazelcastInstance hazelcastInstance;

        ScriptRunnable(String script, Map<String, Object> map) {
            this.script = script;
            this.map = map;
        }

        public void run() {
            final ScriptEngineManager scriptEngineManager = new ScriptEngineManager();
            ScriptEngine e = scriptEngineManager.getEngineByName("javascript");
            if (map != null) {
                for (Map.Entry<String, Object> entry : map.entrySet()) {
                    e.put(entry.getKey(), entry.getValue());
                }
            }
            e.put("hazelcast", hazelcastInstance);
            try {
                // For new JavaScript engine called Nashorn we need the compatibility script
                if (e.getFactory().getEngineName().toLowerCase().contains("nashorn")) {
                    e.eval("load('nashorn:mozilla_compat.js');");
                }

                e.eval("importPackage(java.lang);");
                e.eval("importPackage(java.util);");
                e.eval("importPackage(com.hazelcast.core);");
                e.eval("importPackage(com.hazelcast.config);");
                e.eval("importPackage(java.util.concurrent);");
                e.eval("importPackage(org.junit);");
                e.eval(script);
            } catch (ScriptException e1) {
                e1.printStackTrace();
            }

        }

        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            this.hazelcastInstance = hazelcastInstance;
        }
    }

    private static class ScriptCallable implements Callable, Serializable, HazelcastInstanceAware {
        private final String script;
        private final Map<String, Object> map;
        private transient HazelcastInstance hazelcastInstance;

        ScriptCallable(String script, Map<String, Object> map) {
            this.script = script;
            this.map = map;
        }

        public Object call() {
            final ScriptEngineManager scriptEngineManager = new ScriptEngineManager();
            ScriptEngine e = scriptEngineManager.getEngineByName("javascript");
            if (map != null) {
                for (Map.Entry<String, Object> entry : map.entrySet()) {
                    e.put(entry.getKey(), entry.getValue());
                }
            }
            e.put("hazelcast", hazelcastInstance);
            try {
                // For new JavaScript engine called Nashorn we need the compatibility script
                if (e.getFactory().getEngineName().toLowerCase().contains("nashorn")) {
                    e.eval("load('nashorn:mozilla_compat.js');");
                }

                e.eval("importPackage(java.lang);");
                e.eval("importPackage(java.util);");
                e.eval("importPackage(com.hazelcast.core);");
                e.eval("importPackage(com.hazelcast.config);");
                e.eval("importPackage(java.util.concurrent);");
                e.eval("importPackage(org.junit);");

                return e.eval(script);
            } catch (ScriptException e1) {
                throw new RuntimeException(e1);
            }
        }

        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            this.hazelcastInstance = hazelcastInstance;
        }
    }


    @Test
    public void testSubmitFailingCallableException_withExecutionCallback() throws ExecutionException, InterruptedException {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        HazelcastInstance instance = factory.newHazelcastInstance();
        IExecutorService service = instance.getExecutorService(randomString());
        final CountDownLatch latch = new CountDownLatch(1);
        service.submit(new FailingTestTask(), new ExecutionCallback<String>() {
            @Override
            public void onResponse(String response) {
            }

            @Override
            public void onFailure(Throwable t) {
                latch.countDown();
            }
        });
        assertTrue(latch.await(10,TimeUnit.SECONDS));
    }


    public static class FailingTestTask implements Callable<String>, Serializable {

        public String call() throws Exception {
            throw  new IllegalStateException();
        }
    }

    public static class BasicTestTask implements Callable<String>, Serializable {

        public static String RESULT = "Task completed";

        public String call() throws Exception {
            return RESULT;
        }
    }

    public static class SleepingTask implements Callable<Boolean>, Serializable {

        long sleepTime = 10000;

        public SleepingTask(long sleepTime) {
            this.sleepTime = sleepTime;
        }

        public Boolean call() throws InterruptedException {
            Thread.sleep(sleepTime);
            return Boolean.TRUE;
        }
    }

    public static class NestedExecutorTask implements Callable<String>, Serializable, HazelcastInstanceAware {

        private HazelcastInstance instance;

        public String call() throws Exception {
            Future future = instance.getExecutorService("NestedExecutorTask").submit(new BasicTestTask());
            return (String) future.get();
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            instance = hazelcastInstance;
        }
    }

    public static class MemberCheck implements Callable<Member>, Serializable, HazelcastInstanceAware {

        private Member localMember;

        public Member call() throws Exception {
            return localMember;
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            localMember = hazelcastInstance.getCluster().getLocalMember();
        }
    }
}

