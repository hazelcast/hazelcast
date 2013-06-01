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
import com.hazelcast.core.*;
import com.hazelcast.monitor.LocalExecutorStats;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.HazelcastJUnit4ClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

@RunWith(HazelcastJUnit4ClassRunner.class)
@Category(ParallelTest.class)
public class ExecutorTest extends HazelcastTestSupport {

    public static final int simpleTestNodeCount = 3;
    public static final int COUNT = 1000;

    private IExecutorService createSingleNodeExecutorService(String name) {
        final HazelcastInstance instance = createHazelcastInstanceFactory(1).newHazelcastInstance(new Config());
        return instance.getExecutorService(name);
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
            Assert.assertEquals(Integer.valueOf(rand), future.get());
        }
        final IAtomicLong count = instances[0].getAtomicLong("count");
        Assert.assertEquals(k, count.get());
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
        latch.await(10, TimeUnit.SECONDS);
        Assert.assertEquals(0, instances[0].getAtomicLong("testSubmitToKeyOwnerRunnable").get());
        Assert.assertEquals(k, count.get());
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
        latch.await(10, TimeUnit.SECONDS);
        Assert.assertEquals(0, instances[0].getAtomicLong("testSubmitToMemberRunnable").get());
        Assert.assertEquals(k, count.get());
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
        Assert.assertEquals(sum, result.get());
        Assert.assertEquals(sum, count.get());
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
        Assert.assertEquals(k * k, result.get());
        Assert.assertEquals(k * k, count.get());
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
            Assert.assertEquals((long) (i + 1), future.get());
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
                Assert.assertTrue((Boolean) f.get(5, TimeUnit.SECONDS));
            } else {
                service.submitToKeyOwner(new ScriptCallable(script, map), key, callback);
            }
        }
        assertTrue(latch.await(30, TimeUnit.SECONDS));
        Assert.assertEquals(k / 2, count.get());
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
                Assert.assertTrue((Boolean) f.get(5, TimeUnit.SECONDS));
            } else {
                service.submitToMember(new ScriptCallable(script, map), instance.getCluster().getLocalMember(), callback);
            }
        }
        assertTrue(latch.await(30, TimeUnit.SECONDS));
        Assert.assertEquals(k / 2, count.get());
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
        Assert.assertEquals(sum, result.get());
        Assert.assertEquals(sum, count.get());
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
        Assert.assertEquals(k * k, result.get());
        Assert.assertEquals(k * k, count.get());
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
    public void testCancellationAwareTask() {
        CancellationAwareTask task = new CancellationAwareTask(5000);
        ExecutorService executor = createSingleNodeExecutorService("testCancellationAwareTask");
        Future future = executor.submit(task);
        try {
            future.get(2, TimeUnit.SECONDS);
            fail("Should throw TimeoutException!");
        } catch (TimeoutException expected) {
        } catch (Exception e) {
            fail("No other Exception!!");
        }
        assertFalse(future.isDone());
        assertFalse(future.cancel(true));
        assertFalse(future.isCancelled());
        assertTrue(future.isDone());

        try {
            future.get();
        } catch (InterruptedException e) {
            fail();
        } catch (ExecutionException e) {
            fail();
        }

    }

    /**
     * Test the method isDone()
     */
    @Test
    public void isDoneMethod() throws Exception {
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
    public void isDoneMethod2() throws Exception {
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
    public void isTwoGetFromFuture() throws Exception {
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
    public void testExecutorServiceStats() throws InterruptedException {
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

        final LocalExecutorStats stats = executorService.getLocalExecutorStats();
        Assert.assertEquals(k, stats.getStartedTaskCount());
        Assert.assertEquals(k, stats.getCompletedTaskCount());
        Assert.assertEquals(0, stats.getPendingTaskCount());
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

    public static class BasicTestTask implements Callable<String>, Serializable {

        public static String RESULT = "Task completed";

        public String call() throws Exception {
            return RESULT;
        }
    }

    public static class CancellationAwareTask implements Callable<Boolean>, Serializable {

        long sleepTime = 10000;

        public CancellationAwareTask(long sleepTime) {
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

