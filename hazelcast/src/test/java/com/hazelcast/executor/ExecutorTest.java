/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.instance.StaticNodeFactory;
import org.junit.*;
import org.junit.runner.RunWith;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

import static org.junit.Assert.*;

@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
public class ExecutorTest {

    public static int COUNT = 1000;

    @BeforeClass
    @AfterClass
    public static void init() throws Exception {
        Hazelcast.shutdownAll();
    }

    public static IExecutorService getExecutorService(String name) {
        final HazelcastInstance instance = new StaticNodeFactory(1).newHazelcastInstance(new Config());
        return instance.getExecutorService(name);
    }

    @Before
    @After
    public void shutdown() {
        Hazelcast.shutdownAll();
    }

    @Test
    public void testExecuteMultipleNode() {
        final int k = 4;
        final HazelcastInstance[] instances = StaticNodeFactory.newInstances(new Config(), k);

        for (int i = 0; i < k; i++) {
            final IExecutorService service = instances[i].getExecutorService("testExecuteOnMember");
            final String script = "hazelcast.getAtomicLong('count').incrementAndGet();";
            service.execute(new ScriptRunnable(script, null));
        }
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        final IAtomicLong count = instances[0].getAtomicLong("count");
        Assert.assertEquals(k, count.get());
    }

    @Test
    public void testExecuteOnMember() {
        final int k = 4;
        final HazelcastInstance[] instances = StaticNodeFactory.newInstances(new Config(), k);

        for (int i = 0; i < k; i++) {
            final HazelcastInstance instance = instances[k - i - 1];
            final IExecutorService service = instance.getExecutorService("testExecuteOnMember");
            final String script = "if(!hazelcast.getCluster().getLocalMember().equals(member)) " +
                    "hazelcast.getAtomicLong('testExecuteOnMember').incrementAndGet();";
            final HashMap map = new HashMap();
            map.put("member", instance.getCluster().getLocalMember());
            service.executeOnMember(new ScriptRunnable(script, map), instance.getCluster().getLocalMember());
        }
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        Assert.assertEquals(0, instances[0].getAtomicLong("testExecuteOnMember").get());
    }

    @Test
    public void testExecuteOnAllMembers() {
        final int k = 4;
        final HazelcastInstance[] instances = StaticNodeFactory.newInstances(new Config(), k);

        for (int i = 0; i < k; i++) {
            final IExecutorService service = instances[i].getExecutorService("testExecuteOnMember");
            final String script = "hazelcast.getAtomicLong('count').incrementAndGet();";
            service.executeOnAllMembers(new ScriptRunnable(script, null));
        }
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        final IAtomicLong count = instances[0].getAtomicLong("count");
        Assert.assertEquals(k * k, count.get());
    }

    /**
     * Get a service instance.
     */
    @Test
    public void testGetExecutorService() {
        ExecutorService executor = getExecutorService("testDefault");
        assertNotNull(executor);
    }

    @Test
    public void testIssue292() throws Exception {
        final BlockingQueue qResponse = new ArrayBlockingQueue(1);
        new StaticNodeFactory(1).newHazelcastInstance(new Config());
        getExecutorService("testIssue292").submit(new MemberCheck(), new ExecutionCallback<Member>() {
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
        ExecutorService executor = getExecutorService("submitNullTask");
        Callable c = null;
        executor.submit(c);
    }

    /**
     * Run a basic task
     */
    @Test
    public void testBasicTask() throws Exception {
        Callable<String> task = new BasicTestTask();
        ExecutorService executor = getExecutorService("testBasicTask");
        Future future = executor.submit(task);
        assertEquals(future.get(), BasicTestTask.RESULT);
    }

    @Test
    public void testCancellationAwareTask() {
        CancellationAwareTask task = new CancellationAwareTask(5000);
        ExecutorService executor = getExecutorService("testCancellationAwareTask");
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
        IExecutorService executor = getExecutorService("isDoneMethod");
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
        ExecutorService executor = getExecutorService("isDoneMethod2");
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
        IExecutorService executor = getExecutorService("testExecutionCallback");
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
        ExecutorService executor = getExecutorService("testNestedExecution");
        Future future = executor.submit(task);
        future.get();
    }

    /**
     * Test multiple Future.get() invocation
     */
    @Test
    public void isTwoGetFromFuture() throws Exception {
        Callable<String> task = new BasicTestTask();
        ExecutorService executor = getExecutorService("isTwoGetFromFuture");
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
        Callable<String> task = new BasicTestTask();
        ExecutorService executor = getExecutorService("testInvokeAll");
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
        ExecutorService executor = getExecutorService("testShutdownBehaviour");
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
        ExecutorService executor = getExecutorService("testClusterShutdown");
        Hazelcast.shutdownAll();
        Thread.sleep(2000);
        assertNotNull(executor);
        assertTrue(executor.isShutdown());
        assertTrue(executor.isTerminated());
        // New tasks must be rejected
        Callable<String> task = new BasicTestTask();

        Future future = executor.submit(task);

    }

    @Test
    public void testExecutorServiceStats() throws InterruptedException {
        final IExecutorService executorService = StaticNodeFactory.newInstances(new Config(), 1)[0].getExecutorService("testExecutorServiceStats");
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
        Assert.assertEquals(k, executorService.getLocalExecutorStats().getTotalStarted());
        Assert.assertEquals(k, executorService.getLocalExecutorStats().getTotalFinished());
        Assert.assertEquals(k, executorService.getLocalExecutorStats().getOperationStats().getStarted());
        Assert.assertEquals(k, executorService.getLocalExecutorStats().getOperationStats().getCompleted());

        Assert.assertTrue(executionTime <= executorService.getLocalExecutorStats().getOperationStats().getAverageExecutionTime());
        Assert.assertTrue(executionTime <= executorService.getLocalExecutorStats().getOperationStats().getMinExecutionTime());
        Assert.assertTrue(executionTime <= executorService.getLocalExecutorStats().getOperationStats().getMaxExecutionTime());

    }

    static class ScriptRunnable implements Runnable, Serializable, HazelcastInstanceAware {
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

    static class ScriptCallable implements Callable, Serializable, HazelcastInstanceAware {
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
                e.eval("importPackage(import org.junit);");

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

    public static class NestedExecutorTask implements Callable<String>, Serializable {

        public String call() throws Exception {
            Future future = getExecutorService("NestedExecutorTask").submit(new BasicTestTask());
            return (String) future.get();
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

