/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.core;

import org.junit.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import static junit.framework.Assert.*;
import static org.junit.Assert.assertEquals;

/**
 * Testing suite for ExecutorService
 *
 * @author Marco Ferrante, DISI - University of Genoa
 * @oztalip
 */
public class ExecutorServiceTest {

    public static int COUNT = 1000;

    /**
     * Get a service instance.
     */
    @Test
    public void testGetExecutorService() {
        ExecutorService executor = Hazelcast.getExecutorService();
        assertNotNull(executor);
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
            Future future = Hazelcast.getExecutorService().submit(new BasicTestTask());
            return (String) future.get();
        }
    }

    public static class MemberCheck implements Callable<Member>, Serializable {
        public Member call() throws Exception {
            return Hazelcast.getCluster().getLocalMember();
        }
    }

    @Test
    public void testIssue292() throws Exception {
        final BlockingQueue qResponse = new ArrayBlockingQueue(1);
        Hazelcast.getDefaultInstance();//instance creation
        Thread thread = new Thread(new Runnable() {
            public void run() {
                try {
                    DistributedTask<Member> dtask = new DistributedTask<Member>(new MemberCheck());
                    Hazelcast.getExecutorService().submit(dtask);//instance usage
                    Member member = dtask.get();
                    qResponse.offer(member);
                } catch (Throwable e) {
                    qResponse.offer(e);
                }
            }
        });
        thread.start();
        Object response = qResponse.poll(10, TimeUnit.SECONDS);
        assertNotNull(response);
        assertTrue(response instanceof Member);
    }

    /**
     * Submit a null task must raise a NullPointerException
     */
    @Test
    public void submitNullTask() throws Exception {
        ExecutorService executor = Hazelcast.getExecutorService();
        try {
            Callable c = null;
            Future future = executor.submit(c);
            fail();
        }
        catch (NullPointerException npe) {
            ; // It's ok
        }
    }

    /**
     * Run a basic task
     */
    @Test
    public void testBasicTask() throws Exception {
        Callable<String> task = new BasicTestTask();
        ExecutorService executor = Hazelcast.getExecutorService();
        Future future = executor.submit(task);
        assertEquals(future.get(), BasicTestTask.RESULT);
    }

    @Test
    public void testCancellationAwareTask() {
        CancellationAwareTask task = new CancellationAwareTask(10000);
        ExecutorService executor = Hazelcast.getExecutorService();
        Future future = executor.submit(task);
        try {
            future.get(2, TimeUnit.SECONDS);
        } catch (TimeoutException expected) {
        } catch (Exception e) {
            fail("No other Exception!!");
        }
        assertFalse(future.isDone());
        assertTrue(future.cancel(true));
        assertTrue(future.isCancelled());
        try {
            future.get();
            fail("Should not complete the task successfully");
        } catch (CancellationException e) {
            // expected
        } catch (Exception e) {
            fail("Unexpected exception " + e.getMessage());
        }
    }

    /**
     * Test the method isDone()
     */
    @Test
    public void isDoneMethod() throws Exception {
        Callable<String> task = new BasicTestTask();
        ExecutorService executor = Hazelcast.getExecutorService();
        Future future = executor.submit(task);
        if (future.isDone()) {
            assertTrue(future.isDone());
        }
        assertEquals(future.get(), BasicTestTask.RESULT);
        assertTrue(future.isDone());
    }

    /**
     * Test the Execution Callback
     */
    @Test
    public void testExecutionCallback() throws Exception {
        Callable<String> task = new BasicTestTask();
        ExecutorService executor = Hazelcast.getExecutorService();
        DistributedTask dtask = new DistributedTask(task);
        final CountDownLatch latch = new CountDownLatch(1);
        dtask.setExecutionCallback(new ExecutionCallback() {
            public void done(Future future) {
                assertTrue(future.isDone());
                try {
                    assertEquals(future.get(), BasicTestTask.RESULT);
                    latch.countDown();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
        Future future = executor.submit(dtask);
        assertTrue(latch.await(2, TimeUnit.SECONDS));
        assertTrue(future.isDone());
        assertEquals(future.get(), BasicTestTask.RESULT);
        assertTrue(future.isDone());
    }

    /**
     * Execute a task that is executing
     * something else inside. Nested Execution.
     */
    @Test(timeout = 10000)
    public void testNestedExecution() throws Exception {
        Callable<String> task = new NestedExecutorTask();
        ExecutorService executor = Hazelcast.getExecutorService();
        Future future = executor.submit(task);
        future.get();
    }

    /**
     * Test for the issue 129.
     * Repeatedly runs tasks and check for isDone() status after
     * get().
     */
    @Test
    public void idDoneMethod() throws Exception {
        ExecutorService executor = Hazelcast.getExecutorService();
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
     * Test multiple Future.get() invocation
     */
    @Test
    public void isTwoGetFromFuture() throws Exception {
        Callable<String> task = new BasicTestTask();
        ExecutorService executor = Hazelcast.getExecutorService();
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
        ExecutorService executor = Hazelcast.getExecutorService();
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
        ExecutorService executor = Hazelcast.getExecutorService();
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
        }
        catch (InterruptedException ie) {
            fail("InterruptedException");
        }
        assertTrue(executor.isShutdown());
        assertTrue(executor.isTerminated());
    }

    /**
     * Shutting down the cluster should act as the ExecutorService shutdown
     */
    @Test
    public void testClusterShutdown() throws Exception {
        ExecutorService executor = Hazelcast.getExecutorService();
        Hazelcast.shutdown();
        assertNotNull(executor);
        assertTrue(executor.isShutdown());
        assertTrue(executor.isTerminated());
        // New tasks must be rejected
        Callable<String> task = new BasicTestTask();
        try {
            Future future = executor.submit(task);
            fail("Should not be here!");
        }
        catch (RejectedExecutionException ree) {
            ; // It's ok
        }
        // Reanimate Hazelcast
        Hazelcast.restart();
        executor = Hazelcast.getExecutorService();
        assertFalse(executor.isShutdown());
        assertFalse(executor.isTerminated());
        Future future = executor.submit(task);
    }
}
