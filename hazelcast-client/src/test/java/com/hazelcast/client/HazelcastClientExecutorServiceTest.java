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

package com.hazelcast.client;

import static com.hazelcast.client.TestUtility.getHazelcastClient;
import static junit.framework.Assert.assertNull;
import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import com.hazelcast.core.*;
import com.hazelcast.core.Member;
import com.hazelcast.monitor.DistributedMapStatsCallable;
import org.junit.Test;

public class HazelcastClientExecutorServiceTest {

    private static final int COUNT = 10;

    /**
     * Test multiple Future.get() invokation
     */
    @Test
    public void isTwoGetFromFuture() throws Exception {
        Callable<String> task = new BasicTestTask();
        ExecutorService executor = getExecutorService();
        Future<String> future = executor.submit(task);
        String s1 = future.get();
        assertEquals(BasicTestTask.RESULT, s1);
        assertTrue(future.isDone());
        String s2 = future.get();
        assertEquals(BasicTestTask.RESULT, s2);
        assertTrue(future.isDone());
        String s3 = future.get();
        assertEquals(BasicTestTask.RESULT, s3);
        assertTrue(future.isDone());
        String s4 = future.get();
        assertEquals(BasicTestTask.RESULT, s4);
        assertTrue(future.isDone());
    }

    /**
     * invokeAll tests
     */
    @Test
    public void testInvokeAll() throws Exception {
        Callable<String> task = new BasicTestTask();
        ExecutorService executor = getExecutorService();

        // Only one task
        ArrayList<Callable<String>> tasks = new ArrayList<Callable<String>>();
        tasks.add(task);
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
     * Submit a null task must raise a NullPointerException
     */
    @Test(expected = NullPointerException.class)
    public void submitNullTask() {
        Callable<?> callable = null;
        getExecutorService().submit(callable);
        fail();
    }

    private ExecutorService getExecutorService() {
        HazelcastClient hClient = getHazelcastClient();
        return hClient.getExecutorService();
    }

    /**
     * Run a basic task
     */
    @Test
    public void testBasicTask() throws Exception {
        Callable<String> task = new BasicTestTask();
        ExecutorService executor = getExecutorService();
        Future<?> future = executor.submit(task);
        assertEquals(future.get(), BasicTestTask.RESULT);
    }

    /**
     * Test the method isDone()
     */
    @Test
    public void isDoneMethod() throws Exception {
        Callable<String> task = new BasicTestTask();
        ExecutorService executor = getExecutorService();
        Future<?> future = executor.submit(task);
        if (future.isDone()) {
            assertTrue(future.isDone());
        }
        assertEquals(future.get(), BasicTestTask.RESULT);
        assertTrue(future.isDone());
    }

    /**
     * Test for the issue 129.
     * Repeadetly runs tasks and check for isDone() status after
     * get().
     */
    @Test
    public void isDoneMethod_issue129() throws Exception {
        ExecutorService executor = getExecutorService();
        for (int i = 0; i < COUNT; i++) {
            Callable<String> task1 = new BasicTestTask();
            Callable<String> task2 = new BasicTestTask();

            Future<?> future1 = executor.submit(task1);
            Future<?> future2 = executor.submit(task2);
            assertEquals(future2.get(), BasicTestTask.RESULT);
            assertTrue(future2.isDone());
            assertEquals(future1.get(), BasicTestTask.RESULT);
            assertTrue(future1.isDone());
        }
    }

    @Test
    public void testBasicRunnable() throws Exception {
        ExecutorService executor = getExecutorService();

        Future<?> future = executor.submit(new BasicRunnable());
        assertNull(future.get());
    }

    public static class BasicRunnable implements Runnable, Serializable {
        public void run() {
            System.out.println("I am running: Hazelcast rocks in Thread: -> " + Thread.currentThread().getName());
        }
    }

    public static class BasicTestTask implements Callable<String>, Serializable {

        public static String RESULT = "Task completed";

        public String call() throws Exception {
            return RESULT;
        }
    }

    @Test
    public void multiTaskWithOneMember() throws ExecutionException, InterruptedException {
        ExecutorService esService = getExecutorService();
        Set<Member> members = getHazelcastClient().getCluster().getMembers();
        MultiTask<DistributedMapStatsCallable.MemberMapStat> task =
                new MultiTask<DistributedMapStatsCallable.MemberMapStat>(new DistributedMapStatsCallable("default"), members);

        esService.submit(task);

        Collection<DistributedMapStatsCallable.MemberMapStat> mapStats = null;
        mapStats = task.get();

        for (DistributedMapStatsCallable.MemberMapStat memberMapStat : mapStats) {
            assertNotNull(memberMapStat);
        }
        assertEquals(members.size(), mapStats.size());
    }

    @Test
    public void multiTaskWithTwoMember() throws ExecutionException, InterruptedException {
        HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
        multiTaskWithOneMember();
        h.shutdown();

    }


}
