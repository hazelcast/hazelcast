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

package com.hazelcast.client.executor;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.*;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.*;

import static org.junit.Assert.*;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

/**
 * @author ali 5/27/13
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ClientExecutorServiceTest {

    static final String name = "test1";
    static HazelcastInstance client;
    static HazelcastInstance instance1;
    static HazelcastInstance instance2;
    static HazelcastInstance instance3;
    static IExecutorService service;

    @BeforeClass
    public static void init() {
        instance1 = Hazelcast.newHazelcastInstance();
        instance2 = Hazelcast.newHazelcastInstance();
        instance3 = Hazelcast.newHazelcastInstance();
        client = HazelcastClient.newHazelcastClient();
        service = client.getExecutorService(name);
    }

    @AfterClass
    public static void destroy() {
        client.shutdown();
        Hazelcast.shutdownAll();
    }

    @Test
    public void testCancellationAwareTask() {
        CancellationAwareTask task = new CancellationAwareTask(5000);
        Future future = service.submit(task);
        try {
            future.get(2, TimeUnit.SECONDS);
            fail("Should throw TimeoutException!");
        } catch (TimeoutException expected) {
        } catch (Exception e) {
            fail("No other Exception!! -> " + e);
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

    public static class CancellationAwareTask implements Callable<Boolean>, Serializable {

        long sleepTime = 10000;

        public CancellationAwareTask() {
        }

        public CancellationAwareTask(long sleepTime) {
            this.sleepTime = sleepTime;
        }

        public Boolean call() throws InterruptedException {
            Thread.sleep(sleepTime);
            return Boolean.TRUE;
        }
    }

    @Test
    public void testSubmitWithResult() throws ExecutionException, InterruptedException {
        final Integer res = 5;
        final Future<Integer> future = service.submit(new RunnableTask("task"), res);
        final Integer integer = future.get();
        assertEquals(res, integer);
    }

    @Test
    public void submitCallable1() throws Exception {

        final Future<String> future = service.submitToKeyOwner(new CallableTask("foo"), "key");
        String result = future.get();
        assertEquals("foo:result", result);
    }

    @Test
    public void submitCallable2() throws Exception {

        final CountDownLatch latch = new CountDownLatch(1);
        service.submitToKeyOwner(new CallableTask("foo"), "key", new ExecutionCallback<String>() {
            public void onResponse(String response) {
                if (response.equals("foo:result")) {
                    latch.countDown();
                }
            }

            public void onFailure(Throwable t) {
            }
        });

        assertTrue(latch.await(5, TimeUnit.SECONDS));
    }

    @Test
    public void submitCallable3() throws Exception {
        final Map<Member, Future<String>> map = service.submitToAllMembers(new CallableTask("asd"));
        for (Member member : map.keySet()) {
            final Future<String> future = map.get(member);
            String s = future.get();
            assertEquals("asd:result", s);
        }
    }

    @Test
    public void submitCallable4() throws Exception {
        final CountDownLatch latch = new CountDownLatch(4);
        service.submitToAllMembers(new CallableTask("asd"), new MultiExecutionCallback() {
            public void onResponse(Member member, Object value) {
                if (value.equals("asd:result")) {
                    latch.countDown();
                }
            }

            public void onComplete(Map<Member, Object> values) {
                for (Member member : values.keySet()) {
                    Object value = values.get(member);
                    if (value.equals("asd:result")) {
                        latch.countDown();
                    }
                }
            }
        });
        assertTrue(latch.await(5, TimeUnit.SECONDS));
    }

    @Test(expected = IllegalStateException.class)
    public void submitFailingCallable() {
        final Future<String> f = service.submit(new FailingTask());
        try {
            f.get();
        } catch (InterruptedException e) {
        } catch (ExecutionException e) {
            throw (RuntimeException) e.getCause();
        }
    }

}
