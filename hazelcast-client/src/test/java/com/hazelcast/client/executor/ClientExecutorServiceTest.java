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
import com.hazelcast.test.HazelcastJUnit4ClassRunner;
import com.hazelcast.test.annotation.SerialTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author ali 5/27/13
 */
@RunWith(HazelcastJUnit4ClassRunner.class)
@Category(SerialTest.class)
public class ClientExecutorServiceTest {

    static final String name = "test1";
    static HazelcastInstance hz;
    static HazelcastInstance server;
    static HazelcastInstance second;
    static HazelcastInstance third;
    static IExecutorService service;

    @BeforeClass
    public static void init(){
        server = Hazelcast.newHazelcastInstance();
        second = Hazelcast.newHazelcastInstance();
        third = Hazelcast.newHazelcastInstance();
        hz = HazelcastClient.newHazelcastClient(null);
        service = hz.getExecutorService(name);
    }

    @AfterClass
    public static void destroy() {
        hz.getLifecycleService().shutdown();
        Hazelcast.shutdownAll();
    }

    @Test
    public void submitCallable1() throws Exception {

        final Future<String> future = service.submitToKeyOwner(new CallableTask("naber"), "key");
        String result = future.get();
        assertEquals("naber:result", result);
    }

    @Test
    public void submitCallable2() throws Exception {

        final CountDownLatch latch = new CountDownLatch(1);
        service.submitToKeyOwner(new CallableTask("naber"), "key", new ExecutionCallback<String>() {
            public void onResponse(String response) {
                if (response.equals("naber:result")){
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
        service.submitToAllMembers(new CallableTask("asd"),new MultiExecutionCallback() {
            public void onResponse(Member member, Object value) {
                if (value.equals("asd:result")){
                    latch.countDown();
                }
            }

            public void onComplete(Map<Member, Object> values) {
                for (Member member : values.keySet()) {
                    Object value = values.get(member);
                    if (value.equals("asd:result")){
                        latch.countDown();
                    }
                }
            }
        });
        assertTrue(latch.await(5, TimeUnit.SECONDS));
    }

    @Test
    @Ignore
    public void testThreadPoolSize() throws Exception {
        final Thread thread = new Thread() {
            public void run() {
                while (true) {
                    service.submit(new CallableTask("asd"));
                }
            }
        };
        thread.start();
        thread.join();
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
