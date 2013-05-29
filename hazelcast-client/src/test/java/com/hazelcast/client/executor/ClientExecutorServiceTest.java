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

package com.hazelcast.client.executor;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.*;
import com.hazelcast.test.RandomBlockJUnit4ClassRunner;
import com.hazelcast.test.annotation.SerialTest;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @ali 5/27/13
 */
@RunWith(RandomBlockJUnit4ClassRunner.class)
@Category(SerialTest.class)
public class ClientExecutorServiceTest {

    static final String name = "test1";
    static HazelcastInstance hz;
    static HazelcastInstance server;
    static HazelcastInstance second;
    static IExecutorService service;

    @BeforeClass
    public static void init(){
        server = Hazelcast.newHazelcastInstance();
        second = Hazelcast.newHazelcastInstance();
        hz = HazelcastClient.newHazelcastClient(null);
        service = hz.getExecutorService(name);
    }

    @AfterClass
    public static void destroy() {
        hz.getLifecycleService().shutdown();
        Hazelcast.shutdownAll();
    }

    @Before
    @After
    public void clear() throws IOException {
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
                } else {
                    System.err.println("problem!!!");
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
            System.err.println("member: " + member);
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
                System.err.println("member: " + member);
                if (value.equals("asd:result")){
                    latch.countDown();
                }
            }

            public void onComplete(Map<Member, Object> values) {
                for (Member member : values.keySet()) {
                    System.err.println("member: " + member);
                    Object value = values.get(member);
                    if (value.equals("asd:result")){
                        latch.countDown();
                    }
                }
            }
        });
        assertTrue(latch.await(5, TimeUnit.SECONDS));
    }




}
