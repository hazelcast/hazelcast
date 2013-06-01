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

package com.hazelcast.client.semaphore;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.*;
import com.hazelcast.test.HazelcastJUnit4ClassRunner;
import com.hazelcast.test.annotation.SerialTest;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @ali 5/24/13
 */
@RunWith(HazelcastJUnit4ClassRunner.class)
@Category(SerialTest.class)
public class ClientSemaphoreTest {

    static final String name = "test1";
    static HazelcastInstance hz;
    static HazelcastInstance server;
    static HazelcastInstance second;
    static ISemaphore s;

    @BeforeClass
    public static void init(){

        server = Hazelcast.newHazelcastInstance();
        hz = HazelcastClient.newHazelcastClient(null);
        s = hz.getSemaphore(name);
    }

    @AfterClass
    public static void destroy() {
        hz.getLifecycleService().shutdown();
        Hazelcast.shutdownAll();
    }

    @Before
    @After
    public void clear() throws IOException {
        s.reducePermits(100);
        s.init(10);
    }

    @Test
    public void testAcquire() throws Exception {

        assertEquals(10, s.drainPermits());

        final CountDownLatch latch = new CountDownLatch(1);
        new Thread(){
            public void run() {
                try {
                    s.acquire();
                    latch.countDown();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }.start();
        Thread.sleep(1000);

        s.release(2);
        assertTrue(latch.await(10, TimeUnit.SECONDS));
        assertEquals(1, s.availablePermits());

    }

    @Test
    public void tryAcquire() throws Exception {
        assertTrue(s.tryAcquire());
        assertTrue(s.tryAcquire(9));
        assertEquals(0, s.availablePermits());
        assertFalse(s.tryAcquire(1, TimeUnit.SECONDS));
        assertFalse(s.tryAcquire(2, 1, TimeUnit.SECONDS));


        final CountDownLatch latch = new CountDownLatch(1);
        new Thread(){
            public void run() {
                try {
                    if(s.tryAcquire(2, 5, TimeUnit.SECONDS)){
                        latch.countDown();
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }.start();

        s.release(2);
        assertTrue(latch.await(10, TimeUnit.SECONDS));
        assertEquals(0, s.availablePermits());

    }
}
