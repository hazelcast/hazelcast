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

package com.hazelcast.client.countdownlatch;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICountDownLatch;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

/**
 * @author ali 5/28/13
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ClientCountDownLatchTest {

    static final String name = "test";
    static HazelcastInstance hz;
    static ICountDownLatch latch;

    @Before
    public void init() {
        Hazelcast.newHazelcastInstance();
        hz = HazelcastClient.newHazelcastClient();
        latch = hz.getCountDownLatch(name);
    }

    @After
    public void stop(){
        hz.shutdown();
        Hazelcast.shutdownAll();
    }

    @Test
    public void testLatch() throws Exception {
        assertTrue(latch.trySetCount(20));
        assertFalse(latch.trySetCount(10));
        assertEquals(20, latch.getCount());

        new Thread(){
            public void run() {
                for (int i=0; i<20; i++) {
                    latch.countDown();
                    try {
                        Thread.sleep(60);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }.start();

        assertFalse(latch.await(1, TimeUnit.SECONDS));
        assertTrue(latch.await(5, TimeUnit.SECONDS));
    }
}
