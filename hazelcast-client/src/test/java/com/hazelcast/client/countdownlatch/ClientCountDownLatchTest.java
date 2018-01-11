/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICountDownLatch;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientCountDownLatchTest extends HazelcastTestSupport {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();
    private ICountDownLatch l;

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
    }

    @Before
    public void setup() {
        hazelcastFactory.newHazelcastInstance();
        HazelcastInstance client = hazelcastFactory.newHazelcastClient();
        l = client.getCountDownLatch(randomString());
    }

    @Test
    public void testLatch() throws Exception {
        assertTrue(l.trySetCount(20));
        assertFalse(l.trySetCount(10));
        assertEquals(20, l.getCount());

        new Thread() {
            public void run() {
                for (int i = 0; i < 20; i++) {
                    l.countDown();
                    try {
                        Thread.sleep(60);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }.start();

        assertFalse(l.await(1, TimeUnit.SECONDS));

        assertTrue(l.await(5, TimeUnit.SECONDS));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testTrySetCount_whenArgumentNegative() {
        l.trySetCount(-20);
    }

    @Test
    public void testTrySetCount_whenCountIsNotZero() {
        l.trySetCount(10);
        assertFalse(l.trySetCount(20));
        assertFalse(l.trySetCount(0));
        assertEquals(10, l.getCount());
    }
}
