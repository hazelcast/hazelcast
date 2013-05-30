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

package com.hazelcast.client.atomiclong;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.test.HazelcastJUnit4ClassRunner;
import com.hazelcast.test.annotation.SerialTest;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @ali 5/24/13
 */
@RunWith(HazelcastJUnit4ClassRunner.class)
@Category(SerialTest.class)
public class ClientAtomicLongTest {

    static final String name = "test1";
    static HazelcastInstance hz;
    static HazelcastInstance server;
    static HazelcastInstance second;
    static IAtomicLong l;

    @BeforeClass
    public static void init(){
        server = Hazelcast.newHazelcastInstance();
        hz = HazelcastClient.newHazelcastClient(null);
        l = hz.getAtomicLong(name);
    }

    @AfterClass
    public static void destroy() {
        hz.getLifecycleService().shutdown();
        Hazelcast.shutdownAll();
    }

    @Before
    @After
    public void clear() throws IOException {
        l.set(0);
    }

    @Test
    public void test() throws Exception {


        assertEquals(0, l.getAndAdd(2));
        assertEquals(2, l.get());
        l.set(5);
        assertEquals(5, l.get());
        assertEquals(8, l.addAndGet(3));
        assertFalse(l.compareAndSet(7, 4));
        assertEquals(8, l.get());
        assertTrue(l.compareAndSet(8, 4));
        assertEquals(4, l.get());
        assertEquals(3, l.decrementAndGet());
        assertEquals(3, l.getAndIncrement());
        assertEquals(4, l.getAndSet(9));
        assertEquals(10, l.incrementAndGet());

    }
}
