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

package com.hazelcast.concurrent.semaphore;

import com.hazelcast.client.ClientTestSupport;
import com.hazelcast.concurrent.semaphore.client.*;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ISemaphore;
import com.hazelcast.instance.StaticNodeFactory;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.nio.serialization.SerializationServiceImpl;
import org.junit.*;
import org.junit.runner.RunWith;

import java.io.IOException;

import static org.junit.Assert.*;

/**
 * @ali 5/13/13
 */
@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
public class SemaphoreBinaryClientTest extends ClientTestSupport {

    static final String name = "test";
    static final SerializationService ss = new SerializationServiceImpl(0);
    static HazelcastInstance hz = null;

    @BeforeClass
    public static void init() {
        Config config = new Config();
        hz = new StaticNodeFactory(1).newHazelcastInstance(config);
    }

    @AfterClass
    public static void destroy() {
        Hazelcast.shutdownAll();
    }

    @Before
    public void start() throws IOException {
        ISemaphore s = hz.getSemaphore(name);
        s.reducePermits(100);
        assertEquals(0, s.availablePermits());
    }

    @After
    public void clear() throws IOException {
        ISemaphore s = hz.getSemaphore(name);
        s.reducePermits(100);
        assertEquals(0, s.availablePermits());
    }

    @Test
    public void testAcquire() throws Exception {

        ISemaphore s = hz.getSemaphore(name);
        assertTrue(s.init(10));

        client().send(new AcquireRequest(name, 3, 0));
        boolean result = (Boolean) client().receive();
        assertTrue(result);
        assertEquals(7, s.availablePermits());

        client().send(new AcquireRequest(name, 8, 6 * 1000));
        assertEquals(7, s.availablePermits());

        Thread.sleep(2 * 1000);

        s.release(1);

        result = (Boolean) client().receive();
        assertTrue(result);
        assertEquals(0, s.availablePermits());

        client().send(new AcquireRequest(name, 4, 2 * 1000));
        result = (Boolean) client().receive();
        assertFalse(result);

    }

    @Test
    public void testAvailable() throws Exception {
        client().send(new AvailableRequest(name));
        int result = (Integer) client().receive();
        assertEquals(0, result);

        ISemaphore s = hz.getSemaphore(name);
        s.release(5);

        client().send(new AvailableRequest(name));
        result = (Integer) client().receive();
        assertEquals(5, result);
    }

    @Test
    public void testDrain() throws Exception {
        ISemaphore s = hz.getSemaphore(name);
        assertTrue(s.init(10));

        client().send(new DrainRequest(name));
        int result = (Integer) client().receive();
        assertEquals(10, result);


        s.release(4);

        client().send(new DrainRequest(name));
        result = (Integer) client().receive();
        assertEquals(4, result);
    }

    @Test
    public void testInit() throws Exception {
        ISemaphore s = hz.getSemaphore(name);

        client().send(new InitRequest(name, 10));
        boolean result = (Boolean) client().receive();
        assertTrue(result);
        assertEquals(10, s.availablePermits());

        client().send(new InitRequest(name, 20));
        result = (Boolean) client().receive();
        assertFalse(result);
        assertEquals(10, s.availablePermits());
    }

    @Test
    public void testReduce() throws Exception {
        ISemaphore s = hz.getSemaphore(name);
        assertTrue(s.init(10));

        client().send(new ReduceRequest(name, 4));
        boolean result = (Boolean) client().receive();
        assertTrue(result);
        assertEquals(6, s.availablePermits());
    }

    @Test
    public void testRelease() throws Exception {
        ISemaphore s = hz.getSemaphore(name);
        assertTrue(s.init(10));

        client().send(new ReleaseRequest(name, 4));
        boolean result = (Boolean) client().receive();
        assertTrue(result);
        assertEquals(14, s.availablePermits());
    }
}
