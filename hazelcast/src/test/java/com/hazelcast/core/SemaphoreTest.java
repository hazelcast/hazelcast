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

import com.hazelcast.config.Config;
import com.hazelcast.config.SemaphoreConfig;
import org.junit.*;
import org.junit.runner.RunWith;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

/**
 * @author: iocanel
 */
@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
public class SemaphoreTest {

    @BeforeClass
    @AfterClass
    public static void init() throws Exception {
        Hazelcast.shutdownAll();
    }

    @Before
    public void setUp() throws Exception {
        Hazelcast.shutdownAll();
    }

    @After
    public void tearDown() throws Exception {
        Hazelcast.shutdownAll();
    }

    @Test
    public void testSemaphoreWithTimeout() {
        SemaphoreConfig semaphoreConfig = new SemaphoreConfig("default", 10);
        Config config = new Config();
        config.addSemaphoreConfig(semaphoreConfig);
        HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);
        ISemaphore semaphore = instance.getSemaphore("testSemaphoreWithTimeout");
        //Test acquire and timeout.
        assertEquals(10, semaphore.availablePermits());
        semaphore.tryAcquire();
        assertEquals(9, semaphore.availablePermits());
        assertEquals(false, semaphore.tryAcquire(10, 10, TimeUnit.MILLISECONDS));
        assertEquals(9, semaphore.availablePermits());
        //Test acquire and timeout and check for partial acquisitions.
        semaphore = instance.getSemaphore("testSemaphoreWithTimeoutAnd10Permits");
        assertEquals(10, semaphore.availablePermits());
        assertEquals(false, semaphore.tryAcquire(20, 10, TimeUnit.MILLISECONDS));
        assertEquals(10, semaphore.availablePermits());
    }

    @Test
    public void testSimpleSemaphore() {
        SemaphoreConfig semaphoreConfig = new SemaphoreConfig("default", 1);
        Config config = new Config();
        config.addSemaphoreConfig(semaphoreConfig);
        HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);
        ISemaphore semaphore = instance.getSemaphore("testSimpleSemaphore");
        assertEquals(1, semaphore.availablePermits());
        semaphore.tryAcquire();
        assertEquals(0, semaphore.availablePermits());
        semaphore.release();
        assertEquals(1, semaphore.availablePermits());
    }

    @Test
    public void testSemaphoreReducePermits() {
        SemaphoreConfig semaphoreConfig = new SemaphoreConfig("default", 10);
        Config config = new Config();
        config.addSemaphoreConfig(semaphoreConfig);
        HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);
        ISemaphore semaphore = instance.getSemaphore("testSemaphoreReducePermits");
        assertEquals(10, semaphore.availablePermits());
        semaphore.reducePermits(1);
        assertEquals(9, semaphore.availablePermits());
        semaphore.tryAcquire(9);
        assertEquals(0, semaphore.availablePermits());
        semaphore.reducePermits(8);
        assertEquals(-8, semaphore.availablePermits());
        semaphore.release();
        assertEquals(-7, semaphore.availablePermits());
        semaphore.release(8);
        assertEquals(1, semaphore.availablePermits());
    }

    @Test
    @Ignore
    public void testSemaphoreDisconnect() {
        SemaphoreConfig semaphoreConfig = new SemaphoreConfig("default", 10);
        Config config = new Config();
        config.addSemaphoreConfig(semaphoreConfig);
        HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance instance2 = Hazelcast.newHazelcastInstance(config);
        ISemaphore semaphore1 = instance1.getSemaphore("testDisconnectSemaphore");
        ISemaphore semaphore2 = instance2.getSemaphore("testDisconnectSemaphore");
        assertEquals(10, semaphore1.availablePermits());
        semaphore1.tryAcquire(5);
        semaphore1.reducePermits(1);
        instance1.shutdown();
        int result = semaphore2.availablePermits();
        int expectedResult = 9;
        assertEquals(expectedResult, result);
    }

    @Test
    public void testSemaphorePeerDisconnect() {
        SemaphoreConfig semaphoreConfig = new SemaphoreConfig("default", 10);
        Config config = new Config();
        config.addSemaphoreConfig(semaphoreConfig);
        HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance instance2 = Hazelcast.newHazelcastInstance(config);
        ISemaphore semaphore1 = instance1.getSemaphore("testDisconnectSemaphore");
        ISemaphore semaphore2 = instance2.getSemaphore("testDisconnectSemaphore");
        semaphore2.tryAcquire(5);
        int result = semaphore1.availablePermits();
        int expectedResult = 5;
        assertEquals(expectedResult, result);
        instance2.shutdown();
        expectedResult = 10;
        result = semaphore1.availablePermits();
        assertEquals(expectedResult, result);
    }
}
