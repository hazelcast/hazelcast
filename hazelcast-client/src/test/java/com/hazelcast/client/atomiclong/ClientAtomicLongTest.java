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

package com.hazelcast.client.atomiclong;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.IFunction;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;

import static org.junit.Assert.*;
import static org.junit.Assert.fail;

/**
 * @author ali 5/24/13
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ClientAtomicLongTest {

    static final String name = "test1";
    static HazelcastInstance client;
    static HazelcastInstance server;
    static IAtomicLong counter;

    @BeforeClass
    public static void init(){
        server = Hazelcast.newHazelcastInstance();
        client = HazelcastClient.newHazelcastClient();
        counter = client.getAtomicLong(name);
    }

    @AfterClass
    public static void destroy() {
        client.shutdown();
        Hazelcast.shutdownAll();
    }

    @Before
    @After
    public void clear() throws IOException {
        counter.set(0);
    }

    @Test
    public void test() throws Exception {
        assertEquals(0, counter.getAndAdd(2));
        assertEquals(2, counter.get());
        counter.set(5);
        assertEquals(5, counter.get());
        assertEquals(8, counter.addAndGet(3));
        assertFalse(counter.compareAndSet(7, 4));
        assertEquals(8, counter.get());
        assertTrue(counter.compareAndSet(8, 4));
        assertEquals(4, counter.get());
        assertEquals(3, counter.decrementAndGet());
        assertEquals(3, counter.getAndIncrement());
        assertEquals(4, counter.getAndSet(9));
        assertEquals(10, counter.incrementAndGet());

    }

    @Test(expected = IllegalArgumentException.class)
    public void apply_whenCalledWithNullFunction() {
        IAtomicLong hzCounter = client.getAtomicLong("apply_whenCalledWithNullFunction");

        hzCounter.apply(null);
    }

    @Test
    public void apply() {
        IAtomicLong hzCounter = client.getAtomicLong("apply");

        assertEquals(new Long(1), hzCounter.apply(new AddOneFunction()));
        assertEquals(0, hzCounter.get());
    }

    @Test
    public void apply_whenException() {
        IAtomicLong hzCounter = client.getAtomicLong("apply_whenException");
        hzCounter.set(1);
        try {
            hzCounter.apply(new FailingFunction());
            fail();
        } catch (AtomicLongRuntimeException ignored) {
        }

        assertEquals(1, hzCounter.get());
    }

    @Test(expected = IllegalArgumentException.class)
    public void alter_whenCalledWithNullFunction() {
        IAtomicLong hzCounter = client.getAtomicLong("alter_whenCalledWithNullFunction");

        hzCounter.alter(null);
    }

    @Test
    public void alter_whenException() {
        IAtomicLong hzCounter = client.getAtomicLong("alter_whenException");
        hzCounter.set(10);

        try {
            hzCounter.alter(new FailingFunction());
            fail();
        } catch (AtomicLongRuntimeException ignored) {
        }

        assertEquals(10, hzCounter.get());
    }

    @Test
    public void alter() {
        IAtomicLong hzCounter = client.getAtomicLong("alter");

        hzCounter.set(10);
        hzCounter.alter(new AddOneFunction());
        assertEquals(11, hzCounter.get());

    }

    @Test(expected = IllegalArgumentException.class)
    public void alterAndGet_whenCalledWithNullFunction() {
        IAtomicLong hzCounter = client.getAtomicLong("alterAndGet_whenCalledWithNullFunction");

        hzCounter.alterAndGet(null);
    }

    @Test
    public void alterAndGet_whenException() {
        IAtomicLong hzCounter = client.getAtomicLong("alterAndGet_whenException");
        hzCounter.set(10);

        try {
            hzCounter.alterAndGet(new FailingFunction());
            fail();
        } catch (AtomicLongRuntimeException ignored) {
        }

        assertEquals(10, hzCounter.get());
    }

    @Test
    public void alterAndGet() {
        IAtomicLong hzCounter = client.getAtomicLong("alterAndGet");

        hzCounter.set(10);
        assertEquals(11, hzCounter.alterAndGet(new AddOneFunction()));
        assertEquals(11, hzCounter.get());
    }

    @Test(expected = IllegalArgumentException.class)
    public void getAndAlter_whenCalledWithNullFunction() {
        IAtomicLong hzCounter = client.getAtomicLong("getAndAlter_whenCalledWithNullFunction");

        hzCounter.getAndAlter(null);
    }

    @Test
    public void getAndAlter_whenException() {
        IAtomicLong hzCounter = client.getAtomicLong("getAndAlter_whenException");
        hzCounter.set(10);

        try {
            hzCounter.getAndAlter(new FailingFunction());
            fail();
        } catch (AtomicLongRuntimeException ignored) {
        }

        assertEquals(10, hzCounter.get());
    }

    @Test
    public void getAndAlter() {
        IAtomicLong hzCounter = client.getAtomicLong("getAndAlter");

        hzCounter.set(10);
        assertEquals(10, hzCounter.getAndAlter(new AddOneFunction()));
        assertEquals(11, hzCounter.get());
    }

    private static class AddOneFunction implements IFunction<Long, Long> {
        @Override
        public Long apply(Long input) {
            return input+1;
        }
    }

    private static class FailingFunction implements IFunction<Long, Long> {
        @Override
        public Long apply(Long input) {
            throw new AtomicLongRuntimeException();
        }
    }

    private static class AtomicLongRuntimeException extends RuntimeException {
    }
}
