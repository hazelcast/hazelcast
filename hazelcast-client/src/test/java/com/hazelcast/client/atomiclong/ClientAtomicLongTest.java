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
    static IAtomicLong l;

    @BeforeClass
    public static void init(){
        server = Hazelcast.newHazelcastInstance();
        client = HazelcastClient.newHazelcastClient();
        l = client.getAtomicLong(name);
    }

    @AfterClass
    public static void destroy() {
        client.shutdown();
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

    @Test(expected = IllegalArgumentException.class)
    public void apply_whenCalledWithNullFunction() {
        IAtomicLong ref = client.getAtomicLong("apply_whenCalledWithNullFunction");

        ref.apply(null);
    }

    @Test
    public void apply() {
        IAtomicLong ref = client.getAtomicLong("apply");

        assertEquals(new Long(1), ref.apply(new AddOneFunction()));
        assertEquals(0, ref.get());
    }

    @Test
    public void apply_whenException() {
        IAtomicLong ref = client.getAtomicLong("apply_whenException");
        ref.set(1);
        try {
            ref.apply(new FailingFunction());
            fail();
        } catch (WoohaaException expected) {
        }

        assertEquals(1, ref.get());
    }

    @Test(expected = IllegalArgumentException.class)
    public void alter_whenCalledWithNullFunction() {
        IAtomicLong ref = client.getAtomicLong("alter_whenCalledWithNullFunction");

        ref.alter(null);
    }

    @Test
    public void alter_whenException() {
        IAtomicLong ref = client.getAtomicLong("alter_whenException");
        ref.set(10);

        try {
            ref.alter(new FailingFunction());
            fail();
        } catch (WoohaaException expected) {
        }

        assertEquals(10, ref.get());
    }

    @Test
    public void alter() {
        IAtomicLong ref = client.getAtomicLong("alter");

        ref.set(10);
        ref.alter(new AddOneFunction());
        assertEquals(11, ref.get());

    }

    @Test(expected = IllegalArgumentException.class)
    public void alterAndGet_whenCalledWithNullFunction() {
        IAtomicLong ref = client.getAtomicLong("alterAndGet_whenCalledWithNullFunction");

        ref.alterAndGet(null);
    }

    @Test
    public void alterAndGet_whenException() {
        IAtomicLong ref = client.getAtomicLong("alterAndGet_whenException");
        ref.set(10);

        try {
            ref.alterAndGet(new FailingFunction());
            fail();
        } catch (WoohaaException expected) {
        }

        assertEquals(10, ref.get());
    }

    @Test
    public void alterAndGet() {
        IAtomicLong ref = client.getAtomicLong("alterAndGet");

        ref.set(10);
        assertEquals(11, ref.alterAndGet(new AddOneFunction()));
        assertEquals(11, ref.get());
    }

    @Test(expected = IllegalArgumentException.class)
    public void getAndAlter_whenCalledWithNullFunction() {
        IAtomicLong ref = client.getAtomicLong("getAndAlter_whenCalledWithNullFunction");

        ref.getAndAlter(null);
    }

    @Test
    public void getAndAlter_whenException() {
        IAtomicLong ref = client.getAtomicLong("getAndAlter_whenException");
        ref.set(10);

        try {
            ref.getAndAlter(new FailingFunction());
            fail();
        } catch (WoohaaException expected) {
        }

        assertEquals(10, ref.get());
    }

    @Test
    public void getAndAlter() {
        IAtomicLong ref = client.getAtomicLong("getAndAlter");

        ref.set(10);
        assertEquals(10, ref.getAndAlter(new AddOneFunction()));
        assertEquals(11, ref.get());
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
            throw new WoohaaException();
        }
    }

    private static class WoohaaException extends RuntimeException {

    }
}
