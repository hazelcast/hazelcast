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
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.IFunction;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author ali 5/24/13
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ClientAtomicLongTest {

    private static final String name = "atomicLongTest";

    private static IAtomicLong counter;

    @BeforeClass
    public static void beforeClass(){
        Hazelcast.newHazelcastInstance();
        HazelcastInstance client = HazelcastClient.newHazelcastClient();

        counter = client.getAtomicLong(name);
    }

    @AfterClass
    public static void afterClass() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Before
    @After
    public void reset() throws IOException {
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
        counter.apply(null);
    }

    @Test
    public void apply() {
        assertEquals(new Long(1), counter.apply(new AddOneFunction()));
        assertEquals(0, counter.get());
    }

    @Test
    public void apply_whenException() {
        counter.set(1);
        try {
            counter.apply(new FailingFunction());
            fail();
        } catch (ClientAtomicLongTestRuntimeException ignored) {
        }

        assertEquals(1, counter.get());
    }

    @Test(expected = IllegalArgumentException.class)
    public void alter_whenCalledWithNullFunction() {
        counter.alter(null);
    }

    @Test
    public void alter_whenException() {
        counter.set(10);
        try {
            counter.alter(new FailingFunction());
            fail();
        } catch (ClientAtomicLongTestRuntimeException ignored) {
        }

        assertEquals(10, counter.get());
    }

    @Test
    public void alter() {
        counter.set(10);
        counter.alter(new AddOneFunction());

        assertEquals(11, counter.get());
    }

    @Test(expected = IllegalArgumentException.class)
    public void alterAndGet_whenCalledWithNullFunction() {
        counter.alterAndGet(null);
    }

    @Test
    public void alterAndGet_whenException() {
        counter.set(10);
        try {
            counter.alterAndGet(new FailingFunction());
            fail();
        } catch (ClientAtomicLongTestRuntimeException ignored) {
        }

        assertEquals(10, counter.get());
    }

    @Test
    public void alterAndGet() {
        counter.set(10);

        assertEquals(11, counter.alterAndGet(new AddOneFunction()));
        assertEquals(11, counter.get());
    }

    @Test(expected = IllegalArgumentException.class)
    public void getAndAlter_whenCalledWithNullFunction() {
        counter.getAndAlter(null);
    }

    @Test
    public void getAndAlter_whenException() {
        counter.set(10);
        try {
            counter.getAndAlter(new FailingFunction());
            fail();
        } catch (ClientAtomicLongTestRuntimeException ignored) {
        }

        assertEquals(10, counter.get());
    }

    @Test
    public void getAndAlter() {
        counter.set(10);

        assertEquals(10, counter.getAndAlter(new AddOneFunction()));
        assertEquals(11, counter.get());
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
            throw new ClientAtomicLongTestRuntimeException();
        }
    }

    private static class ClientAtomicLongTestRuntimeException extends RuntimeException {
    }
}
