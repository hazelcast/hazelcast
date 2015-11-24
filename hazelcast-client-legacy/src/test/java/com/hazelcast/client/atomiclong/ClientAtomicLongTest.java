/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.IFunction;
import com.hazelcast.test.ExpectedRuntimeException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientAtomicLongTest extends HazelcastTestSupport {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();
    private HazelcastInstance client;
    private IAtomicLong l;

    @Before
    public void setup() {
        hazelcastFactory.newHazelcastInstance();
        client = hazelcastFactory.newHazelcastClient();
        l = client.getAtomicLong(randomString());
    }

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
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
        } catch (ExpectedRuntimeException expected) {
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
        } catch (ExpectedRuntimeException expected) {
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
        } catch (ExpectedRuntimeException expected) {
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
        } catch (ExpectedRuntimeException expected) {
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
            return input + 1;
        }
    }


    private static class FailingFunction implements IFunction<Long, Long> {
        @Override
        public Long apply(Long input) {
            throw new ExpectedRuntimeException();
        }
    }

}
