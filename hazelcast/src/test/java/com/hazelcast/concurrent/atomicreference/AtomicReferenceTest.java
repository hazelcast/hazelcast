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

package com.hazelcast.concurrent.atomicreference;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicReference;
import com.hazelcast.core.IFunction;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ClientCompatibleTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.BitSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class AtomicReferenceTest extends HazelcastTestSupport {

    @Test
    @ClientCompatibleTest
    public void getAndSet() {
        HazelcastInstance hazelcastInstance = createHazelcastInstance();
        IAtomicReference<String> ref = hazelcastInstance.getAtomicReference("getAndSet");
        assertNull(ref.getAndSet("foo"));
        assertEquals("foo", ref.getAndSet("bar"));
        assertEquals("bar", ref.getAndSet("bar"));
    }

    @Test
    @ClientCompatibleTest
    public void isNull() {
        HazelcastInstance hazelcastInstance = createHazelcastInstance();
        IAtomicReference<String> ref = hazelcastInstance.getAtomicReference("isNull");

        assertTrue(ref.isNull());
        ref.set("foo");
        assertFalse(ref.isNull());
    }

    @Test
    @ClientCompatibleTest
    public void get() {
        HazelcastInstance hazelcastInstance = createHazelcastInstance();
        IAtomicReference<String> ref = hazelcastInstance.getAtomicReference("get");

        assertNull(ref.get());
        ref.set("foo");
        assertEquals("foo", ref.get());
    }

    @Test
    @ClientCompatibleTest
    public void setAndGet() {
        HazelcastInstance hazelcastInstance = createHazelcastInstance();
        IAtomicReference<String> ref = hazelcastInstance.getAtomicReference("setAndGet");

        assertNull(ref.setAndGet(null));
        assertNull(ref.get());

        assertEquals("foo", ref.setAndGet("foo"));
        assertEquals("foo", ref.get());

        assertEquals("bar", ref.setAndGet("bar"));
        assertEquals("bar", ref.get());

        assertNull(ref.setAndGet(null));
        assertNull(ref.get());
    }

    @Test
    @ClientCompatibleTest
    public void set() {
        HazelcastInstance hazelcastInstance = createHazelcastInstance();
        IAtomicReference<String> ref = hazelcastInstance.getAtomicReference("set");

        ref.set(null);
        assertNull(ref.get());

        ref.set("foo");
        assertEquals("foo", ref.get());

        ref.setAndGet("bar");
        assertEquals("bar", ref.get());

        ref.set(null);
        assertNull(ref.get());
    }

    @Test
    @ClientCompatibleTest
    public void clear() {
        HazelcastInstance hazelcastInstance = createHazelcastInstance();
        IAtomicReference<String> ref = hazelcastInstance.getAtomicReference("clear");

        ref.clear();
        assertNull(ref.get());

        ref.set("foo");
        ref.clear();
        assertNull(ref.get());

        ref.set(null);
        assertNull(ref.get());
    }

    @Test
    @ClientCompatibleTest
    public void contains() {
        HazelcastInstance hazelcastInstance = createHazelcastInstance();
        IAtomicReference<String> ref = hazelcastInstance.getAtomicReference("clear");

        assertTrue(ref.contains(null));
        assertFalse(ref.contains("foo"));

        ref.set("foo");

        assertFalse(ref.contains(null));
        assertTrue(ref.contains("foo"));
        assertFalse(ref.contains("bar"));
    }

    @Test
    @ClientCompatibleTest
    public void compareAndSet() {
        HazelcastInstance hazelcastInstance = createHazelcastInstance();
        IAtomicReference<String> ref = hazelcastInstance.getAtomicReference("compareAndSet");

        assertTrue(ref.compareAndSet(null, null));
        assertNull(ref.get());

        assertFalse(ref.compareAndSet("foo", "bar"));
        assertNull(ref.get());

        assertTrue(ref.compareAndSet(null, "foo"));
        assertEquals("foo", ref.get());

        ref.set("foo");
        assertTrue(ref.compareAndSet("foo", "foo"));
        assertEquals("foo", ref.get());

        assertTrue(ref.compareAndSet("foo", "bar"));
        assertEquals("bar", ref.get());

        assertTrue(ref.compareAndSet("bar", null));
        assertNull(ref.get());
    }

    @Test(expected = IllegalArgumentException.class)
    @ClientCompatibleTest
    public void apply_whenCalledWithNullFunction() {
        HazelcastInstance hazelcastInstance = createHazelcastInstance();
        IAtomicReference<String> ref = hazelcastInstance.getAtomicReference("apply_whenCalledWithNullFunction");

        ref.apply(null);
    }

    @Test
    @ClientCompatibleTest
    public void apply() {
        HazelcastInstance hazelcastInstance = createHazelcastInstance();
        IAtomicReference<String> ref = hazelcastInstance.getAtomicReference("apply");

        assertEquals("null", ref.apply(new AppendFunction("")));
        assertEquals(null, ref.get());

        ref.set("foo");
        assertEquals("foobar", ref.apply(new AppendFunction("bar")));
        assertEquals("foo", ref.get());

        assertEquals(null, ref.apply(new NullFunction()));
        assertEquals("foo", ref.get());
    }

    @Test
    @ClientCompatibleTest
    public void apply_whenException() {
        HazelcastInstance hazelcastInstance = createHazelcastInstance();
        IAtomicReference<String> ref = hazelcastInstance.getAtomicReference("apply");
        ref.set("foo");

        try {
            ref.apply(new FailingFunction());
            fail();
        } catch (WoohaaException expected) {
        }

        assertEquals("foo", ref.get());
    }

    @Test(expected = IllegalArgumentException.class)
    @ClientCompatibleTest
    public void alter_whenCalledWithNullFunction() {
        HazelcastInstance hazelcastInstance = createHazelcastInstance();
        IAtomicReference<String> ref = hazelcastInstance.getAtomicReference("alter_whenCalledWithNullFunction");

        ref.alter(null);
    }

    @Test
    @ClientCompatibleTest
    public void alter_whenException() {
        HazelcastInstance hazelcastInstance = createHazelcastInstance();
        IAtomicReference<String> ref = hazelcastInstance.getAtomicReference("alter_whenException");
        ref.set("foo");

        try {
            ref.alter(new FailingFunction());
            fail();
        } catch (WoohaaException expected) {
        }

        assertEquals("foo", ref.get());
    }

    @Test
    @ClientCompatibleTest
    public void alter() {
        HazelcastInstance hazelcastInstance = createHazelcastInstance();
        IAtomicReference<String> ref = hazelcastInstance.getAtomicReference("alter");

        ref.alter(new NullFunction());
        assertEquals(null, ref.get());

        ref.set("foo");
        ref.alter(new AppendFunction("bar"));
        assertEquals("foobar", ref.get());

        ref.alter(new NullFunction());
        assertEquals(null, ref.get());
    }

    @Test(expected = IllegalArgumentException.class)
    @ClientCompatibleTest
    public void alterAndGet_whenCalledWithNullFunction() {
        HazelcastInstance hazelcastInstance = createHazelcastInstance();
        IAtomicReference<String> ref = hazelcastInstance.getAtomicReference("alterAndGet_whenCalledWithNullFunction");

        ref.alterAndGet(null);
    }

    @Test
    @ClientCompatibleTest
    public void alterAndGet_whenException() {
        HazelcastInstance hazelcastInstance = createHazelcastInstance();
        IAtomicReference<String> ref = hazelcastInstance.getAtomicReference("alterAndGet_whenException");
        ref.set("foo");

        try {
            ref.alterAndGet(new FailingFunction());
            fail();
        } catch (WoohaaException expected) {
        }

        assertEquals("foo", ref.get());
    }

    @Test
    @ClientCompatibleTest
    public void alterAndGet() {
        HazelcastInstance hazelcastInstance = createHazelcastInstance();
        IAtomicReference<String> ref = hazelcastInstance.getAtomicReference("alterAndGet");

        assertNull(ref.alterAndGet(new NullFunction()));
        assertEquals(null, ref.get());

        ref.set("foo");
        assertEquals("foobar", ref.alterAndGet(new AppendFunction("bar")));
        assertEquals("foobar", ref.get());

        assertEquals(null, ref.alterAndGet(new NullFunction()));
        assertEquals(null, ref.get());
    }

    @Test(expected = IllegalArgumentException.class)
    @ClientCompatibleTest
    public void getAndAlter_whenCalledWithNullFunction() {
        HazelcastInstance hazelcastInstance = createHazelcastInstance();
        IAtomicReference<String> ref = hazelcastInstance.getAtomicReference("getAndAlter_whenCalledWithNullFunction");

        ref.getAndAlter(null);
    }

    @Test
    @ClientCompatibleTest
    public void getAndAlter_whenException() {
        HazelcastInstance hazelcastInstance = createHazelcastInstance();
        IAtomicReference<String> ref = hazelcastInstance.getAtomicReference("getAndAlter_whenException");
        ref.set("foo");

        try {
            ref.getAndAlter(new FailingFunction());
            fail();
        } catch (WoohaaException expected) {
        }

        assertEquals("foo", ref.get());
    }

    @Test
    @ClientCompatibleTest
    public void getAndAlter() {
        HazelcastInstance hazelcastInstance = createHazelcastInstance();
        IAtomicReference<String> ref = hazelcastInstance.getAtomicReference("getAndAlter");

        assertNull(ref.getAndAlter(new NullFunction()));
        assertEquals(null, ref.get());

        ref.set("foo");
        assertEquals("foo", ref.getAndAlter(new AppendFunction("bar")));
        assertEquals("foobar", ref.get());

        assertEquals("foobar", ref.getAndAlter(new NullFunction()));
        assertEquals(null, ref.get());
    }

    private static class AppendFunction implements IFunction<String, String> {
        private String add;

        private AppendFunction(String add) {
            this.add = add;
        }

        @Override
        public String apply(String input) {
            return input + add;
        }
    }

    private static class NullFunction implements IFunction<String, String> {
        @Override
        public String apply(String input) {
            return null;
        }
    }

    private static class FailingFunction implements IFunction<String, String> {
        @Override
        public String apply(String input) {
            throw new WoohaaException();
        }
    }

    private static class WoohaaException extends RuntimeException {

    }

    @Test
    public void testToString() {
        HazelcastInstance hazelcastInstance = createHazelcastInstance();
        IAtomicReference<String> ref = hazelcastInstance.getAtomicReference("toString");

        assertEquals("IAtomicReference{name='toString'}", ref.toString());
    }

    @Test
    @ClientCompatibleTest
    public void getAndAlter_when_same_reference() {
        HazelcastInstance hazelcastInstance = createHazelcastInstance();

        BitSet bitSet = new BitSet();
        IAtomicReference<BitSet> ref2 = hazelcastInstance.getAtomicReference(randomString());
        ref2.set(bitSet);
        bitSet.set(100);
        assertEquals(bitSet, ref2.alterAndGet(new FailingFunctionAlter()));
        assertEquals(bitSet, ref2.get());
    }

    private static class FailingFunctionAlter implements IFunction<BitSet, BitSet> {
        @Override
        public BitSet apply(BitSet input) {
            input.set(100);
            return input;
        }
    }
}
