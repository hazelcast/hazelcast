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

package com.hazelcast.concurrent.atomicreference;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicReference;
import com.hazelcast.core.IFunction;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.BitSet;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public abstract class AtomicReferenceBasicTest extends HazelcastTestSupport {

    protected HazelcastInstance[] instances;
    protected IAtomicReference<String> ref;

    @Before
    public void setup() {
        instances = newInstances();
        ref = newInstance();
    }

    @After
    public void tearDown() {
        for (HazelcastInstance instance : instances) {
            instance.getLifecycleService().terminate();
        }
    }

    protected IAtomicReference newInstance() {
        HazelcastInstance local = instances[0];
        HazelcastInstance target = instances[instances.length - 1];
        String name = generateKeyOwnedBy(target);
        return local.getAtomicReference(name);
    }

    protected abstract HazelcastInstance[] newInstances();

    @Test
    public void getAndSet() {
        assertNull(ref.getAndSet("foo"));
        assertEquals("foo", ref.getAndSet("bar"));
        assertEquals("bar", ref.getAndSet("bar"));
    }

    @Test
    public void isNull() {
        assertTrue(ref.isNull());
        ref.set("foo");
        assertFalse(ref.isNull());
    }

    @Test
    public void get() {
        assertNull(ref.get());
        ref.set("foo");
        assertEquals("foo", ref.get());
    }

    @Test
    public void setAndGet() {
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
    public void set() {
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
    public void clear() {
        ref.clear();
        assertNull(ref.get());

        ref.set("foo");
        ref.clear();
        assertNull(ref.get());

        ref.set(null);
        assertNull(ref.get());
    }

    @Test
    public void contains() {
        assertTrue(ref.contains(null));
        assertFalse(ref.contains("foo"));

        ref.set("foo");

        assertFalse(ref.contains(null));
        assertTrue(ref.contains("foo"));
        assertFalse(ref.contains("bar"));
    }

    @Test
    public void compareAndSet() {
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
    public void apply_whenCalledWithNullFunction() {
        ref.apply(null);
    }

    @Test
    public void apply() {
        assertEquals("null", ref.apply(new AppendFunction("")));
        assertEquals(null, ref.get());

        ref.set("foo");
        assertEquals("foobar", ref.apply(new AppendFunction("bar")));
        assertEquals("foo", ref.get());

        assertEquals(null, ref.apply(new NullFunction()));
        assertEquals("foo", ref.get());
    }

    @Test
    public void apply_whenException() {
        ref.set("foo");

        try {
            ref.apply(new FailingFunction());
            fail();
        } catch (HazelcastException expected) {
        }

        assertEquals("foo", ref.get());
    }

    @Test(expected = IllegalArgumentException.class)
    public void alter_whenCalledWithNullFunction() {
        ref.alter(null);
    }

    @Test
    public void alter_whenException() {
        ref.set("foo");

        try {
            ref.alter(new FailingFunction());
            fail();
        } catch (HazelcastException expected) {
        }

        assertEquals("foo", ref.get());
    }

    @Test
    public void alter() {
        ref.alter(new NullFunction());
        assertEquals(null, ref.get());

        ref.set("foo");
        ref.alter(new AppendFunction("bar"));
        assertEquals("foobar", ref.get());

        ref.alter(new NullFunction());
        assertEquals(null, ref.get());
    }

    @Test(expected = IllegalArgumentException.class)
    public void alterAndGet_whenCalledWithNullFunction() {
        ref.alterAndGet(null);
    }

    @Test
    public void alterAndGet_whenException() {
        ref.set("foo");

        try {
            ref.alterAndGet(new FailingFunction());
            fail();
        } catch (HazelcastException expected) {
        }

        assertEquals("foo", ref.get());
    }

    @Test
    public void alterAndGet() {
        assertNull(ref.alterAndGet(new NullFunction()));
        assertEquals(null, ref.get());

        ref.set("foo");
        assertEquals("foobar", ref.alterAndGet(new AppendFunction("bar")));
        assertEquals("foobar", ref.get());

        assertEquals(null, ref.alterAndGet(new NullFunction()));
        assertEquals(null, ref.get());
    }

    @Test(expected = IllegalArgumentException.class)
    public void getAndAlter_whenCalledWithNullFunction() {
        ref.getAndAlter(null);
    }

    @Test
    public void getAndAlter_whenException() {
        ref.set("foo");

        try {
            ref.getAndAlter(new FailingFunction());
            fail();
        } catch (HazelcastException expected) {
        }

        assertEquals("foo", ref.get());
    }

    @Test
    public void getAndAlter() {
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
            throw new HazelcastException();
        }
    }

    @Test
    public void testToString() {
        String name = ref.getName();
        assertEquals(format("IAtomicReference{name='%s'}", name), ref.toString());
    }

    @Test
    public void getAndAlter_when_same_reference() {
        BitSet bitSet = new BitSet();
        IAtomicReference<BitSet> ref2 = newInstance();
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
