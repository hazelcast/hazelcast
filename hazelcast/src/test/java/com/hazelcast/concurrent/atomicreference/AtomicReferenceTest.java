package com.hazelcast.concurrent.atomicreference;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicReference;
import com.hazelcast.test.HazelcastJUnit4ClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ClientCompatibleTest;
import com.hazelcast.test.annotation.ParallelTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.*;

@RunWith(HazelcastJUnit4ClassRunner.class)
@Category(ParallelTest.class)
public class AtomicReferenceTest extends HazelcastTestSupport {

    @Test
    @ClientCompatibleTest
    public void getAndSet() {
        HazelcastInstance hazelcastInstance = createHazelcastInstanceFactory(1).newHazelcastInstance(new Config());
        IAtomicReference<String> ref = hazelcastInstance.getAtomicReference("getAndSet");
        assertNull(ref.getAndSet("foo"));
        assertEquals("foo", ref.getAndSet("bar"));
        assertEquals("bar", ref.getAndSet("bar"));
    }

    @Test
    @ClientCompatibleTest
    public void isNull() {
        HazelcastInstance hazelcastInstance = createHazelcastInstanceFactory(1).newHazelcastInstance(new Config());
        IAtomicReference<String> ref = hazelcastInstance.getAtomicReference("isNull");

        assertTrue(ref.isNull());
        ref.set("foo");
        assertFalse(ref.isNull());
    }

    @Test
    @ClientCompatibleTest
    public void get() {
        HazelcastInstance hazelcastInstance = createHazelcastInstanceFactory(1).newHazelcastInstance(new Config());
        IAtomicReference<String> ref = hazelcastInstance.getAtomicReference("get");

        assertNull(ref.get());
        ref.set("foo");
        assertEquals("foo", ref.get());
    }

    @Test
    @ClientCompatibleTest
    public void setAndGet() {
        HazelcastInstance hazelcastInstance = createHazelcastInstanceFactory(1).newHazelcastInstance(new Config());
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
        HazelcastInstance hazelcastInstance = createHazelcastInstanceFactory(1).newHazelcastInstance(new Config());
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
        HazelcastInstance hazelcastInstance = createHazelcastInstanceFactory(1).newHazelcastInstance(new Config());
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
    public void compareAndSet() {
        HazelcastInstance hazelcastInstance = createHazelcastInstanceFactory(1).newHazelcastInstance(new Config());
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

    @Test
    public void testToString() {
        HazelcastInstance hazelcastInstance = createHazelcastInstanceFactory(1).newHazelcastInstance(new Config());
        IAtomicReference<String> ref = hazelcastInstance.getAtomicReference("toString");

        assertEquals("IAtomicReference{name='toString'}", ref.toString());
    }
}
