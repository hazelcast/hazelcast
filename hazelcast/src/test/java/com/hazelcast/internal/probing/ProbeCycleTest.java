package com.hazelcast.internal.probing;

import static com.hazelcast.internal.probing.ProbeRegistryImpl.appendEscaped;
import static com.hazelcast.internal.probing.ProbeRegistryImpl.toLong;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import com.hazelcast.internal.util.counters.SwCounter;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ProbeCycleTest {

    @Test
    public void escapesTagValues() {
        assertEscapes("", "");
        assertEscapes("aaa", "aaa");
        assertEscapes("=", "\\=");
        assertEscapes(",", "\\,");
        assertEscapes("\\", "\\\\");
        assertEscapes("a=b", "a\\=b");
        assertEscapes("=b", "\\=b");
        assertEscapes("a=", "a\\=");
        assertEscapes("foo bar", "foo\\ bar");
    }

    @Test
    public void doubleToLongConversion() {
        assertEquals(10000L, toLong(1d));
        assertEquals(20000L, toLong(2d));
        assertEquals(1000L, toLong(0.1d));
        assertEquals(54000L, toLong(5.4d));
        assertEquals(12345L, toLong(1.2345d));
        assertEquals(12345L, toLong(1.23451d));
        assertEquals(12346L, toLong(1.23456d));
    }

    @Test
    public void objectToLongConversion() {
        assertEquals(-1L, toLong(null));
        assertEquals(42L, toLong(new AtomicLong(42L)));
        assertEquals(42L, toLong(new AtomicInteger(42)));
        assertEquals(1L, toLong(new AtomicBoolean(true)));
        assertEquals(0L, toLong(new AtomicBoolean(false)));
        assertEquals(1L, toLong(Boolean.TRUE));
        assertEquals(0L, toLong(Boolean.FALSE));
        assertEquals(42L, toLong(new Long(42L)));
        assertEquals(42L, toLong(new Integer(42)));
        assertEquals(42L, toLong(new Short((short) 42)));
        assertEquals(toLong(42d), toLong(new Float(42)));
        assertEquals(toLong(42d), toLong(new Double(42)));
        assertEquals(1L, toLong(singletonList("x")));
        assertEquals(1L, toLong(singletonMap("x", "y")));
        assertEquals(42L, toLong(new Semaphore(42)));
        assertEquals(42L, toLong(SwCounter.newSwCounter(42)));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void objectToLongConversion_Unsupported() {
        assertEquals(-1L, toLong(new Character('a')));
    }

    private static void assertEscapes(String unescaped, String escaped) {
        StringBuilder buf = new StringBuilder();
        appendEscaped(buf, unescaped);
        assertEquals(escaped, buf.toString());
    }
}
