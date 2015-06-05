package com.hazelcast.util.counters;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class SafeSwCounterTest {
    private SwCounter.SafeSwCounter counter;

    @Before
    public void setup() {
        counter = new SwCounter.SafeSwCounter(0);
    }

    @Test
    public void inc() {
        counter.inc();
        assertEquals(1, counter.get());
    }

    @Test
    public void inc_withAmount() {
        counter.inc(10);
        assertEquals(10, counter.get());

        counter.inc(0);
        assertEquals(10, counter.get());
    }

    @Test
    public void test_toString() {
        String s = counter.toString();
        assertEquals("Counter{value=0}", s);
    }
}
