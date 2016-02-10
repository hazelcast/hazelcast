package com.hazelcast.jet.impl.counters;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static java.lang.Long.valueOf;
import static org.junit.Assert.assertEquals;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class LongCounterTest {

    @Test
    public void testCounterStartsFromZero() throws Exception {
        LongCounter counter = new LongCounter();
        assertEquals(0, counter.getPrimitiveValue());
    }

    @Test
    public void testGetLocalValue() throws Exception {
        LongCounter counter = new LongCounter(4);
        assertEquals(valueOf(4), counter.getLocalValue());
    }

    @Test
    public void testGetPrimitiveValue() throws Exception {
        LongCounter counter = new LongCounter(4);
        assertEquals(4, counter.getPrimitiveValue());
    }

    @Test
    public void testAddPrimitive() throws Exception {
        LongCounter counter = new LongCounter(4);
        counter.add(3);
        assertEquals(7, counter.getPrimitiveValue());
    }

    @Test
    public void testAddBoxed() throws Exception {
        LongCounter counter = new LongCounter(4);
        counter.add(valueOf(5));
        assertEquals(9, counter.getPrimitiveValue());
    }

    @Test
    public void testMerge() throws Exception {
        LongCounter c1 = new LongCounter(4);
        LongCounter c2 = new LongCounter(2);
        c1.merge(c2);
        assertEquals(6, c1.getPrimitiveValue());
    }

    @Test
    public void testResetLocal() throws Exception {
        LongCounter counter = new LongCounter(4);
        counter.resetLocal();
        assertEquals(0, counter.getPrimitiveValue());
    }
}
