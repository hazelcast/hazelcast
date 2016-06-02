package com.hazelcast.jet.internal.impl.counters;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static java.lang.Double.valueOf;
import static org.junit.Assert.assertEquals;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class DoubleCounterTest {

    @Test
    public void testCounterStartsFromZero() throws Exception {
        DoubleCounter counter = new DoubleCounter();
        assertEquals(valueOf(0), counter.getLocalValue());
    }

    @Test
    public void testAdd() throws Exception {
        DoubleCounter counter = new DoubleCounter();
        Double value = valueOf(4);
        counter.add(value);
        assertEquals(value, counter.getLocalValue());
    }

    @Test
    public void testMerge() throws Exception {
        DoubleCounter c1 = new DoubleCounter();
        DoubleCounter c2 = new DoubleCounter();
        c1.add(valueOf(3));
        c2.add(valueOf(5));
        c1.merge(c2);
        assertEquals(valueOf(8), c1.getLocalValue());
    }

    @Test
    public void testResetLocal() throws Exception {
        DoubleCounter counter = new DoubleCounter();
        counter.add(valueOf(4));
        counter.resetLocal();
        assertEquals(valueOf(0), counter.getLocalValue());
    }
}
