package com.hazelcast.ringbuffer.impl;

import com.hazelcast.ringbuffer.StaleSequenceException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ArrayRingbufferTest {
    @Test(expected = StaleSequenceException.class)
    public void testStaleSequenceThrowsException() {
        final ArrayRingbuffer rb = new ArrayRingbuffer(5);
        for (int i = 0; i < rb.getCapacity(); i++) {
            rb.add("");
        }
        rb.read(rb.headSequence() - 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFutureSequenceThrowsException() {
        final ArrayRingbuffer rb = new ArrayRingbuffer(5);
        for (int i = 0; i < rb.getCapacity(); i++) {
            rb.add("");
        }
        rb.read(rb.tailSequence() + 1);
    }

    @Test
    public void testIsEmpty() {
        final ArrayRingbuffer rb = new ArrayRingbuffer(5);
        assertTrue(rb.isEmpty());
        rb.add("");
        assertFalse(rb.isEmpty());
    }
}
