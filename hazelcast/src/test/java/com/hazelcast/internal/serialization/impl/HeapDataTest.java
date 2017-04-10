package com.hazelcast.internal.serialization.impl;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class HeapDataTest {

    @Test
    public void totalSize_whenNonEmpty() {
        HeapData heapData = new HeapData(new byte[10]);
        assertEquals(10, heapData.totalSize());
    }

    @Test
    public void totalSize_whenEmpty() {
        HeapData heapData = new HeapData(new byte[0]);
        assertEquals(0, heapData.totalSize());
    }

    @Test
    public void totalSize_whenNullByteArray() {
        HeapData heapData = new HeapData(null);
        assertEquals(0, heapData.totalSize());
    }

    @Test
    public void copyTo() {
        byte[] inputBytes = "12345678890".getBytes();
        HeapData heap = new HeapData(inputBytes);
        byte[] bytes = new byte[inputBytes.length];
        heap.copyTo(bytes, 0);

        assertEquals(new String(inputBytes), new String(bytes));
    }
}
