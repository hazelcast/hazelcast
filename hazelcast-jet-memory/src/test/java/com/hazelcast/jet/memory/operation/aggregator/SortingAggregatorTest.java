/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.memory.operation.aggregator;

import com.hazelcast.jet.io.IOContext;
import com.hazelcast.jet.io.impl.IOContextImpl;
import com.hazelcast.jet.io.tuple.Tuple;
import com.hazelcast.jet.io.tuple.Tuple2;
import com.hazelcast.jet.memory.BaseMemoryTest;
import com.hazelcast.jet.memory.JetMemoryException;
import com.hazelcast.jet.memory.binarystorage.SortOrder;
import com.hazelcast.jet.memory.binarystorage.accumulator.Accumulator;
import com.hazelcast.jet.memory.binarystorage.accumulator.IntSumAccumulator;
import com.hazelcast.jet.memory.binarystorage.comparator.Comparator;
import com.hazelcast.jet.memory.binarystorage.comparator.StringComparator;
import com.hazelcast.jet.memory.memoryblock.MemoryChainingRule;
import com.hazelcast.jet.memory.memoryblock.MemoryContext;
import com.hazelcast.jet.memory.operation.OperationFactory;
import com.hazelcast.jet.memory.operation.aggregator.cursor.TupleCursor;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;

import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class SortingAggregatorTest extends BaseMemoryTest {
    private SortedAggregator aggregator;
    private IOContext ioContext = new IOContextImpl();

    @Override
    protected long heapSize() {
        return 200L * 1024L * 1024L;
    }

    @Override
    protected long blockSize() {
        return 128 * 1024;
    }

    @Before
    public void setUp() throws Exception {
        init();
    }

    private void initAggregator(Comparator comparator) {
        initAggregator(comparator, null);
    }

    private void initAggregator(Comparator comparator, Accumulator binaryAccumulator) {
        memoryContext = new MemoryContext(heapMemoryPool, nativeMemoryPool, blockSize(), useBigEndian());
        aggregator = OperationFactory.getSortedAggregator(memoryContext, ioContext, MemoryChainingRule.HEAP,
                2,//partitionCount
                1024,//spillingBufferSize
                comparator,
                new Tuple2(),
                binaryAccumulator,
                "",
                SortOrder.ASC,
                1024,//spillingChunkSize
                false,
                true
        );
    }

    private void insertElements(Tuple2<String, String> tuple, int start, int end
    ) throws Exception {
        for (int i = end; i >= start; i--) {
            tuple.set0(String.valueOf(i));
            tuple.set1(String.valueOf(i));
            if (!aggregator.accept(tuple)) {
                throw new JetMemoryException("Not enough memory (spilling is turned off)");
            }
        }
    }

    @Test
    public void testString2String() throws Exception {
        initAggregator(new StringComparator());
        Tuple2<String, String> tuple = new Tuple2<>();
        int CNT = 1_000_000;
        long t = System.currentTimeMillis();
        insertElements(tuple, 1, CNT);
        System.out.println("InsertionTime=" + (System.currentTimeMillis() - t));
        t = System.currentTimeMillis();
        final SortedAggregator aggregator = this.aggregator;
        aggregator.prepareToSort();
        while (!aggregator.sort()) {
        }
        System.out.println("SortingTime=" + (System.currentTimeMillis() - t));
        long time = System.currentTimeMillis();
        String previous = null;
        int iterations_count = 0;
        for (TupleCursor cursor = aggregator.cursor(); cursor.advance();) {
            Tuple2<String, Integer> tt = (Tuple2<String, Integer>) cursor.asTuple();
            assertTrue(previous == null || tt.get0().compareTo(previous) > 0);
            previous = tt.get0();
            iterations_count++;
        }
        Assert.assertEquals(CNT, iterations_count);
        System.out.println("SelectionTime=" + (System.currentTimeMillis() - time));
    }

    @Test
    public void testString2StringMultiValue() throws Exception {
        initAggregator(new StringComparator());
        Tuple2<String, String> tuple = new Tuple2<>();
        int KEYS_CNT = 100_000;
        int VALUES_CNT = 10;
        byte[] markers = new byte[KEYS_CNT];
        Arrays.fill(markers, (byte) 0);
        long t = System.currentTimeMillis();
        for (int i = 1; i <= 100_000; i++) {
            tuple.set0(String.valueOf(i));
            for (int ii = 0; ii < 10; ii++) {
                tuple.set1(String.valueOf(ii));
                if (!aggregator.accept(tuple)) {
                    throw new JetMemoryException("Not enough memory (spilling is turned off)");
                }
            }
        }
        System.out.println("InsertionTime=" + (System.currentTimeMillis() - t));
        t = System.currentTimeMillis();
        aggregator.prepareToSort();
        do {
        } while (!aggregator.sort());
        System.out.println("SortingTime=" + (System.currentTimeMillis() - t));
        int value_offset = 0;
        String previous = null;
        int iterations_count = 0;
        for (TupleCursor cursor = aggregator.cursor(); cursor.advance();) {
            Tuple2<String, Integer> tt = (Tuple2<String, Integer>) cursor.asTuple();
            String key = tt.get0();
            if (value_offset == 0) {
                if (previous != null) {
                    assertTrue(tt.get0().compareTo(previous) > 0);
                }
                previous = key;
                value_offset++;
            } else {
                Assert.assertEquals(key, previous);
                if (value_offset < VALUES_CNT - 1) {
                    value_offset++;
                } else if (value_offset == VALUES_CNT - 1) {
                    value_offset = 0;
                }

                previous = key;
            }

            iterations_count++;
        }

        Assert.assertEquals(KEYS_CNT * VALUES_CNT, iterations_count);
    }

    @Test
    public void testString2StringAssociativeAccumulator() throws Exception {
        initAggregator(new StringComparator(), new IntSumAccumulator());
        testAccumulator();
    }

    @Test
    public void testString2StringNonAssociativeAccumulator() throws Exception {
        initAggregator(new StringComparator(), new NonAssociativeSumAccumulator());
        testAccumulator();
    }

    private void testAccumulator() throws Exception {
        Tuple2<String, Integer> tuple = new Tuple2<String, Integer>();

        int KEYS_CNT = 100_000;
        Integer VALUES_CNT = 10;

        long t = System.currentTimeMillis();
        for (int i = 1; i <= KEYS_CNT; i++) {
            tuple.set0(String.valueOf(i));
            for (int ii = 0; ii < VALUES_CNT; ii++) {
                tuple.set1(1);
                aggregator.accept(tuple);
            }
        }

        System.out.println("InsertionTime=" + (System.currentTimeMillis() - t));

        long time = System.currentTimeMillis();

        aggregator.prepareToSort();
        do {
        } while (!aggregator.sort());
        System.out.println("SortingTime=" + (System.currentTimeMillis() - time));

        time = System.currentTimeMillis();
        String previous = null;
        int iterations_count = 0;
        for (TupleCursor cursor = aggregator.cursor(); cursor.advance();) {
            Tuple2<String, Integer> tt = (Tuple2<String, Integer>) cursor.asTuple();
            if (previous != null) {
                assertTrue(tt.get0().compareTo(previous) > 0);
            }
            Assert.assertEquals("Iteration=" + iterations_count + " " + tt.get0(), VALUES_CNT, tt.get1());
            previous = tt.get0();
            iterations_count++;
        }
        Assert.assertEquals(KEYS_CNT, iterations_count);
        System.out.println("SelectionTime=" + (System.currentTimeMillis() - time));
    }

    @After
    public void tearDown() throws Exception {
        aggregator.dispose();
        cleanUp();
    }
}
