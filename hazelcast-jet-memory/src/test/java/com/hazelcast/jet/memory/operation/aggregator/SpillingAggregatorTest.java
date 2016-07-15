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
import com.hazelcast.jet.memory.binarystorage.accumulator.Accumulator;
import com.hazelcast.jet.memory.binarystorage.accumulator.IntSumAccumulator;
import com.hazelcast.jet.memory.binarystorage.comparator.Comparator;
import com.hazelcast.jet.memory.binarystorage.comparator.StringComparator;
import com.hazelcast.jet.memory.memoryblock.MemoryChainingRule;
import com.hazelcast.jet.memory.memoryblock.MemoryContext;
import com.hazelcast.jet.memory.operation.OperationFactory;
import com.hazelcast.jet.memory.operation.aggregator.cursor.TupleCursor;
import com.hazelcast.jet.memory.util.Util;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class SpillingAggregatorTest extends BaseMemoryTest {
    private Aggregator aggregator;
    private IOContext ioContext = new IOContextImpl();

    protected long blockSize() {
        return 128 * 1024;
    }

    protected long heapSize() {
        return 1024 * 1024 * 1024;
    }

    @Before
    public void setUp() throws Exception {
        init();
    }

    @After
    public void tearDown() throws Exception {
        aggregator.dispose();
        cleanUp();
    }

    private void initAggregator(Comparator comparator) {
        try {
            initAggregator(comparator, null);
        } catch (IOException e) {
            throw Util.rethrow(e);
        }
    }

    private void initAggregator(Comparator comparator,
                                Accumulator binaryAccumulator) throws IOException {
        memoryContext = new MemoryContext(heapMemoryPool, nativeMemoryPool, blockSize(), useBigEndian());

        aggregator = OperationFactory.getAggregator(
                memoryContext,
                ioContext,
                MemoryChainingRule.HEAP,
                1024,//partitionCount
                65536,//spillingBufferSize
                comparator,
                new Tuple2(),
                binaryAccumulator,
                Files.createTempDirectory("hazelcast-jet-spilling").toString(),
                65536,//spillingChunkSize
                true,
                true
        );
    }

    @Test
    public void testString2String() throws Exception {
        Comparator comparator = new StringComparator();
        initAggregator(comparator);
        int CNT = 10_000_000;
        int VALUES_CNT = 1;
        int[] markers = new int[CNT];
        Arrays.fill(markers, (byte) 0);
        Tuple2<String, String> tuple = new Tuple2<>();
        long t = System.currentTimeMillis();
        for (int i = 0; i < VALUES_CNT; i++) {
            insertElements(tuple, 1, CNT, i);
        }
        System.out.println("InsertionTime=" + (System.currentTimeMillis() - t));
        int iterations_cnt = 0;
        String lastKey = "";
        int value_idx = 0;
        for (TupleCursor cursor = aggregator.cursor(); cursor.advance();) {
            Tuple2<String, String> tt = (Tuple2<String, String>) cursor.asTuple();
            if (!lastKey.equals(tt.get0())) {
                if (!(lastKey.isEmpty())) {
                    Assert.assertEquals(value_idx, VALUES_CNT);
                }
                lastKey = tt.get0();
                value_idx = 1;
            } else {
                value_idx++;
            }
            markers[Integer.valueOf(tt.get0()) - 1] += 1;
            iterations_cnt++;
        }
        Assert.assertEquals(value_idx, VALUES_CNT);
        Assert.assertEquals(CNT * VALUES_CNT, iterations_cnt);
        for (int i = 0; i < CNT; i++) {
            Assert.assertTrue(markers[i] > 0);
        }
        for (int i = 0; i < CNT; i++) {
            Assert.assertEquals("key " + i, VALUES_CNT, markers[i]);
        }
    }

    @Test
    public void testString2StringAssociativeAccumulator() throws Exception {
        initAggregator(new StringComparator(), new IntSumAccumulator());
        int CNT = 500_000;
        int VALUES_CNT = 20;
        Tuple2<String, Integer> tuple = new Tuple2<>();
        long t = System.currentTimeMillis();
        for (int i = 0; i < VALUES_CNT; i++) {
            insertElementsWithOneValue(tuple, 1, CNT, i);
            System.out.println("inserted " + CNT);
        }
        System.out.println("InsertionTime=" + (System.currentTimeMillis() - t));
        int iterations_cnt = 0;
        for (TupleCursor cursor = aggregator.cursor(); cursor.advance();) {
            Tuple2<String, Integer> tt = (Tuple2) cursor.asTuple();
            Assert.assertEquals(VALUES_CNT, (int) tt.get1());
            iterations_cnt++;
        }
        Assert.assertEquals(CNT, iterations_cnt);
    }


    @Test
    public void testString2StringNonAssociativeAccumulator() throws Exception {
        initAggregator(new StringComparator(), new NonAssociativeSumAccumulator());
        int CNT = 2_000_000;
        int VALUES_CNT = 2;
        Tuple2<String, Integer> tuple = new Tuple2<>();
        long t = System.currentTimeMillis();
        for (int i = 0; i < VALUES_CNT; i++) {
            insertElementsWithOneValue(tuple, 1, CNT, i);
            System.out.println("inserted " + CNT);
        }
        System.out.println("InsertionTime=" + (System.currentTimeMillis() - t));
        int iterations_cnt = 0;
        for (TupleCursor cursor = aggregator.cursor(); cursor.advance();) {
            Tuple2<String, Integer> tt = (Tuple2<String, Integer>) cursor.asTuple();
            Assert.assertEquals(VALUES_CNT, (int) tt.get1());
            iterations_cnt++;
        }
        Assert.assertEquals(CNT, iterations_cnt);
    }

    private void insertElementsWithOneValue(Tuple2<String, Integer> tuple, int start, int elementsCount, int step)
    throws Exception {
        for (int i = start; i <= elementsCount; i++) {
            tuple.set0(String.valueOf(i));
            tuple.set1(1);
            insertNewRecord(tuple, elementsCount, step, i);
        }
    }

    private void insertNewRecord(Tuple2 tuple, int elementsCount, int step, int i) throws Exception {
        boolean result = aggregator.accept(tuple);
        if (!result) {
            long t = System.currentTimeMillis();
            System.out.println("Start spilling i=" + i);
            aggregator.startSpilling();
            while (!aggregator.spillNextChunk()) {
            }
            aggregator.finishSpilling();
            aggregator.accept(tuple);
            System.out.println("Spilled " + elementsCount
                    + " start i=" + i +
                    " time=" + (System.currentTimeMillis() - t)
                    + " step=" + step

            );
        }
    }

    private void insertElements(Tuple2<String, String> tuple, int start, int elementsCount, int step)
    throws Exception {
        for (int i = start; i <= elementsCount; i++) {
            tuple.set0(String.valueOf(i));
            tuple.set1(String.valueOf(i));
            insertNewRecord(tuple, elementsCount, step, i);
        }
    }
}
