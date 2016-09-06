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

import com.hazelcast.jet.io.SerializationOptimizer;
import com.hazelcast.jet.io.Pair;
import com.hazelcast.jet.memory.BaseMemoryTest;
import com.hazelcast.jet.memory.binarystorage.accumulator.Accumulator;
import com.hazelcast.jet.memory.binarystorage.accumulator.IntSumAccumulator;
import com.hazelcast.jet.memory.binarystorage.comparator.Comparator;
import com.hazelcast.jet.memory.binarystorage.comparator.StringComparator;
import com.hazelcast.jet.memory.memoryblock.MemoryChainingRule;
import com.hazelcast.jet.memory.memoryblock.MemoryContext;
import com.hazelcast.jet.memory.operation.OperationFactory;
import com.hazelcast.jet.memory.operation.aggregator.cursor.PairCursor;
import com.hazelcast.jet.memory.util.Util;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
@Ignore
public class SpillingAggregatorTest extends BaseMemoryTest {
    private Aggregator aggregator;
    private SerializationOptimizer optimizer = new SerializationOptimizer();

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
                optimizer,
                MemoryChainingRule.HEAP,
                1024,//partitionCount
                65536,//spillingBufferSize
                comparator,
                new Pair(),
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
        Pair<String, String> pair = new Pair<>();
        long t = System.currentTimeMillis();
        for (int i = 0; i < VALUES_CNT; i++) {
            insertElements(pair, 1, CNT, i);
        }
        System.out.println("InsertionTime=" + (System.currentTimeMillis() - t));
        int iterations_cnt = 0;
        String lastKey = "";
        int value_idx = 0;
        for (PairCursor cursor = aggregator.cursor(); cursor.advance();) {
            Pair<String, String> tt = (Pair<String, String>) cursor.asPair();
            if (!lastKey.equals(tt.getKey())) {
                if (!(lastKey.isEmpty())) {
                    Assert.assertEquals(value_idx, VALUES_CNT);
                }
                lastKey = tt.getKey();
                value_idx = 1;
            } else {
                value_idx++;
            }
            markers[Integer.valueOf(tt.getKey()) - 1] += 1;
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
        Pair<String, Integer> pair = new Pair<>();
        long t = System.currentTimeMillis();
        for (int i = 0; i < VALUES_CNT; i++) {
            insertElementsWithOneValue(pair, 1, CNT, i);
            System.out.println("inserted " + CNT);
        }
        System.out.println("InsertionTime=" + (System.currentTimeMillis() - t));
        int iterations_cnt = 0;
        for (PairCursor cursor = aggregator.cursor(); cursor.advance();) {
            Pair<String, Integer> tt = (Pair) cursor.asPair();
            Assert.assertEquals(VALUES_CNT, (int) tt.getValue());
            iterations_cnt++;
        }
        Assert.assertEquals(CNT, iterations_cnt);
    }


    @Test
    public void testString2StringNonAssociativeAccumulator() throws Exception {
        initAggregator(new StringComparator(), new NonAssociativeSumAccumulator());
        int CNT = 2_000_000;
        int VALUES_CNT = 2;
        Pair<String, Integer> pair = new Pair<>();
        long t = System.currentTimeMillis();
        for (int i = 0; i < VALUES_CNT; i++) {
            insertElementsWithOneValue(pair, 1, CNT, i);
            System.out.println("inserted " + CNT);
        }
        System.out.println("InsertionTime=" + (System.currentTimeMillis() - t));
        int iterations_cnt = 0;
        for (PairCursor cursor = aggregator.cursor(); cursor.advance();) {
            Pair<String, Integer> tt = (Pair<String, Integer>) cursor.asPair();
            Assert.assertEquals(VALUES_CNT, (int) tt.getValue());
            iterations_cnt++;
        }
        Assert.assertEquals(CNT, iterations_cnt);
    }

    private void insertElementsWithOneValue(Pair<String, Integer> pair, int start, int elementsCount, int step)
    throws Exception {
        for (int i = start; i <= elementsCount; i++) {
            pair.setKey(String.valueOf(i));
            pair.setValue(1);
            insertNewRecord(pair, elementsCount, step, i);
        }
    }

    private void insertNewRecord(Pair pair, int elementsCount, int step, int i) throws Exception {
        boolean result = aggregator.accept(pair);
        if (!result) {
            long t = System.currentTimeMillis();
            System.out.println("Start spilling i=" + i);
            aggregator.startSpilling();
            while (!aggregator.spillNextChunk()) {
            }
            aggregator.finishSpilling();
            aggregator.accept(pair);
            System.out.println("Spilled " + elementsCount
                    + " start i=" + i +
                    " time=" + (System.currentTimeMillis() - t)
                    + " step=" + step

            );
        }
    }

    private void insertElements(Pair<String, String> pair, int start, int elementsCount, int step)
    throws Exception {
        for (int i = start; i <= elementsCount; i++) {
            pair.setKey(String.valueOf(i));
            pair.setValue(String.valueOf(i));
            insertNewRecord(pair, elementsCount, step, i);
        }
    }
}
