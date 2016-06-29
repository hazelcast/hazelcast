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
package com.hazelcast.jet.stream;

import com.hazelcast.core.IList;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.DoubleSummaryStatistics;
import java.util.List;
import java.util.Map;
import java.util.OptionalDouble;
import java.util.PrimitiveIterator;
import java.util.stream.DoubleStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class DoubleStreamTest extends JetStreamTestSupport {

    private IStreamMap<String, Double> map;
    private DistributedDoubleStream stream;

    @Before
    public void setupMap() {
        map = getStreamMap(instance);
        fillMapDoubles(map);
        stream = map.stream().mapToDouble(Map.Entry::getValue);
    }

    private long fillMapDoubles(IStreamMap<String, Double> map) {
        for (double i = 0D; i < COUNT; i++) {
            map.put("key-" + i, i);
        }
        return COUNT;
    }

    @Test
    public void testFlatMapToDouble() {
        double[] values = map.stream().flatMapToDouble(e -> DoubleStream.of(e.getValue(), e.getValue())).toArray();
        Arrays.sort(values);

        for (int i = 0; i < COUNT * 2; i += 2) {
            assertEquals(i / 2, values[i], 0.0);
            assertEquals(i / 2, values[i + 1], 0.0);
        }
    }

    @Test
    public void testAllMatch() {
        assertTrue(stream.allMatch(f -> f < COUNT));
        assertFalse(stream.allMatch(f -> f > COUNT / 2));
    }

    @Test
    public void testAnyMatch() {
        assertTrue(stream.anyMatch(f -> f < COUNT / 2));
        assertFalse(stream.anyMatch(f -> f > COUNT));
    }

    @Test
    public void testAverage() {
        OptionalDouble average = stream.average();

        assertTrue(average.isPresent());
        assertEquals((COUNT - 1) / 2.0, average.getAsDouble(), 0D);
    }

    @Test
    public void testAverage_whenEmpty() {
        map.clear();

        assertFalse(stream.average().isPresent());
    }

    @Test
    public void testBoxed() {
        DistributedStream<Double> boxed = stream.boxed();

        IList<Double> list = boxed.collect(DistributedCollectors.toIList());

        assertEquals(COUNT, list.size());
    }

    @Test
    public void testCollect() {
        Double[] sum = stream.collect(() -> new Double[]{0D},
                (a, b) -> a[0] += b,
                (a, b) -> a[0] += b[0]);

        assertEquals(COUNT * (COUNT - 1) / 2, sum[0], 0D);
    }

    @Test
    public void testCount() throws Exception {
        long result = stream.count();

        assertEquals(COUNT, result);
    }

    @Test
    public void testDistinct() {
        double mod = 10;
        double[] values = stream.map(m -> m % mod).distinct().toArray();

        assertEquals(mod, values.length, 0D);
    }

    @Test
    public void testFlatMap() {
        int repetitions = 10;
        double[] doubles = stream
                .filter(n -> n < repetitions)
                .flatMap(n -> DoubleStream.iterate(n, Distributed.DoubleUnaryOperator.identity()).limit(repetitions))
                .toArray();

        Arrays.sort(doubles);

        for (int i = 0; i < repetitions; i++) {
            for (int j = 0; j < repetitions; j++) {
                assertEquals(i, doubles[i * repetitions + j], 0D);
            }
        }
    }

    @Test
    public void testFilter() {
        double[] result = stream
                .filter(f -> f < 100)
                .toArray();

        assertEquals(100, result.length);

        Arrays.sort(result);
        for (int i = 0; i < 100; i++) {
            assertEquals(i, result[i], 0D);
        }
    }

    @Test
    public void testFindFirst() {
        OptionalDouble first = stream.sorted().findFirst();

        assertTrue(first.isPresent());
        assertEquals(0, first.getAsDouble(), 0D);
    }

    @Test
    public void testFindFirst_whenEmpty() {
        map.clear();
        OptionalDouble first = stream.findFirst();

        assertFalse(first.isPresent());
    }

    @Test
    public void testFindAny() {
        OptionalDouble any = stream.findAny();

        assertTrue(any.isPresent());
    }

    @Test
    public void testFindAny_whenEmpty() {
        map.clear();
        OptionalDouble any = stream.findAny();

        assertFalse(any.isPresent());
    }

    @Test
    public void testForEach() {
        final Double[] runningTotal = new Double[]{0D};

        stream.forEach(m -> runningTotal[0] += m);

        assertEquals(COUNT * (COUNT - 1) / 2, runningTotal[0], 0D);
    }

    @Test
    public void testForEachOrdered() {
        List<Double> values = new ArrayList<>();

        stream.sorted().forEachOrdered(values::add);

        for (int i = 0; i < COUNT; i++) {
            assertEquals(i, values.get(i), 0D);
        }
    }

    @Test
    public void testIterator() {
        PrimitiveIterator.OfDouble iterator = stream.iterator();

        List<Double> values = new ArrayList<>();
        while (iterator.hasNext()) {
            values.add(iterator.next());
        }

        assertEquals(COUNT, values.size());
    }

    @Test
    public void testLimit() {
        long limit = 10;
        double[] doubles = stream.limit(limit).toArray();

        assertEquals(limit, doubles.length);
    }

    @Test
    public void testMap() {
        double[] doubles = stream.map(m -> m * m).toArray();
        Arrays.sort(doubles);
        for (int i = 0; i < COUNT; i++) {
            assertEquals(i * i, doubles[i], 0D);
        }
    }

    @Test
    public void testMapToLong() {
        long[] longs = stream.mapToLong(m -> (long) m).toArray();

        Arrays.sort(longs);
        for (int i = 0; i < longs.length; i++) {
            assertEquals((long) i, longs[i]);
        }
    }

    @Test
    public void testMapToInt() {
        int[] ints = stream.mapToInt(m -> (int) m).toArray();

        Arrays.sort(ints);
        for (int i = 0; i < ints.length; i++) {
            assertEquals((long) i, ints[i]);
        }
    }

    @Test
    public void testMapToObj() {
        IList<Double> list = stream.mapToObj(m -> (Double) m).collect(DistributedCollectors.toIList());

        Object[] array = list.toArray();
        Arrays.sort(array);
        assertEquals(COUNT, array.length);

        for (int i = 0; i < array.length; i++) {
            assertEquals((double) i, (Double) array[i], 0D);
        }
    }

    @Test
    public void testMax() {
        OptionalDouble max = stream.max();

        assertTrue(max.isPresent());
        assertEquals(COUNT - 1, max.getAsDouble(), 0D);
    }

    @Test
    public void testMax_whenEmpty() {
        map.clear();
        OptionalDouble max = stream.max();

        assertFalse(max.isPresent());
    }

    @Test
    public void testMin() {
        OptionalDouble min = stream.min();

        assertTrue(min.isPresent());
        assertEquals(0, min.getAsDouble(), 0D);
    }

    @Test
    public void testMin_whenEmpty() {
        map.clear();
        OptionalDouble min = stream.min();

        assertFalse(min.isPresent());
    }

    @Test
    public void testNoneMatch() {
        assertTrue(stream.noneMatch(f -> f > COUNT));
        assertFalse(stream.noneMatch(f -> f < COUNT / 2));
    }

    @Test
    public void testReduceWithIdentity() {
        double sum = stream.reduce(0, (a, b) -> a + b);

        assertEquals(COUNT * (COUNT - 1) / 2, sum, 0D);
    }

    @Test
    public void testReduceWithIdentity_whenEmpty() {
        map.clear();
        double sum = stream.reduce(0, (a, b) -> a + b);

        assertEquals(0, sum, 0D);
    }

    @Test
    public void testReduce() {
        OptionalDouble sum = stream.reduce((a, b) -> a + b);

        assertTrue(sum.isPresent());
        assertEquals(COUNT * (COUNT - 1) / 2, sum.getAsDouble(), 0D);
    }

    @Test
    public void testReduce_whenEmpty() {
        map.clear();
        OptionalDouble sum = stream.reduce((a, b) -> a + b);

        assertFalse(sum.isPresent());
    }

    @Test
    public void testSorted() throws Exception {
        double[] array = stream.sorted().toArray();

        for (int i = 0; i < array.length; i++) {
            assertEquals(i, array[i], 0D);
        }
    }

    @Test
    public void testPeek() {
        List<Double> list = new ArrayList<>();

        double[] doubles = stream.peek(list::add).toArray();

        Collections.sort(list);
        Arrays.sort(doubles);

        assertEquals(COUNT, list.size());
        assertEquals(COUNT, doubles.length);
        for (int i = 0; i < list.size(); i++) {
            assertEquals(i, list.get(i), 0D);
            assertEquals(i, doubles[i], 0D);
        }
    }

    @Test
    public void testSkip() {
        long skip = 10;
        double[] doubles = stream.skip(10).toArray();

        assertEquals(COUNT - skip, doubles.length);
    }

    @Test
    public void testSum() {
        double result = stream.sum();

        assertEquals(COUNT * (COUNT - 1) / 2, result, 0D);
    }

    @Test
    public void testSummaryStatistics() {
        DoubleSummaryStatistics longSummaryStatistics = stream.summaryStatistics();

        assertEquals(COUNT, longSummaryStatistics.getCount());
        assertEquals(COUNT - 1, longSummaryStatistics.getMax(), 0D);
        assertEquals(0, longSummaryStatistics.getMin(), 0D);
        assertEquals(COUNT * (COUNT - 1) / 2, longSummaryStatistics.getSum(), 0D);
        assertEquals((COUNT - 1) / 2D, longSummaryStatistics.getAverage(), 0D);
    }

}
