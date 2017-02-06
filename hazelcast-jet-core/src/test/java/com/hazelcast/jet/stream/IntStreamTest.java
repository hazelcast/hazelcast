/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.jet.Distributed;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.IntSummaryStatistics;
import java.util.List;
import java.util.Map;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.PrimitiveIterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static com.hazelcast.jet.stream.impl.StreamUtil.uniqueListName;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class IntStreamTest extends AbstractStreamTest {

    private IStreamMap<String, Integer> map;
    private DistributedIntStream stream;

    @Before
    public void setupMap() {
        map = getMap();
        fillMap(map);
        stream = map.stream().mapToInt(Map.Entry::getValue);
    }

    @Test
    public void flatMapToInt() {
        int[] values = map.stream().flatMapToInt(e -> IntStream.of(e.getValue(), e.getValue())).toArray();
        Arrays.sort(values);

        for (int i = 0; i < COUNT * 2; i += 2) {
            assertEquals(i / 2, values[i]);
            assertEquals(i / 2, values[i + 1]);
        }
    }

    @Test
    public void allMatch() {
        assertTrue(stream.allMatch(f -> f < COUNT));
        assertFalse(stream.allMatch(f -> f > COUNT / 2));
    }

    @Test
    public void anyMatch() {
        assertTrue(stream.anyMatch(f -> f < COUNT / 2));
        assertFalse(stream.anyMatch(f -> f > COUNT));
    }

    @Test
    public void asDoubleStream() {
        DistributedDoubleStream doubleStream = stream.asDoubleStream();

        double[] doubles = doubleStream.toArray();
        Arrays.sort(doubles);
        for (int i = 0; i < doubles.length; i++) {
            assertEquals((double) i, doubles[i], 0.0);
        }
    }

    @Test
    public void asLongStream() {
        DistributedLongStream longStream = stream.asLongStream();

        long[] longs = longStream.toArray();
        Arrays.sort(longs);

        for (int i = 0; i < longs.length; i++) {
            assertEquals((long) i, longs[i]);
        }
    }

    @Test
    public void average() {
        OptionalDouble average = stream.average();

        assertTrue(average.isPresent());
        assertEquals((COUNT - 1) / 2.0, average.getAsDouble(), 0.0);
    }

    @Test
    public void average_whenEmpty() {
        map.clear();

        assertFalse(stream.average().isPresent());
    }

    @Test
    public void boxed() {
        DistributedStream<Integer> boxed = stream.boxed();

        IList<Integer> list = boxed.collect(DistributedCollectors.toIList(uniqueListName()));

        assertEquals(COUNT, list.size());
    }

    @Test
    public void collect() {
        Integer[] sum = stream.collect(() -> new Integer[]{0},
                (a, b) -> a[0] += b,
                (a, b) -> a[0] += b[0]);

        assertEquals(COUNT * (COUNT - 1) / 2, (int) sum[0]);
    }

    @Test
    public void count() throws Exception {
        long result = stream.count();

        assertEquals(COUNT, result);
    }

    @Test
    public void distinct() {
        int mod = 10;
        int[] values = stream.map(m -> m % mod).distinct().toArray();

        assertEquals(mod, values.length);
    }

    @Test
    public void flatMap() {
        int repetitions = 10;
        int[] ints = stream
                .filter(n -> n < repetitions)
                .flatMap(n -> IntStream.iterate(n, Distributed.IntUnaryOperator.identity()).limit(repetitions))
                .toArray();

        Arrays.sort(ints);

        for (int i = 0; i < repetitions; i++) {
            for (int j = 0; j < repetitions; j++) {
                assertEquals(i, ints[i * repetitions + j]);
            }
        }
    }

    @Test
    public void filter() {
        int[] result = stream
                .filter(f -> f < 100)
                .toArray();

        assertEquals(100, result.length);

        Arrays.sort(result);
        for (int i = 0; i < 100; i++) {
            assertEquals(i, result[i]);
        }
    }

    @Test
    public void findFirst() {
        OptionalInt first = stream.sorted().findFirst();

        assertTrue(first.isPresent());
        assertEquals(0, first.getAsInt());
    }

    @Test
    public void findFirst_whenEmpty() {
        map.clear();
        OptionalInt first = stream.findFirst();

        assertFalse(first.isPresent());
    }

    @Test
    public void findAny() {
        OptionalInt any = stream.findAny();

        assertTrue(any.isPresent());
    }

    @Test
    public void findAny_whenEmpty() {
        map.clear();
        OptionalInt any = stream.findAny();

        assertFalse(any.isPresent());
    }

    @Test
    public void forEach() {
        final AtomicInteger runningTotal = new AtomicInteger(0);

        stream.forEach(runningTotal::addAndGet);

        assertEquals(COUNT * (COUNT - 1) / 2, runningTotal.get());
    }

    @Test
    public void forEachOrdered() {
        List<Integer> values = new ArrayList<>();

        stream.sorted().forEachOrdered(values::add);

        for (int i = 0; i < COUNT; i++) {
            assertEquals(i, (int) values.get(i));
        }
    }

    @Test
    public void iterator() {
        PrimitiveIterator.OfInt iterator = stream.iterator();

        List<Integer> values = new ArrayList<>();
        while (iterator.hasNext()) {
            values.add(iterator.next());
        }

        assertEquals(COUNT, values.size());
    }

    @Test
    public void limit() {
        int limit = 10;
        int[] ints = stream.limit(limit).toArray();

        assertEquals(limit, ints.length);
    }

    @Test
    public void map() {
        int[] ints = stream.map(m -> m * m).toArray();
        Arrays.sort(ints);
        for (int i = 0; i < COUNT; i++) {
            assertEquals(i * i, ints[i]);
        }
    }

    @Test
    public void mapToDouble() {
        double[] doubles = stream.mapToDouble(m -> (double) m).toArray();

        Arrays.sort(doubles);
        for (int i = 0; i < doubles.length; i++) {
            assertEquals((double) i, doubles[i], 0.0);
        }
    }

    @Test
    public void mapToLong() {
        long[] longs = stream.mapToLong(m -> (long) m).toArray();

        Arrays.sort(longs);
        for (int i = 0; i < longs.length; i++) {
            assertEquals((long) i, longs[i]);
        }
    }

    @Test
    public void mapToObj() {
        IList<Integer> list = stream.mapToObj(m -> m).collect(DistributedCollectors.toIList(uniqueListName()));

        Object[] array = list.toArray();
        Arrays.sort(array);
        assertEquals(COUNT, array.length);

        for (int i = 0; i < array.length; i++) {
            assertEquals(i, array[i]);
        }
    }

    @Test
    public void max() {
        OptionalInt max = stream.max();

        assertTrue(max.isPresent());
        assertEquals(COUNT - 1, max.getAsInt());
    }

    @Test
    public void max_whenEmpty() {
        map.clear();
        OptionalInt max = stream.max();

        assertFalse(max.isPresent());
    }

    @Test
    public void min() {
        OptionalInt min = stream.min();

        assertTrue(min.isPresent());
        assertEquals(0, min.getAsInt());
    }

    @Test
    public void min_whenEmpty() {
        map.clear();
        OptionalInt min = stream.min();

        assertFalse(min.isPresent());
    }

    @Test
    public void noneMatch() {
        assertTrue(stream.noneMatch(f -> f > COUNT));
        assertFalse(stream.noneMatch(f -> f < COUNT / 2));
    }

    @Test
    public void reduceWithIdentity() {
        int sum = stream.reduce(0, (a, b) -> a + b);

        assertEquals(COUNT * (COUNT - 1) / 2, sum);
    }

    @Test
    public void reduceWithIdentity_whenEmpty() {
        map.clear();
        int sum = stream.reduce(0, (a, b) -> a + b);

        assertEquals(0, sum);
    }

    @Test
    public void reduce() {
        OptionalInt sum = stream.reduce((a, b) -> a + b);

        assertTrue(sum.isPresent());
        assertEquals(COUNT * (COUNT - 1) / 2, sum.getAsInt());
    }

    @Test
    public void reduce_whenEmpty() {
        map.clear();
        OptionalInt sum = stream.reduce((a, b) -> a + b);

        assertFalse(sum.isPresent());
    }

    @Test
    public void sorted() throws Exception {
        int[] array = stream.sorted().toArray();

        for (int i = 0; i < array.length; i++) {
            assertEquals(i, array[i]);
        }
    }

    @Test
    public void peek() {
        List<Integer> list = new ArrayList<>();

        int[] ints = stream.peek(list::add).toArray();

        Collections.sort(list);
        Arrays.sort(ints);

        assertEquals(COUNT, list.size());
        assertEquals(COUNT, ints.length);
        for (int i = 0; i < list.size(); i++) {
            assertEquals(i, (int) list.get(i));
            assertEquals(i, ints[i]);
        }
    }

    @Test
    public void skip() {
        int skip = 10;
        int[] ints = stream.skip(10).toArray();

        assertEquals(COUNT - skip, ints.length);
    }

    @Test
    public void sum() {
        long result = stream.sum();

        assertEquals(COUNT * (COUNT - 1) / 2, result);
    }

    @Test
    public void summaryStatistics() {
        IntSummaryStatistics intSummaryStatistics = stream.summaryStatistics();

        assertEquals(COUNT, intSummaryStatistics.getCount());
        assertEquals(COUNT - 1, intSummaryStatistics.getMax());
        assertEquals(0, intSummaryStatistics.getMin());
        assertEquals(COUNT * (COUNT - 1) / 2, intSummaryStatistics.getSum());
        assertEquals((COUNT - 1) / 2d, intSummaryStatistics.getAverage(), 0d);
    }

}
