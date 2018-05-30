/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.jet.IMapJet;
import com.hazelcast.jet.function.DistributedLongUnaryOperator;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.PrimitiveIterator;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.LongStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class LongStreamTest extends AbstractStreamTest {

    private IMapJet<String, Long> map;
    private DistributedLongStream stream;

    @Before
    public void setupMap() {
        map = getMap();
        fillMapLongs(map);
        stream = DistributedStream.fromMap(map).mapToLong(Map.Entry::getValue);
    }

    private static long fillMapLongs(IMapJet<String, Long> map) {
        for (int i = 0; i < COUNT; i++) {
            map.put("key-" + i, (long) i);
        }
        return COUNT;
    }

    @Test
    public void flatMapToLong() {
        long[] values = DistributedStream
                .fromMap(map)
                .flatMapToLong(e -> LongStream.of(e.getValue(), e.getValue()))
                .toArray();
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
        DistributedStream<Long> boxed = stream.boxed();

        IList<Long> list = boxed.collect(DistributedCollectors.toIList(randomString()));

        assertEquals(COUNT, list.size());
    }

    @Test
    public void collect() {
        Long[] sum = stream.collect(() -> new Long[]{0L},
                (a, b) -> a[0] += b,
                (a, b) -> a[0] += b[0]);

        assertEquals(COUNT * (COUNT - 1) / 2, (long) sum[0]);
    }

    @Test
    public void count() {
        long result = stream.count();

        assertEquals(COUNT, result);
    }

    @Test
    public void distinct() {
        long mod = 10;
        long[] values = stream.map(m -> m % mod).distinct().toArray();

        assertEquals(mod, values.length);
    }

    @Test
    public void flatMap() {
        int repetitions = 10;
        long[] longs = stream
                .filter(n -> n < repetitions)
                .flatMap(n -> LongStream.iterate(n, DistributedLongUnaryOperator.identity()).limit(repetitions))
                .toArray();

        Arrays.sort(longs);

        for (int i = 0; i < repetitions; i++) {
            for (int j = 0; j < repetitions; j++) {
                assertEquals(i, longs[i * repetitions + j]);
            }
        }
    }

    @Test
    public void filter() {
        long[] result = stream
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
        OptionalLong first = stream.sorted().findFirst();

        assertTrue(first.isPresent());
        assertEquals(0, first.getAsLong());
    }

    @Test
    public void findFirst_whenEmpty() {
        map.clear();
        OptionalLong first = stream.findFirst();

        assertFalse(first.isPresent());
    }

    @Test
    public void findAny() {
        OptionalLong any = stream.findAny();

        assertTrue(any.isPresent());
    }

    @Test
    public void findAny_whenEmpty() {
        map.clear();
        OptionalLong any = stream.findAny();

        assertFalse(any.isPresent());
    }

    @Test
    public void forEach() {
        final AtomicLong runningTotal = new AtomicLong(0);

        stream.forEach(runningTotal::addAndGet);

        assertEquals(COUNT * (COUNT - 1) / 2, runningTotal.get());
    }

    @Test
    public void forEachOrdered() {
        List<Long> values = new ArrayList<>();

        stream.sorted().forEachOrdered(values::add);

        for (int i = 0; i < COUNT; i++) {
            assertEquals(i, (long) values.get(i));
        }
    }

    @Test
    public void iterator() {
        PrimitiveIterator.OfLong iterator = stream.iterator();

        List<Long> values = new ArrayList<>();
        while (iterator.hasNext()) {
            values.add(iterator.next());
        }

        assertEquals(COUNT, values.size());
    }

    @Test
    public void limit() {
        long limit = 10;
        long[] longs = stream.limit(limit).toArray();

        assertEquals(limit, longs.length);
    }

    @Test
    public void map() {
        long[] longs = stream.map(m -> m * m).toArray();
        Arrays.sort(longs);
        for (int i = 0; i < COUNT; i++) {
            assertEquals(i * i, longs[i]);
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
    public void mapToInt() {
        int[] longs = stream.mapToInt(m -> (int) m).toArray();

        Arrays.sort(longs);
        for (int i = 0; i < longs.length; i++) {
            assertEquals((long) i, longs[i]);
        }
    }

    @Test
    public void mapToObj() {
        IList<Long> list = stream.mapToObj(m -> m).collect(DistributedCollectors.toIList(randomString()));

        Object[] array = list.toArray();
        Arrays.sort(array);
        assertEquals(COUNT, array.length);

        for (int i = 0; i < array.length; i++) {
            assertEquals((long) i, array[i]);
        }
    }

    @Test
    public void max() {
        OptionalLong max = stream.max();

        assertTrue(max.isPresent());
        assertEquals(COUNT - 1, max.getAsLong());
    }

    @Test
    public void max_whenEmpty() {
        map.clear();
        OptionalLong max = stream.max();

        assertFalse(max.isPresent());
    }

    @Test
    public void min() {
        OptionalLong min = stream.min();

        assertTrue(min.isPresent());
        assertEquals(0, min.getAsLong());
    }

    @Test
    public void min_whenEmpty() {
        map.clear();
        OptionalLong min = stream.min();

        assertFalse(min.isPresent());
    }

    @Test
    public void noneMatch() {
        assertTrue(stream.noneMatch(f -> f > COUNT));
        assertFalse(stream.noneMatch(f -> f < COUNT / 2));
    }

    @Test
    public void reduceWithIdentity() {
        long sum = stream.reduce(0, (a, b) -> a + b);

        assertEquals(COUNT * (COUNT - 1) / 2, sum);
    }

    @Test
    public void reduceWithIdentity_whenEmpty() {
        map.clear();
        long sum = stream.reduce(0, (a, b) -> a + b);

        assertEquals(0, sum);
    }

    @Test
    public void reduce() {
        OptionalLong sum = stream.reduce((a, b) -> a + b);

        assertTrue(sum.isPresent());
        assertEquals(COUNT * (COUNT - 1) / 2, sum.getAsLong());
    }

    @Test
    public void reduce_whenEmpty() {
        map.clear();
        OptionalLong sum = stream.reduce((a, b) -> a + b);

        assertFalse(sum.isPresent());
    }

    @Test
    public void sorted() {
        long[] array = stream.sorted().toArray();

        for (int i = 0; i < array.length; i++) {
            assertEquals(i, array[i]);
        }
    }

    @Test
    public void peek() {
        List<Long> list = new ArrayList<>();

        long[] longs = stream.peek(list::add).toArray();

        Collections.sort(list);
        Arrays.sort(longs);

        assertEquals(COUNT, list.size());
        assertEquals(COUNT, longs.length);
        for (int i = 0; i < list.size(); i++) {
            assertEquals(i, (long) list.get(i));
            assertEquals(i, longs[i]);
        }
    }

    @Test
    public void skip() {
        long skip = 10;
        long[] longs = stream.skip(10).toArray();

        assertEquals(COUNT - skip, longs.length);
    }

    @Test
    public void sum() {
        long result = stream.sum();

        assertEquals(COUNT * (COUNT - 1) / 2, result);
    }

    @Test
    public void summaryStatistics() {
        LongSummaryStatistics longSummaryStatistics = stream.summaryStatistics();

        assertEquals(COUNT, longSummaryStatistics.getCount());
        assertEquals(COUNT - 1, longSummaryStatistics.getMax());
        assertEquals(0, longSummaryStatistics.getMin());
        assertEquals(COUNT * (COUNT - 1) / 2, longSummaryStatistics.getSum());
        assertEquals((COUNT - 1) / 2.0, longSummaryStatistics.getAverage(), 0.0);
    }

}
