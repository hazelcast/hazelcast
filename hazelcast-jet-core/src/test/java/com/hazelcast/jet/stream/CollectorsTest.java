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

import com.hazelcast.jet.ICacheJet;
import com.hazelcast.jet.IListJet;
import com.hazelcast.jet.IMapJet;
import com.hazelcast.jet.function.DistributedComparator;
import org.junit.Test;

import java.util.Collection;
import java.util.Comparator;
import java.util.DoubleSummaryStatistics;
import java.util.IntSummaryStatistics;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CollectorsTest extends AbstractStreamTest {

    @Test
    public void map_toCollection() {
        Set<Integer> collection = streamMap()
                .map(Entry::getValue)
                .collect(DistributedCollectors.toCollection(TreeSet::new));

        assertCollection(collection);
    }

    @Test
    public void cache_toCollection() {
        Set<Integer> collection = streamCache()
                .map(Entry::getValue)
                .collect(DistributedCollectors.toCollection(TreeSet::new));

        assertCollection(collection);
    }

    @Test
    public void list_toCollection() {
        IListJet<Integer> list = getList();
        fillList(list);

        Set<Integer> collection = DistributedStream
                .fromList(list)
                .collect(DistributedCollectors.toCollection(TreeSet::new));

        assertCollection(collection);
    }

    @Test
    public void map_toList() {
        List<Integer> list = streamMap()
                .map(Entry::getValue)
                .collect(DistributedCollectors.toList());

        assertList(list);
    }

    @Test
    public void cache_toList() {
        List<Integer> list = streamCache()
                .map(Entry::getValue)
                .collect(DistributedCollectors.toList());

        assertList(list);
    }

    @Test
    public void list_toList() {
        List<Integer> collected = streamList().collect(DistributedCollectors.toList());

        assertList(collected);
    }

    @Test
    public void map_toSet() {
        Set<Integer> collection = streamMap()
                .map(Entry::getValue)
                .collect(DistributedCollectors.toSet());

        assertCollection(collection);
    }

    @Test
    public void cache_toSet() {
        Set<Integer> collection = streamCache()
                .map(Entry::getValue)
                .collect(DistributedCollectors.toSet());

        assertCollection(collection);
    }

    @Test
    public void list_toSet() {
        Set<Integer> collection = streamList().collect(DistributedCollectors.toSet());

        assertCollection(collection);
    }

    @Test
    public void joining() {
        String result = streamList()
                .map(Object::toString)
                .collect(DistributedCollectors.joining());

        int pos = 0;
        for (int i = 0; i < COUNT; i++) {
            String str = Integer.toString(i);
            int length = str.length();
            assertEquals(str, result.substring(pos, pos + length));
            pos += length;
        }
    }

    @Test
    public void joiningWithDelimiter() {
        String delimiter = ", ";
        String result = streamList()
                .map(Object::toString)
                .collect(DistributedCollectors.joining(delimiter));

        int pos = 0;
        for (int i = 0; i < COUNT; i++) {
            String str = Integer.toString(i) + ((i == COUNT - 1) ? "" : delimiter);
            int length = str.length();
            assertEquals(str, result.substring(pos, pos + length));
            pos += length;
        }
    }

    @Test
    public void joiningWithDelimiterPrefixSuffix() {
        String delimiter = ", ";
        String result = streamList()
                .map(Object::toString)
                .collect(DistributedCollectors.joining(delimiter, "[", "]"));

        int pos = 0;
        assertEquals("[", result.substring(pos, pos + 1));
        pos++;
        for (int i = 0; i < COUNT; i++) {
            String str = Integer.toString(i) + ((i == COUNT - 1) ? "" : delimiter);
            int length = str.length();
            assertEquals(str, result.substring(pos, pos + length));
            pos += length;
        }
        assertEquals("]", result.substring(pos, pos + 1));
    }

    @Test
    public void map_mapping() {
        List<Integer> collected = streamMap()
                .collect(DistributedCollectors.mapping(Entry::getValue, DistributedCollectors.toList()));

        assertList(collected);
    }

    @Test
    public void cache_mapping() {
        List<Integer> collected = streamCache()
                .collect(DistributedCollectors.mapping(Entry::getValue, DistributedCollectors.toList()));

        assertList(collected);
    }

    @Test
    public void list_mapping() {
        List<Integer> collected = streamList()
                .collect(DistributedCollectors.mapping(i -> i * i, DistributedCollectors.toList()));

        assertList(collected, true);
    }

    @Test
    public void map_collectingAndThen() {
        int count = streamMap()
                .collect(DistributedCollectors
                        .collectingAndThen(DistributedCollectors.toList(), List::size));

        assertEquals(COUNT, count);
    }

    @Test
    public void cache_collectingAndThen() {
        int count = streamCache()
                .collect(DistributedCollectors
                        .collectingAndThen(DistributedCollectors.toList(), List::size));

        assertEquals(COUNT, count);
    }

    @Test
    public void list_collectingAndThen() {
        int count = streamList()
                .collect(DistributedCollectors
                        .collectingAndThen(DistributedCollectors.toList(), List::size));

        assertEquals(COUNT, count);
    }

    @Test
    public void map_counting() {
        long count = streamMap().collect(DistributedCollectors.counting());

        assertEquals(COUNT, count);
    }

    @Test
    public void cache_counting() {
        long count = streamCache().collect(DistributedCollectors.counting());

        assertEquals(COUNT, count);
    }

    @Test
    public void list_counting() {
        long count = streamList().collect(DistributedCollectors.counting());

        assertEquals(COUNT, count);
    }

    @Test
    public void list_minBy() {
        Optional<Integer> min = streamList()
                .collect(DistributedCollectors.minBy(DistributedComparator.naturalOrder()));

        assertTrue(min.isPresent());
        assertEquals(0, (int) min.get());
    }

    @Test
    public void empty_minBy() {
        IListJet<Integer> list = getList();

        Optional<Integer> min = DistributedStream
                .fromList(list)
                .collect(DistributedCollectors.minBy(DistributedComparator.naturalOrder()));

        assertFalse(min.isPresent());
    }

    @Test
    public void list_maxBy() {
        Optional<Integer> max = streamList()
                .collect(DistributedCollectors.maxBy(DistributedComparator.naturalOrder()));

        assertTrue(max.isPresent());
        assertEquals(COUNT - 1, (int) max.get());
    }

    @Test
    public void empty_maxBy() {
        IListJet<Integer> list = getList();

        Optional<Integer> max = DistributedStream
                .fromList(list)
                .collect(DistributedCollectors.maxBy(DistributedComparator.naturalOrder()));

        assertFalse(max.isPresent());
    }

    @Test
    public void list_summingInt() {
        int sum = streamList().collect(DistributedCollectors.summingInt(m -> m));

        assertEquals((COUNT * (COUNT - 1)) / 2, sum);
    }

    @Test
    public void list_summingLong() {
        long sum = streamList().collect(DistributedCollectors.summingLong(m -> (long) m));

        assertEquals((COUNT * (COUNT - 1)) / 2, sum);
    }

    @Test
    public void list_summingDouble() {
        double sum = streamList().collect(DistributedCollectors.summingDouble(m -> (double) m));

        assertEquals((COUNT * (COUNT - 1)) / 2, sum, 0.0);
    }

    @Test
    public void list_averagingInt() {
        double avg = streamList().collect(DistributedCollectors.averagingInt(m -> m));

        assertEquals((COUNT - 1) / 2d, avg, 0.0);
    }

    @Test
    public void list_averagingLong() {
        double avg = streamList().collect(DistributedCollectors.averagingLong(m -> (long) m));

        assertEquals((COUNT - 1) / 2d, avg, 0.0);
    }

    @Test
    public void list_averagingDouble() {
        double avg = streamList().collect(DistributedCollectors.averagingDouble(m -> (double) m));

        assertEquals((COUNT - 1) / 2d, avg, 0.0);
    }

    @Test
    public void map_reducing() {
        Optional<Integer> sum = streamMap()
                .map(Entry::getValue)
                .collect(DistributedCollectors.reducing((a, b) -> a + b));

        assertTrue(sum.isPresent());
        assertEquals((COUNT - 1) * (COUNT) / 2, (int) sum.get());
    }

    @Test
    public void cache_reducing() {
        Optional<Integer> sum = streamCache()
                .map(Entry::getValue)
                .collect(DistributedCollectors.reducing((a, b) -> a + b));

        assertTrue(sum.isPresent());
        assertEquals((COUNT - 1) * (COUNT) / 2, (int) sum.get());
    }

    @Test
    public void map_empty_reducing() {
        IMapJet<String, Integer> map = getMap();

        Optional<Integer> sum = DistributedStream
                .fromMap(map)
                .map(Entry::getValue)
                .collect(DistributedCollectors.reducing((a, b) -> a + b));

        assertFalse(sum.isPresent());
    }

    @Test
    public void cache_empty_reducing() {
        ICacheJet<String, Integer> cache = getCache();

        Optional<Integer> sum = DistributedStream.Cache.fromCache(cache)
                                     .map(Entry::getValue)
                                     .collect(DistributedCollectors.reducing((a, b) -> a + b));

        assertFalse(sum.isPresent());
    }

    @Test
    public void map_reducingWithIdentity() {
        int sum = streamMap()
                .map(Entry::getValue)
                .collect(DistributedCollectors.reducing(0, (a, b) -> a + b));

        assertEquals((COUNT - 1) * (COUNT) / 2, sum);
    }

    @Test
    public void cache_reducingWithIdentity() {
        int sum = streamCache()
                .map(Entry::getValue)
                .collect(DistributedCollectors.reducing(0, (a, b) -> a + b));

        assertEquals((COUNT - 1) * (COUNT) / 2, sum);
    }

    @Test
    public void map_reducingWithMappingAndIdentity() {
        long sum = streamMap()
                .map(Entry::getValue)
                .collect(DistributedCollectors.reducing(0L, n -> (long) n, (a, b) -> a + b));

        assertEquals((COUNT - 1) * (COUNT) / 2, sum);
    }

    @Test
    public void cache_reducingWithMappingAndIdentity() {
        long sum = streamCache()
                .map(Entry::getValue)
                .collect(DistributedCollectors.reducing(0L, n -> (long) n, (a, b) -> a + b));

        assertEquals((COUNT - 1) * (COUNT) / 2, sum);
    }

    @Test
    public void list_groupingBy() {
        int mod = 10;

        Map<Integer, List<Integer>> collected = streamList().collect(DistributedCollectors.groupingBy(m -> m % mod));

        assertEquals(mod, collected.size());

        for (int i = 0; i < mod; i++) {
            List<Integer> values = collected.get(i);
            assertEquals(COUNT / mod, values.size());
            for (Integer value : values) {
                assertEquals(i, value % mod);
            }
        }
    }

    @Test
    public void list_groupingByWithDownstream() {
        int mod = 10;

        Map<Integer, Long> collected = streamList()
                .collect(DistributedCollectors.groupingBy(m -> m % mod, DistributedCollectors.counting()));

        assertEquals(mod, collected.size());

        for (int i = 0; i < mod; i++) {
            assertEquals(COUNT / mod, (long) collected.get(i));
        }
    }

    @Test
    public void list_partitioningBy() {
        Map<Boolean, List<Integer>> partitioned = streamList()
                .collect(DistributedCollectors.partitioningBy(l -> l < COUNT / 2));

        assertEquals(2, partitioned.size());

        List<Integer> trueList = partitioned.get(true);
        List<Integer> falseList = partitioned.get(false);
        for (int i = 0; i < COUNT / 2; i++) {
            assertEquals(i, (int) trueList.get(i));
            assertEquals(i + COUNT / 2, (int) falseList.get(i));
        }
    }

    @Test
    public void list_partitioningByWithDownstream() {
        Map<Boolean, Long> partitioned = streamList()
                .collect(DistributedCollectors.partitioningBy(l -> l < COUNT / 2,
                        DistributedCollectors.counting()));

        assertEquals(2, partitioned.size());

        long trueCount = partitioned.get(true);
        long falseCount = partitioned.get(false);

        assertEquals(COUNT / 2, trueCount);
        assertEquals(COUNT / 2, falseCount);
    }

    @Test
    public void list_summarizingInt() {
        IntSummaryStatistics summary = streamList().collect(DistributedCollectors.summarizingInt(m -> m));

        assertEquals(COUNT, summary.getCount());
        assertEquals(COUNT - 1, summary.getMax());
        assertEquals(0, summary.getMin());
        assertEquals(COUNT * (COUNT - 1) / 2, summary.getSum());
        assertEquals((COUNT - 1) / 2d, summary.getAverage(), 0d);
    }

    @Test
    public void list_summarizingLong() {
        LongSummaryStatistics summary = streamList().collect(DistributedCollectors.summarizingLong(m -> (long) m));

        assertEquals(COUNT, summary.getCount());
        assertEquals(COUNT - 1, summary.getMax());
        assertEquals(0, summary.getMin());
        assertEquals(COUNT * (COUNT - 1) / 2, summary.getSum());
        assertEquals((COUNT - 1) / 2d, summary.getAverage(), 0d);
    }

    @Test
    public void list_summarizingDouble() {
        DoubleSummaryStatistics summary = streamList().collect(DistributedCollectors.summarizingDouble(m -> (double) m));

        assertEquals(COUNT, summary.getCount());
        assertEquals(COUNT - 1, summary.getMax(), 0d);
        assertEquals(0, summary.getMin(), 0d);
        assertEquals(COUNT * (COUNT - 1) / 2d, summary.getSum(), 0d);
        assertEquals((COUNT - 1) / 2d, summary.getAverage(), 0d);
    }

    @Test
    public void map_toMap() {
        Map<String, Integer> collected = streamMap()
                .collect(DistributedCollectors.toMap(Entry::getKey, Entry::getValue));

        assertMap(collected);
    }

    @Test
    public void cache_toMap() {
        Map<String, Integer> collected = streamCache()
                .collect(DistributedCollectors.toMap(Entry::getKey, Entry::getValue));

        assertMap(collected);
    }

    @Test
    public void list_toMap() {
        Map<String, Integer> collected = streamList().collect(DistributedCollectors.toMap(v -> "key-" + v, v -> v));

        assertMap(collected);
    }

    @Test
    public void map_collect() {
        Integer[] collected = streamMap().collect(
                () -> new Integer[]{0},
                (r, e) -> r[0] += e.getValue(),
                (a, b) -> a[0] += b[0]
        );

        assertEquals((COUNT - 1) * (COUNT) / 2, (int) collected[0]);
    }

    @Test
    public void cache_collect() {
        Integer[] collected = streamCache().collect(
                () -> new Integer[]{0},
                (r, e) -> r[0] += e.getValue(),
                (a, b) -> a[0] += b[0]
        );

        assertEquals((COUNT - 1) * (COUNT) / 2, (int) collected[0]);
    }

    @Test
    public void map_customCollector() {
        int sum = streamMap().collect(
                DistributedCollector.of(
                        () -> new Integer[]{0},
                        (r, v) -> r[0] += v.getValue(),
                        (l, r) -> {
                            l[0] += r[0];
                            return l;
                        },
                        a -> a[0]
                )
        );

        assertEquals((COUNT - 1) * (COUNT) / 2, sum);
    }

    @Test
    public void cache_customCollector() {
        int sum = streamCache().collect(
                DistributedCollector.of(
                        () -> new Integer[]{0},
                        (r, v) -> r[0] += v.getValue(),
                        (l, r) -> {
                            l[0] += r[0];
                            return l;
                        },
                        a -> a[0]
                )
        );

        assertEquals((COUNT - 1) * (COUNT) / 2, sum);
    }

    @Test
    public void list_collect() {
        Integer[] collected = streamList().collect(
                () -> new Integer[]{0},
                (r, e) -> r[0] += e,
                (a, b) -> a[0] += b[0]
        );

        assertEquals((COUNT - 1) * (COUNT) / 2, (int) collected[0]);
    }

    @Test
    public void list_customCollector() {
        Integer[] collected = streamList().collect(
                DistributedCollector.of(
                        () -> new Integer[]{0},
                        (r, v) -> r[0] += v,
                        (l, r) -> {
                            l[0] += r[0];
                            return l;
                        }
                )
        );

        assertEquals((COUNT - 1) * (COUNT) / 2, (int) collected[0]);
    }

    private void assertCollection(Collection<Integer> collection) {
        assertEquals(COUNT, collection.size());

        int n = 0;
        for (int integer : collection) {
            assertEquals(n++, integer);
        }
    }

    private void assertList(List<Integer> list) {
        assertList(list, false);
    }

    private void assertList(List<Integer> list, boolean square) {
        assertEquals(COUNT, list.size());

        list.sort(Comparator.naturalOrder());

        for (int i = 0; i < list.size(); i++) {
            assertEquals(i * (square ? i : 1), (int) list.get(i));
        }
    }

    private void assertMap(Map<String, Integer> collected) {
        assertEquals(COUNT, collected.size());

        for (int i = 0; i < COUNT; i++) {
            assertEquals(i, (int) collected.get("key-" + i));
        }
    }


}
