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
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Comparator;
import java.util.DoubleSummaryStatistics;
import java.util.IntSummaryStatistics;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class CollectorsTest extends JetStreamTestSupport {

    @Test
    public void testToCollection_whenSourceMap() {
        IStreamMap<String, Integer> map = getMap(instance);
        fillMap(map);

        Set<Integer> collection = map.stream()
                .map(Map.Entry::getValue)
                .collect(DistributedCollectors.toCollection(TreeSet::new));

        assertEquals(COUNT, collection.size());

        int n = 0;
        for (int integer : collection) {
            assertEquals(n++, integer);
        }
    }

    @Test
    public void testToCollection_whenSourceList() {
        IList<Integer> list = getList(instance);
        fillList(list);

        Set<Integer> collection = list
                .stream()
                .collect(DistributedCollectors.toCollection(TreeSet::new));

        assertEquals(COUNT, collection.size());

        int n = 0;
        for (int integer : collection) {
            assertEquals(n++, integer);
        }
    }

    @Test
    public void testToList_whenSourceMap() {
        IStreamMap<String, Integer> map = getMap(instance);
        fillMap(map);

        List<Integer> list = map.stream()
                .map(Map.Entry::getValue)
                .collect(DistributedCollectors.toList());

        assertEquals(COUNT, list.size());

        list.sort(Comparator.naturalOrder());

        for (int i = 0; i < list.size(); i++) {
            assertEquals(i, (int) list.get(i));
        }
    }

    @Test
    public void testToList_whenSourceList() {
        IList<Integer> list = getList(instance);
        fillList(list);

        List<Integer> collected = list
                .stream()
                .collect(DistributedCollectors.toList());

        assertEquals(COUNT, collected.size());

        for (int i = 0; i < collected.size(); i++) {
            assertEquals(i, (int) collected.get(i));
        }
    }

    @Test
    public void testToSet_whenSourceMap() {
        IStreamMap<String, Integer> map = getMap(instance);
        fillMap(map);

        Set<Integer> collection = map.stream()
                .map(Map.Entry::getValue)
                .collect(DistributedCollectors.toSet());

        assertEquals(COUNT, collection.size());

        int n = 0;
        for (int integer : new TreeSet<>(collection)) {
            assertEquals(n++, integer);
        }
    }

    @Test
    public void testToSet_whenSourceList() {
        IList<Integer> list = getList(instance);
        fillList(list);

        Set<Integer> collection = list
                .stream()
                .collect(DistributedCollectors.toSet());

        assertEquals(COUNT, collection.size());

        int n = 0;
        for (int integer : new TreeSet<>(collection)) {
            assertEquals(n++, integer);
        }
    }

    @Test
    public void testJoining() {
        IStreamList<Integer> list = getList(instance);
        fillList(list);

        String result = list
                .stream()
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
    public void testJoiningWithDelimiter() {
        IStreamList<Integer> list = getList(instance);
        fillList(list);

        String delimiter = ", ";
        String result = list
                .stream()
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
    public void testJoiningWithDelimiterPrefixSuffix() {
        IStreamList<Integer> list = getList(instance);
        fillList(list);

        String delimiter = ", ";
        String result = list
                .stream()
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
    public void testMapping_whenSourceMap() {
        IStreamMap<String, Integer> map = getMap(instance);
        fillMap(map);

        List<Integer> collected = map.stream()
                .collect(DistributedCollectors.mapping(Map.Entry::getValue, DistributedCollectors.toList()));

        assertEquals(COUNT, collected.size());

        collected.sort(Comparator.naturalOrder());
        for (int i = 0; i < COUNT; i++) {
            assertEquals(i, (int) collected.get(i));
        }
    }

    @Test
    public void testMapping_whenSourceList() {
        IStreamList<Integer> list = getList(instance);
        fillList(list);

        List<Integer> collected = list.stream()
                .collect(DistributedCollectors.mapping(i -> i * i, DistributedCollectors.toList()));

        assertEquals(COUNT, collected.size());

        for (int i = 0; i < COUNT; i++) {
            assertEquals(i * i, (int) collected.get(i));
        }
    }

    @Test
    public void testCollectingAndThen_whenSourceList() {
        IStreamList<Integer> list = getList(instance);
        fillList(list);

        int count = list.stream()
                .collect(DistributedCollectors
                        .collectingAndThen(DistributedCollectors.toList(), List::size));

        assertEquals(COUNT, count);
    }

    @Test
    public void testCollectingAndThen_whenSourceMap() {
        IStreamMap<String, Integer> map = getMap(instance);
        fillMap(map);

        int count = map.stream()
                .collect(DistributedCollectors
                        .collectingAndThen(DistributedCollectors.toList(), List::size));

        assertEquals(COUNT, count);
    }

    @Test
    public void testCounting_whenSourceMap() throws Exception {
        IStreamMap<String, Integer> map = getMap(instance);
        fillMap(map);

        long count = map.stream().collect(DistributedCollectors.counting());

        assertEquals(COUNT, count);
    }

    @Test
    public void testCounting_whenSourceList() throws Exception {
        IStreamList<Integer> list = getList(instance);
        fillList(list);

        long count = list.stream().collect(DistributedCollectors.counting());

        assertEquals(COUNT, count);
    }

    @Test
    public void testMinBy_whenSourceList() throws Exception {
        IStreamList<Integer> list = getList(instance);
        fillList(list);

        Distributed.Optional<Integer> min = list
                .stream()
                .collect(DistributedCollectors.minBy(Distributed.Comparator.naturalOrder()));

        assertTrue(min.isPresent());
        assertEquals(0, (int) min.get());
    }

    @Test
    public void testMinBy_whenSourceEmpty() throws Exception {
        IStreamList<Integer> list = getList(instance);

        Distributed.Optional<Integer> min = list
                .stream()
                .collect(DistributedCollectors.minBy(Distributed.Comparator.naturalOrder()));

        assertFalse(min.isPresent());
    }

    @Test
    public void testMaxBy_whenSourceList() throws Exception {
        IStreamList<Integer> list = getList(instance);
        fillList(list);

        Distributed.Optional<Integer> max = list
                .stream()
                .collect(DistributedCollectors.maxBy(Distributed.Comparator.naturalOrder()));

        assertTrue(max.isPresent());
        assertEquals(COUNT - 1, (int) max.get());
    }

    @Test
    public void testMaxBy_whenSourceEmpty() throws Exception {
        IStreamList<Integer> list = getList(instance);

        Distributed.Optional<Integer> max = list
                .stream()
                .collect(DistributedCollectors.maxBy(Distributed.Comparator.naturalOrder()));

        assertFalse(max.isPresent());
    }

    @Test
    public void testSummingInt_whenSourceList() throws Exception {
        IStreamList<Integer> list = getList(instance);
        fillList(list);

        int sum = list.stream().collect(DistributedCollectors.summingInt(m -> m));

        assertEquals((COUNT * (COUNT - 1)) / 2, sum);
    }

    @Test
    public void testSummingLong_whenSourceList() throws Exception {
        IStreamList<Integer> list = getList(instance);
        fillList(list);

        long sum = list.stream().collect(DistributedCollectors.summingLong(m -> (long) m));

        assertEquals((COUNT * (COUNT - 1)) / 2, sum);
    }

    @Test
    public void testSummingDouble_whenSourceList() throws Exception {
        IStreamList<Integer> list = getList(instance);
        fillList(list);

        double sum = list.stream().collect(DistributedCollectors.summingDouble(m -> (double) m));

        assertEquals((COUNT * (COUNT - 1)) / 2, sum, 0.0);
    }

    @Test
    public void testAveragingInt_whenSourceList() throws Exception {
        IStreamList<Integer> list = getList(instance);
        fillList(list);

        double avg = list.stream().collect(DistributedCollectors.averagingInt(m -> m));

        assertEquals((COUNT - 1) / 2d, avg, 0.0);
    }

    @Test
    public void testAveragingLong_whenSourceList() throws Exception {
        IStreamList<Integer> list = getList(instance);
        fillList(list);

        double avg = list.stream().collect(DistributedCollectors.averagingLong(m -> (long) m));

        assertEquals((COUNT - 1) / 2d, avg, 0.0);
    }

    @Test
    public void testAveragingDouble_whenSourceList() throws Exception {
        IStreamList<Integer> list = getList(instance);
        fillList(list);

        double avg = list.stream().collect(DistributedCollectors.averagingDouble(m -> (double) m));

        assertEquals((COUNT - 1) / 2d, avg, 0.0);
    }

    @Test
    public void testReducingCollector_whenSourceMap() throws Exception {
        IStreamMap<String, Integer> map = getMap(instance);
        fillMap(map);

        Distributed.Optional<Integer> sum = map.stream()
                .map(Map.Entry::getValue)
                .collect(DistributedCollectors.reducing((a, b) -> a + b));

        assertTrue(sum.isPresent());
        assertEquals((COUNT - 1) * (COUNT) / 2, (int) sum.get());
    }

    @Test
    public void testReducingCollector_whenSourceEmpty() throws Exception {
        IStreamMap<String, Integer> map = getMap(instance);

        Distributed.Optional<Integer> sum = map.stream()
                .map(Map.Entry::getValue)
                .collect(DistributedCollectors.reducing((a, b) -> a + b));

        assertFalse(sum.isPresent());
    }

    @Test
    public void testReducingCollectorWithIdentity_whenSourceMap() throws Exception {
        IStreamMap<String, Integer> map = getMap(instance);
        fillMap(map);

        int sum = map.stream()
                .map(Map.Entry::getValue)
                .collect(DistributedCollectors.reducing(0, (a, b) -> a + b));

        assertEquals((COUNT - 1) * (COUNT) / 2, sum);
    }

    @Test
    public void testReducingCollectorWithMappingAndIdentity_whenSourceMap() throws Exception {
        IStreamMap<String, Integer> map = getMap(instance);
        fillMap(map);

        long sum = map.stream()
                .map(Map.Entry::getValue)
                .collect(DistributedCollectors.reducing(0L, n -> (long) n, (a, b) -> a + b));

        assertEquals((COUNT - 1) * (COUNT) / 2, sum);
    }

    @Test
    public void testGroupingBy_whenSourceList() {
        IList<Integer> list = getList(instance);
        fillList(list);

        int mod = 10;

        Map<Integer, List<Integer>> collected = list
                .stream()
                .collect(DistributedCollectors.groupingBy(m -> m % mod));

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
    public void testGroupingByWithDownstream_whenSourceList() {
        IList<Integer> list = getList(instance);
        fillList(list);

        int mod = 10;

        Map<Integer, Long> collected = list
                .stream()
                .collect(DistributedCollectors.groupingBy(m -> m % mod, DistributedCollectors.counting()));

        assertEquals(mod, collected.size());

        for (int i = 0; i < mod; i++) {
            assertEquals(COUNT / mod, (long) collected.get(i));
        }
    }

    @Test
    public void testPartitioningBy_whenSourceList() {
        IList<Integer> list = getList(instance);
        fillList(list);

        Map<Boolean, List<Integer>> partitioned = list
                .stream().collect(DistributedCollectors.partitioningBy(l -> l < COUNT / 2));

        assertEquals(2, partitioned.size());

        List<Integer> trueList = partitioned.get(true);
        List<Integer> falseList = partitioned.get(false);
        for (int i = 0; i < COUNT / 2; i++) {
            assertEquals(i, (int) trueList.get(i));
            assertEquals(i + COUNT / 2, (int) falseList.get(i));
        }
    }

    @Test
    public void testPartitioningByWithDownstream_whenSourceList() {
        IList<Integer> list = getList(instance);
        fillList(list);

        Map<Boolean, Long> partitioned = list
                .stream().collect(DistributedCollectors.partitioningBy(l -> l < COUNT / 2,
                        DistributedCollectors.counting()));

        assertEquals(2, partitioned.size());

        long trueCount = partitioned.get(true);
        long falseCount = partitioned.get(false);

        assertEquals(COUNT / 2, trueCount);
        assertEquals(COUNT / 2, falseCount);
    }

    @Test
    public void testSummarizingInt_whenSourceList() {
        IStreamList<Integer> list = getList(instance);
        fillList(list);

        IntSummaryStatistics summary
                = list.stream().collect(DistributedCollectors.summarizingInt(m -> m));

        assertEquals(COUNT, summary.getCount());
        assertEquals(COUNT - 1, summary.getMax());
        assertEquals(0, summary.getMin());
        assertEquals(COUNT * (COUNT - 1) / 2, summary.getSum());
        assertEquals((COUNT - 1) / 2d, summary.getAverage(), 0d);
    }

    @Test
    public void testSummarizingLong_whenSourceList() {
        IStreamList<Integer> list = getList(instance);
        fillList(list);

        LongSummaryStatistics summary
                = list.stream().collect(DistributedCollectors.summarizingLong(m -> (long)m));

        assertEquals(COUNT, summary.getCount());
        assertEquals(COUNT - 1, summary.getMax());
        assertEquals(0, summary.getMin());
        assertEquals(COUNT * (COUNT - 1) / 2, summary.getSum());
        assertEquals((COUNT - 1) / 2d, summary.getAverage(), 0d);
    }

    @Test
    public void testSummarizingDouble_whenSourceList() {
        IStreamList<Integer> list = getList(instance);
        fillList(list);

        DoubleSummaryStatistics summary
                = list.stream().collect(DistributedCollectors.summarizingDouble(m -> (double)m));

        assertEquals(COUNT, summary.getCount());
        assertEquals(COUNT - 1, summary.getMax(), 0d);
        assertEquals(0, summary.getMin(), 0d);
        assertEquals(COUNT * (COUNT - 1) / 2d, summary.getSum(), 0d);
        assertEquals((COUNT - 1) / 2d, summary.getAverage(), 0d);
    }

    @Test
    public void testToMap_whenSourceMap() {
        IStreamMap<String, Integer> map = getMap(instance);
        fillMap(map);

        Map<String, Integer> collected = map.stream()
                .collect(DistributedCollectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        assertEquals(COUNT, collected.size());

        for (int i = 0; i < COUNT; i++) {
            assertEquals(i, (int) collected.get("key-" + i));
        }
    }

    @Test
    public void testToMap_whenSourceList() {
        IList<Integer> list = getList(instance);
        fillList(list);

        Map<Integer, Integer> collected = list.stream().collect(DistributedCollectors.toMap(v -> v, v -> v));

        assertEquals(COUNT, collected.size());

        for (int i = 0; i < COUNT; i++) {
            assertEquals(i, (int) collected.get(i));
        }
    }

    @Test
    public void testCollect_whenSourceMap() throws Exception {
        IStreamMap<String, Integer> map = getMap(instance);
        fillMap(map);

        Integer[] collected = map.stream().collect(
                () -> new Integer[]{0},
                (r, e) -> r[0] += e.getValue(),
                (a, b) -> a[0] += b[0]
        );

        assertEquals((COUNT - 1) * (COUNT) / 2, (int) collected[0]);
    }

    @Test
    public void testCustomCollector_whenSourceMap() throws Exception {
        IStreamMap<String, Integer> map = getMap(instance);
        fillMap(map);

        int sum = map.stream().collect(
                Distributed.Collector.of(
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
    public void testCollect_whenSourceList() throws Exception {
        IStreamList<Integer> list = getList(instance);
        fillList(list);

        Integer[] collected = list.stream().collect(
                () -> new Integer[]{0},
                (r, e) -> r[0] += e,
                (a, b) -> a[0] += b[0]
        );

        assertEquals((COUNT - 1) * (COUNT) / 2, (int) collected[0]);
    }

    @Test
    public void testCustomCollector_whenSourceList() throws Exception {
        IStreamList<Integer> list = getList(instance);
        fillList(list);

        Integer[] collected = list.stream().collect(
                Distributed.Collector.of(
                        () -> new Integer[]{0},
                        (r, v) -> r[0] += v,
                        (l, r) -> {
                            l[0] += r[0];
                            return l;
                        },
                        Collector.Characteristics.IDENTITY_FINISH
                )
        );

        assertEquals((COUNT - 1) * (COUNT) / 2, (int) collected[0]);
    }


}
