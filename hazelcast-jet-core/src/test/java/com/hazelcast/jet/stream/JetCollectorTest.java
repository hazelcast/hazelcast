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
import com.hazelcast.core.IMap;
import org.junit.Test;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.hazelcast.jet.stream.impl.StreamUtil.uniqueListName;
import static com.hazelcast.jet.stream.impl.StreamUtil.uniqueMapName;
import static com.hazelcast.jet.stream.DistributedCollectors.groupingByToIMap;
import static com.hazelcast.jet.stream.DistributedCollectors.toIList;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class JetCollectorTest extends AbstractStreamTest {

    @Test
    public void imapCollect_whenNoIntermediaries() throws Exception {
        IStreamMap<String, Integer> map = getMap();
        fillMap(map);

        IMap<String, Integer> collected = map.stream().collect(DistributedCollectors.toIMap(uniqueMapName()));

        assertEquals(COUNT, collected.size());
        for (int i = 0; i < COUNT; i++) {
            Integer val = collected.get("key-" + i);
            assertEquals(i, (int) val);
        }
    }

    @Test
    public void imapCollectWithMerge() throws Exception {
        IStreamMap<String, Integer> map = getMap();
        fillMap(map);

        IMap<Integer, Integer> collected = map.stream()
                .collect(DistributedCollectors.toIMap(uniqueMapName(), e -> Integer.parseInt(e.getKey().split("-")[1]),
                        e -> e.getValue() * 2, (l, r) -> l));

        assertEquals(COUNT, collected.size());

        for (int i = 0; i < COUNT; i++) {
            int value = collected.get(i);
            assertNotNull(String.valueOf(i), value);
            assertEquals(i * 2, value);
        }
    }

    @Test
    public void grouping_whenSourceMap() throws Exception {
        IStreamMap<String, Integer> map = getMap();
        fillMap(map);

        int mod = 10;

        IMap<Integer, List<Integer>> collected = map
                .stream()
                .map(Map.Entry::getValue)
                .collect(groupingByToIMap(uniqueMapName(), m -> m % mod));

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
    public void twoLevelGrouping_whenSourceMap() throws Exception {
        IStreamMap<String, Integer> map = getMap();
        fillMap(map);

        int mod = 10;

        IMap<Integer, Map<Integer, List<Integer>>> collected = map
                .stream()
                .map(Map.Entry::getValue)
                .collect(groupingByToIMap(
                        uniqueMapName(),
                        m -> m < COUNT / 2 ? 0 : 1,
                        DistributedCollectors.groupingBy(m -> m % mod)));

        assertEquals(2, collected.size());

        for (int i = 0; i < 2; i++) {
            Map<Integer, List<Integer>> modGroup = collected.get(i);
            assertEquals(mod, modGroup.size());

            for (int j = 0; j < mod; j++) {
                List<Integer> values = modGroup.get(j);
                assertEquals(COUNT / 2 / mod, values.size());
                for (Integer value : values) {
                    assertEquals(j, value % mod);
                }
            }
        }
    }

    @Test
    public void grouping_whenSourceList() throws Exception {
        IStreamList<Integer> list = getList();
        fillList(list);

        int mod = 10;

        IMap<Integer, List<Integer>> collected = list
                .stream()
                .collect(groupingByToIMap(uniqueMapName(), m -> m % mod));

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
    public void wordCount() throws Exception {
        IStreamMap<String, String> map = getMap();
        String words = "0 1 2 3 4 5 6 7 8 9";
        for (int i = 0; i < COUNT; i++) {
            map.put("key-" + i, words);
        }

        IMap<String, Integer> collected = map.stream()
                .flatMap(m -> Stream.of(m.getValue().split("\\s")))
                .collect(DistributedCollectors.toIMap(uniqueMapName(), v -> v, v -> 1, (l, r) -> l + r));

        assertEquals(10, collected.size());

        for (Integer count : collected.values()) {
            assertEquals(COUNT, (int) count);
        }
    }

    @Test
    public void imapCollectWithMerge_whenSourceList() throws Exception {
        IStreamList<String> list = getList();
        String words = "0 1 2 3 4 5 6 7 8 9";
        for (int i = 0; i < COUNT; i++) {
            list.add(words);
        }

        IMap<String, Integer> collected = list.stream()
                                              .flatMap(m -> Stream.of(m.split("\\s")))
                                              .collect(DistributedCollectors.toIMap(uniqueMapName(), v -> v, v -> 1, (l, r) -> l + r));

        assertEquals(10, collected.size());

        for (Integer count : collected.values()) {
            assertEquals(COUNT, (int) count);
        }
    }

    @Test
    public void ilistCollect_whenNoIntermediaries() throws Exception {
        IStreamList<Integer> list = getList();
        fillList(list);

        IStreamList<Integer> collected = list.stream().collect(toIList(uniqueListName()));

        assertArrayEquals(list.toArray(), collected.toArray());
    }

    @Test
    public void ilistCollect_whenSourceMap() throws Exception {
        IStreamMap<String, Integer> map = getMap();
        fillMap(map);

        IList<Map.Entry<String, Integer>> collected = map.stream().collect(toIList(uniqueListName()));

        Map.Entry<String, Integer>[] expecteds = map.entrySet().toArray(new Map.Entry[0]);
        Map.Entry<String, Integer>[] actuals = collected.toArray(new Map.Entry[0]);

        Comparator<Map.Entry<String, Integer>> entryComparator = Comparator.comparing(Entry::getKey);
        Arrays.sort(expecteds, entryComparator);
        Arrays.sort(actuals, entryComparator);

        assertArrayEquals(expecteds, actuals);
    }
}
