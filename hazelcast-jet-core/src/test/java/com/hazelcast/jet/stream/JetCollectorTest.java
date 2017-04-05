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

import com.hazelcast.cache.ICache;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import org.junit.Test;

import javax.cache.Cache;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Stream;

import static com.hazelcast.jet.stream.DistributedCollectors.groupingByToICache;
import static com.hazelcast.jet.stream.DistributedCollectors.groupingByToIMap;
import static com.hazelcast.jet.stream.DistributedCollectors.toICache;
import static com.hazelcast.jet.stream.DistributedCollectors.toIList;
import static com.hazelcast.jet.stream.DistributedCollectors.toIMap;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class JetCollectorTest extends AbstractStreamTest {

    @Test
    public void imapCollect_whenNoIntermediaries() throws Exception {
        IMap<String, Integer> collected = streamMap().collect(toIMap(randomName()));

        assertEquals(COUNT, collected.size());
        for (int i = 0; i < COUNT; i++) {
            Integer val = collected.get("key-" + i);
            assertEquals(i, (int) val);
        }
    }

    @Test
    public void icacheCollect_whenNoIntermediaries() throws Exception {
        ICache<String, Integer> collected = streamMap().collect(toICache(randomName()));

        assertEquals(COUNT, collected.size());
        for (int i = 0; i < COUNT; i++) {
            Integer val = collected.get("key-" + i);
            assertEquals(i, (int) val);
        }
    }

    @Test
    public void imapCollectWithMerge() throws Exception {
        IMap<Integer, Integer> collected = streamMap()
                .collect(toIMap(randomName(), e -> Integer.parseInt(e.getKey().split("-")[1]),
                        e -> e.getValue() * 2, (l, r) -> l));

        assertEquals(COUNT, collected.size());

        for (int i = 0; i < COUNT; i++) {
            int value = collected.get(i);
            assertNotNull(String.valueOf(i), value);
            assertEquals(i * 2, value);
        }
    }

    @Test
    public void icacheCollectWithMerge() throws Exception {
        ICache<Integer, Integer> collected = streamMap()
                .collect(toICache(randomName(),
                        e -> Integer.parseInt(e.getKey().split("-")[1]),
                        e -> e.getValue() * 2, (l, r) -> l));

        assertEquals(COUNT, collected.size());

        for (int i = 0; i < COUNT; i++) {
            int value = collected.get(i);
            assertNotNull(String.valueOf(i), value);
            assertEquals(i * 2, value);
        }
    }


    @Test
    public void grouping_whenSourceMap_toIMap() throws Exception {
        int mod = 10;

        IMap<Integer, List<Integer>> collected = streamMap()
                .map(Entry::getValue)
                .collect(groupingByToIMap(randomName(), m -> m % mod));

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
    public void grouping_whenSourceMap_toICache() throws Exception {
        int mod = 10;

        ICache<Integer, List<Integer>> collected = streamMap()
                .map(Entry::getValue)
                .collect(groupingByToICache(randomName(), m -> m % mod));

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
    public void grouping_whenSourceCache_toIMap() throws Exception {
        int mod = 10;

        IMap<Integer, List<Integer>> collected = streamCache()
                .map(Entry::getValue)
                .collect(groupingByToIMap(randomName(), m -> m % mod));

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
    public void grouping_whenSourceCache_toICache() throws Exception {
        int mod = 10;

        ICache<Integer, List<Integer>> collected = streamCache()
                .map(Entry::getValue)
                .collect(groupingByToICache(randomName(), m -> m % mod));

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
    public void twoLevelGrouping_whenSourceMap_toMap() throws Exception {
        int mod = 10;

        IMap<Integer, Map<Integer, List<Integer>>> collected = streamMap()
                .map(Entry::getValue)
                .collect(groupingByToIMap(
                        randomName(),
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
    public void twoLevelGrouping_whenSourceMap_toCache() throws Exception {
        int mod = 10;

        ICache<Integer, Map<Integer, List<Integer>>> collected = streamMap()
                .map(Entry::getValue)
                .collect(groupingByToICache(
                        randomName(),
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
    public void twoLevelGrouping_whenSourceCache_toMap() throws Exception {
        int mod = 10;

        IMap<Integer, Map<Integer, List<Integer>>> collected = streamCache()
                .map(Entry::getValue)
                .collect(groupingByToIMap(
                        randomName(),
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
    public void twoLevelGrouping_whenSourceCache_toCache() throws Exception {
        int mod = 10;

        ICache<Integer, Map<Integer, List<Integer>>> collected = streamCache()
                .map(Entry::getValue)
                .collect(groupingByToICache(
                        randomName(),
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
        int mod = 10;

        IMap<Integer, List<Integer>> collected = streamList()
                .collect(groupingByToIMap(randomName(), m -> m % mod));

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
    public void wordCount_sourceMap_toMap() throws Exception {
        IStreamMap<String, String> map = getMap();
        String words = "0 1 2 3 4 5 6 7 8 9";
        for (int i = 0; i < COUNT; i++) {
            map.put("key-" + i, words);
        }

        IMap<String, Integer> collected = map.stream()
                                             .flatMap(m -> Stream.of(m.getValue().split("\\s")))
                                             .collect(toIMap(randomName(), v -> v, v -> 1, (l, r) -> l + r));

        assertEquals(10, collected.size());

        for (Integer count : collected.values()) {
            assertEquals(COUNT, (int) count);
        }
    }

    @Test
    public void wordCount_sourceMap_toCache() throws Exception {
        IStreamMap<String, String> map = getMap();
        String words = "0 1 2 3 4 5 6 7 8 9";
        for (int i = 0; i < COUNT; i++) {
            map.put("key-" + i, words);
        }

        ICache<String, Integer> collected = map.stream()
                                               .flatMap(m -> Stream.of(m.getValue().split("\\s")))
                                               .collect(toICache(randomName(),
                                                       v -> v, v -> 1, (l, r) -> l + r));

        assertEquals(10, collected.size());

        for (Cache.Entry<String, Integer> entry : collected) {
            assertEquals(COUNT, (int) entry.getValue());
        }
    }

    @Test
    public void wordCount_sourceCache_toMap() throws Exception {
        IStreamCache<String, String> cache = getCache();
        String words = "0 1 2 3 4 5 6 7 8 9";
        for (int i = 0; i < COUNT; i++) {
            cache.put("key-" + i, words);
        }

        IMap<String, Integer> collected = cache.stream()
                                               .flatMap(m -> Stream.of(m.getValue().split("\\s")))
                                               .collect(toIMap(randomName(), v -> v, v -> 1, (l, r) -> l + r));

        assertEquals(10, collected.size());

        for (Integer count : collected.values()) {
            assertEquals(COUNT, (int) count);
        }
    }

    @Test
    public void wordCount_sourceCache_toCache() throws Exception {
        IStreamCache<String, String> cache = getCache();
        String words = "0 1 2 3 4 5 6 7 8 9";
        for (int i = 0; i < COUNT; i++) {
            cache.put("key-" + i, words);
        }

        ICache<String, Integer> collected = cache.stream()
                                                 .flatMap(m -> Stream.of(m.getValue().split("\\s")))
                                                 .collect(toICache(randomName(),
                                                         v -> v, v -> 1, (l, r) -> l + r));

        assertEquals(10, collected.size());

        for (Cache.Entry<String, Integer> entry : collected) {
            assertEquals(COUNT, (int) entry.getValue());
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
                                              .collect(toIMap(randomName(), v -> v, v -> 1, (l, r) -> l + r));

        assertEquals(10, collected.size());

        for (Integer count : collected.values()) {
            assertEquals(COUNT, (int) count);
        }
    }

    @Test
    public void icacheCollectWithMerge_whenSourceList() throws Exception {
        IStreamList<String> list = getList();
        String words = "0 1 2 3 4 5 6 7 8 9";
        for (int i = 0; i < COUNT; i++) {
            list.add(words);
        }

        ICache<String, Integer> collected = list.stream()
                                                .flatMap(m -> Stream.of(m.split("\\s")))
                                                .collect(toICache(randomName(),
                                                        v -> v, v -> 1, (l, r) -> l + r));

        assertEquals(10, collected.size());

        for (Cache.Entry<String, Integer> entry : collected) {
            assertEquals(COUNT, (int) entry.getValue());
        }
    }

    @Test
    public void ilistCollect_whenNoIntermediaries() throws Exception {
        IStreamList<Integer> list = getList();
        fillList(list);

        IStreamList<Integer> collected = list.stream().collect(toIList(randomString()));

        assertArrayEquals(list.toArray(), collected.toArray());
    }

    @Test
    public void ilistCollect_whenSourceMap() throws Exception {
        IStreamMap<String, Integer> map = getMap();
        fillMap(map);

        IList<Entry<String, Integer>> collected = map.stream().collect(toIList(randomString()));

        Entry<String, Integer>[] expecteds = map.entrySet().toArray(new Entry[0]);
        Entry<String, Integer>[] actuals = collected.toArray(new Entry[0]);

        Comparator<Entry<String, Integer>> entryComparator = Comparator.comparing(Entry::getKey);
        Arrays.sort(expecteds, entryComparator);
        Arrays.sort(actuals, entryComparator);

        assertArrayEquals(expecteds, actuals);
    }

    @Test
    public void ilistCollect_whenSourceCache() throws Exception {
        IStreamCache<String, Integer> cache = getCache();
        fillCache(cache);

        IList<Entry<String, Integer>> collected = cache.stream().collect(toIList(randomString()));

        Cache.Entry<String, Integer>[] expecteds = new Cache.Entry[cache.size()];
        int count = 0;
        for (Cache.Entry<String, Integer> entry : cache) {
            expecteds[count++] = entry;
        }
        Map.Entry<String, Integer>[] actuals = collected.toArray(new Map.Entry[0]);

        Arrays.sort(expecteds, Comparator.comparing(Cache.Entry::getKey));
        Arrays.sort(actuals, Comparator.comparing(Map.Entry::getKey));

        assertEquals(expecteds.length, actuals.length);
        for (int i = 0; i < expecteds.length; i++) {
            assertEquals(expecteds[i].getKey(), actuals[i].getKey());
            assertEquals(expecteds[i].getValue(), actuals[i].getValue());
        }
    }
}
