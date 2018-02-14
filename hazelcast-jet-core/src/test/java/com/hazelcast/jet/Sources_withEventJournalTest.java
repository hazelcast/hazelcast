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

package com.hazelcast.jet;

import com.hazelcast.cache.ICache;
import com.hazelcast.cache.journal.EventJournalCacheEvent;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.jet.function.DistributedPredicate;
import com.hazelcast.jet.stream.IStreamList;
import com.hazelcast.jet.stream.IStreamMap;
import com.hazelcast.map.journal.EventJournalMapEvent;
import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Stream;
import javax.cache.Cache;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.hazelcast.jet.JournalInitialPosition.START_FROM_OLDEST;
import static com.hazelcast.jet.Util.mapPutEvents;
import static com.hazelcast.jet.core.WatermarkEmissionPolicy.suppressDuplicates;
import static com.hazelcast.jet.core.WatermarkGenerationParams.noWatermarks;
import static com.hazelcast.jet.core.WatermarkGenerationParams.wmGenParams;
import static com.hazelcast.jet.core.WatermarkPolicies.withFixedLag;
import static com.hazelcast.jet.function.DistributedFunctions.entryValue;
import static com.hazelcast.query.impl.predicates.PredicateTestUtils.entry;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;
import static org.junit.Assert.assertEquals;

public class Sources_withEventJournalTest extends PipelineTestSupport {
    private static HazelcastInstance remoteHz;
    private static ClientConfig clientConfig;

    @BeforeClass
    public static void setUp() {
        Config config = new Config();
        config.addCacheConfig(new CacheSimpleConfig().setName("*"));
        config.getMapEventJournalConfig(JOURNALED_MAP_PREFIX + '*').setEnabled(true);
        config.getCacheEventJournalConfig(JOURNALED_CACHE_PREFIX + '*').setEnabled(true);

        remoteHz = createRemoteCluster(config, 2).get(0);
        clientConfig = getClientConfigForRemoteCluster(remoteHz);
    }

    @AfterClass
    public static void after() {
        Hazelcast.shutdownAll();
    }

    @Test
    public void mapJournal() {
        String mapName = JOURNALED_MAP_PREFIX + randomName();
        IStreamMap<String, Integer> map = jet().getMap(mapName);
        Source<Entry<String, Integer>> source = Sources.mapJournal(mapName, START_FROM_OLDEST,
                wmGenParams(Entry::getValue, withFixedLag(0), suppressDuplicates(), 10_000));
        testMapJournal(map, source);
    }

    @Test
    public void remoteMapJournal() {
        String mapName = JOURNALED_MAP_PREFIX + randomName();
        IMap<String, Integer> map = remoteHz.getMap(mapName);
        Source<Entry<String, Integer>> source = Sources.remoteMapJournal(mapName, clientConfig, START_FROM_OLDEST,
                wmGenParams(Entry::getValue, withFixedLag(0), suppressDuplicates(), 10_000));
        testMapJournal(map, source);
    }

    public void testMapJournal(IMap<String, Integer> map, Source<Entry<String, Integer>> source) {
        // Given a pre-populated source map...
        List<Integer> input = sequence(ITEM_COUNT);
        int[] key = {0};
        input.forEach(i -> map.put(String.valueOf(key[0]++), Integer.MIN_VALUE + i));

        // When we start the job...
        pipeline.drawFrom(source)
                .map(entryValue())
                .drainTo(sink);
        jet().newJob(pipeline);

        // Then eventually we get all the map values in the sink.
        assertSizeEventually(ITEM_COUNT, sinkList);

        // When we update all the map items...
        key[0] = 0;
        input.forEach(i -> map.put(String.valueOf(key[0]++), i));

        // Then eventually we get all the updated values in the sink.
        assertSizeEventually(2 * ITEM_COUNT, sinkList);

        // When we delete all map items...
        input.forEach(i -> map.remove(String.valueOf(key[0]++)));

        // Then we won't get any more events in the sink.
        assertTrueAllTheTime(() -> assertEquals(2 * ITEM_COUNT, sinkList.size()), 2);

        // The values we got are exactly all the original values
        // and all the updated values.
        List<Integer> expected = Stream.concat(input.stream().map(i -> Integer.MIN_VALUE + i), input.stream())
                                       .collect(toList());
        assertEquals(toBag(expected), sinkToBag());
    }

    @Test
    public void mapJournal_withProjectionToNull_then_nullsSkipped() {
        // given
        String mapName = JOURNALED_MAP_PREFIX + randomName();
        Source<String> source = Sources.mapJournal(mapName, mapPutEvents(),
                (EventJournalMapEvent<Integer, Entry<Integer, String>> entry) -> entry.getNewValue().getValue(),
                START_FROM_OLDEST,
                noWatermarks()
        );
        IStreamMap<Integer, Entry<Integer, String>> sourceMap = jet().getMap(mapName);
        range(0, ITEM_COUNT).forEach(i -> sourceMap.put(i, entry(i, i % 2 == 0 ? null : String.valueOf(i))));

        // when
        pipeline.drawFrom(source)
                .drainTo(sink);
        jet().newJob(pipeline);

        // then
        assertTrueEventually(() -> assertEquals(
                range(0, ITEM_COUNT)
                        .filter(i -> i % 2 != 0)
                        .mapToObj(String::valueOf)
                        .sorted()
                        .collect(joining("\n")),
                jet().getHazelcastInstance().<String>getList(sinkName)
                        .stream()
                        .sorted()
                        .collect(joining("\n"))
        ));
    }

    @Test
    public void mapJournal_withDefaultFilter() {
        // given
        String mapName = JOURNALED_MAP_PREFIX + randomName();
        Source<Entry<Integer, Integer>> source = Sources.mapJournal(mapName,
                START_FROM_OLDEST,
                noWatermarks()
        );

        IStreamMap<Integer, Integer> sourceMap = jet().getMap(mapName);
        sourceMap.put(1, 1); // ADDED
        sourceMap.remove(1); // REMOVED - filtered out
        sourceMap.put(1, 2); // ADDED


        // when
        pipeline.drawFrom(source)
                .drainTo(sink);
        jet().newJob(pipeline);

        // then
        IStreamList<Entry<Integer, Integer>> sinkList = jet().getList(sinkName);
        assertTrueEventually(() -> {
                    assertEquals(2, sinkList.size());

                    Entry<Integer, Integer> e = sinkList.get(0);
                    assertEquals(Integer.valueOf(1), e.getKey());
                    assertEquals(Integer.valueOf(1), e.getValue());

                    e = sinkList.get(1);
                    assertEquals(Integer.valueOf(1), e.getKey());
                    assertEquals(Integer.valueOf(2), e.getValue());
                }
        );
    }


    @Test
    public void cacheJournal_withDefaultFilter() {
        // given
        String cacheName = JOURNALED_CACHE_PREFIX + randomName();
        Source<Entry<Integer, Integer>> source = Sources.cacheJournal(cacheName,
                START_FROM_OLDEST,
                noWatermarks()
        );
        ICache<Integer, Integer> sourceMap = jet().getCacheManager().getCache(cacheName);
        sourceMap.put(1, 1); // ADDED
        sourceMap.remove(1); // REMOVED - filtered out
        sourceMap.put(1, 2); // ADDED

        // when
        pipeline.drawFrom(source)
                .drainTo(sink);
        jet().newJob(pipeline);

        // then
        IStreamList<Entry<Integer, Integer>> sinkList = jet().getList(sinkName);
        assertTrueEventually(() -> {
                    assertEquals(2, sinkList.size());

                    Entry<Integer, Integer> e = sinkList.get(0);
                    assertEquals(Integer.valueOf(1), e.getKey());
                    assertEquals(Integer.valueOf(1), e.getValue());

                    e = sinkList.get(1);
                    assertEquals(Integer.valueOf(1), e.getKey());
                    assertEquals(Integer.valueOf(2), e.getValue());
                }
        );
    }

    @Test
    public void mapJournal_withPredicateAndProjection() {
        String mapName = JOURNALED_MAP_PREFIX + randomName();
        IStreamMap<String, Integer> map = jet().getMap(mapName);
        DistributedPredicate<EventJournalMapEvent<String, Integer>> p = e -> e.getNewValue() % 2 == 0;
        Source<Integer> source = Sources.mapJournal(mapName, p,
                EventJournalMapEvent::getNewValue, START_FROM_OLDEST,
                wmGenParams(v -> v, withFixedLag(0), suppressDuplicates(), 10_000));
        testMapJournal_withPredicateAndProjection(map, source);

    }

    @Test
    public void remoteMapJournal_withPredicateAndProjectionFn() {
        String mapName = JOURNALED_MAP_PREFIX + randomName();
        IMap<String, Integer> map = remoteHz.getMap(mapName);
        DistributedPredicate<EventJournalMapEvent<String, Integer>> p = e -> e.getNewValue() % 2 == 0;
        Source<Integer> source = Sources.remoteMapJournal(mapName, clientConfig, p,
                EventJournalMapEvent::getNewValue, START_FROM_OLDEST,
                wmGenParams(v -> v, withFixedLag(0), suppressDuplicates(), 10_000));
        testMapJournal_withPredicateAndProjection(map, source);
    }

    public void testMapJournal_withPredicateAndProjection(IMap<String, Integer> srcMap, Source<Integer> source) {
        // Given a pre-populated source map...
        List<Integer> input = sequence(ITEM_COUNT);
        int[] key = {0};
        input.forEach(i -> srcMap.put(String.valueOf(key[0]++), Integer.MIN_VALUE + i));

        // When we start the job...
        pipeline.drawFrom(source)
                .drainTo(sink);
        jet().newJob(pipeline);

        // Then eventually we get all the map values in the sink.
        assertSizeEventually(ITEM_COUNT / 2, sinkList);

        // When we update all the map items...
        key[0] = 0;
        input.forEach(i -> srcMap.put(String.valueOf(key[0]++), i));

        // Then eventually we get all the updated values in the sink.
        assertSizeEventually(ITEM_COUNT, sinkList);

        // The values we got are exactly all the original values
        // and all the updated values.
        List<Integer> expected = Stream
                .concat(input.stream().map(i -> Integer.MIN_VALUE + i),
                        input.stream())
                .filter(i -> i % 2 == 0)
                .collect(toList());
        assertEquals(toBag(expected), sinkToBag());
    }


    @Test
    public void cacheJournal() {
        String cacheName = JOURNALED_CACHE_PREFIX + randomName();
        Cache<String, Integer> cache = jet().getCacheManager().getCache(cacheName);
        Source<Entry<String, Integer>> source = Sources.cacheJournal(cacheName, START_FROM_OLDEST,
                wmGenParams(Entry::getValue, withFixedLag(0), suppressDuplicates(), 10_000));
        testCacheJournal(cache, source);
    }

    @Test
    public void remoteCacheJournal() {
        String cacheName = JOURNALED_CACHE_PREFIX + randomName();
        Cache<String, Integer> cache = remoteHz.getCacheManager().getCache(cacheName);
        Source<Entry<String, Integer>> source = Sources.remoteCacheJournal(cacheName, clientConfig, START_FROM_OLDEST,
                wmGenParams(Entry::getValue, withFixedLag(0), suppressDuplicates(), 10_000));
        testCacheJournal(cache, source);
    }


    public void testCacheJournal(Cache<String, Integer> cache, Source<Entry<String, Integer>> source) {
        // Given a pre-populated source cache...
        List<Integer> input = sequence(ITEM_COUNT);
        int[] key = {0};
        input.forEach(i -> cache.put(String.valueOf(key[0]++), Integer.MIN_VALUE + i));

        // When we start the job...
        pipeline.drawFrom(source)
                .map(entryValue())
                .drainTo(sink);
        jet().newJob(pipeline);

        // Then eventually we get all the cache values in the sink.
        assertSizeEventually(ITEM_COUNT, sinkList);

        // When we update all the cache items...
        key[0] = 0;
        input.forEach(i -> cache.put(String.valueOf(key[0]++), i));

        // Then eventually we get all the updated values in the sink.
        assertSizeEventually(2 * ITEM_COUNT, sinkList);

        // When we delete all cache items...
        input.forEach(i -> cache.remove(String.valueOf(key[0]++)));

        // Then we won't get any more events in the sink.
        assertTrueAllTheTime(() -> assertEquals(2 * ITEM_COUNT, sinkList.size()), 2);

        // The values we got are exactly all the original values
        // and all the updated values.
        List<Integer> expected = Stream.concat(input.stream().map(i -> Integer.MIN_VALUE + i), input.stream())
                                       .collect(toList());
        assertEquals(toBag(expected), sinkToBag());
    }


    @Test
    public void cacheJournal_withPredicateAndProjectionFn() {
        String cacheName = JOURNALED_CACHE_PREFIX + randomName();
        DistributedPredicate<EventJournalCacheEvent<String, Integer>> p = e -> e.getNewValue() % 2 == 0;
        Cache<String, Integer> cache = jet().getCacheManager().getCache(cacheName);
        Source<Integer> source = Sources.cacheJournal(cacheName, p,
                EventJournalCacheEvent::getNewValue, START_FROM_OLDEST,
                wmGenParams(v -> v, withFixedLag(0), suppressDuplicates(), 10_000));
        testCacheJournal_withPredicateAndProjection(cache, source);
    }


    @Test
    public void remoteCacheJournal_withPredicateAndProjectionFn() {
        String cacheName = JOURNALED_CACHE_PREFIX + randomName();
        DistributedPredicate<EventJournalCacheEvent<String, Integer>> p = e -> e.getNewValue() % 2 == 0;
        Cache<String, Integer> cache = remoteHz.getCacheManager().getCache(cacheName);
        Source<Integer> source = Sources.remoteCacheJournal(cacheName, clientConfig, p,
                EventJournalCacheEvent::getNewValue, START_FROM_OLDEST,
                wmGenParams(v -> v, withFixedLag(0), suppressDuplicates(), 10_000));
        testCacheJournal_withPredicateAndProjection(cache, source);
    }

    public void testCacheJournal_withPredicateAndProjection(Cache<String, Integer> srcCache, Source<Integer> source) {
        // Given a pre-populated source map...
        List<Integer> input = sequence(ITEM_COUNT);
        int[] key = {0};
        input.forEach(i -> srcCache.put(String.valueOf(key[0]++), Integer.MIN_VALUE + i));

        // When we start the job...
        pipeline.drawFrom(source)
                .drainTo(sink);
        jet().newJob(pipeline);

        // Then eventually we get all the map values in the sink.
        assertSizeEventually(ITEM_COUNT / 2, sinkList);

        // When we update all the map items...
        key[0] = 0;
        input.forEach(i -> srcCache.put(String.valueOf(key[0]++), i));

        // Then eventually we get all the updated values in the sink.
        assertSizeEventually(ITEM_COUNT, sinkList);

        // The values we got are exactly all the original values
        // and all the updated values.
        List<Integer> expected = Stream
                .concat(input.stream().map(i -> Integer.MIN_VALUE + i),
                        input.stream())
                .filter(i -> i % 2 == 0)
                .collect(toList());
        assertEquals(toBag(expected), sinkToBag());
    }

}
