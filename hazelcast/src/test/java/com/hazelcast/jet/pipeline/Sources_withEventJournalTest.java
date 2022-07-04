/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.pipeline;

import com.hazelcast.cache.EventJournalCacheEvent;
import com.hazelcast.cache.ICache;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.collection.IList;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.PredicateEx;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.map.EventJournalMapEvent;
import com.hazelcast.map.IMap;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Stream;

import static com.hazelcast.function.Functions.entryValue;
import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.Util.mapPutEvents;
import static com.hazelcast.jet.pipeline.JournalInitialPosition.START_FROM_OLDEST;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@Category(SlowTest.class)
public class Sources_withEventJournalTest extends PipelineTestSupport {
    private static HazelcastInstance remoteHz;
    private static ClientConfig clientConfig;

    @BeforeClass
    public static void setUp() {
        Config config = new Config();
        config.setClusterName(randomName());
        config.addCacheConfig(new CacheSimpleConfig().setName("*"));
        config.getMapConfig(JOURNALED_MAP_PREFIX + '*').getEventJournalConfig().setEnabled(true);
        config.getCacheConfig(JOURNALED_CACHE_PREFIX + '*').getEventJournalConfig().setEnabled(true);

        remoteHz = createRemoteCluster(config, 2).get(0);
        clientConfig = getClientConfigForRemoteCluster(remoteHz);
    }

    @AfterClass
    public static void afterClass() {
        HazelcastInstanceFactory.terminateAll();
    }

    @Test
    public void mapJournal_byName() {
        // Given
        String mapName = JOURNALED_MAP_PREFIX + randomName();
        IMap<String, Integer> map = hz().getMap(mapName);

        // When
        StreamSource<Entry<String, Integer>> source = Sources.mapJournal(mapName, START_FROM_OLDEST);

        // Then
        testMapJournal(map, source);
    }

    @Test
    public void mapJournal_byRef() {
        // Given
        String mapName = JOURNALED_MAP_PREFIX + randomName();
        IMap<String, Integer> map = hz().getMap(mapName);

        // When
        StreamSource<Entry<String, Integer>> source = Sources.mapJournal(map, START_FROM_OLDEST);

        // Then
        testMapJournal(map, source);
    }

    @Test
    public void remoteMapJournal() {
        // Given
        String mapName = JOURNALED_MAP_PREFIX + randomName();
        IMap<String, Integer> map = remoteHz.getMap(mapName);

        // When
        StreamSource<Entry<String, Integer>> source = Sources.remoteMapJournal(
                mapName, clientConfig, START_FROM_OLDEST);

        // Then
        testMapJournal(map, source);
    }

    private void testMapJournal(IMap<String, Integer> map, StreamSource<Entry<String, Integer>> source) {
        // Given a pre-populated source map...
        List<Integer> input = sequence(itemCount);
        int[] key = {0};
        input.forEach(i -> map.put(String.valueOf(key[0]++), Integer.MIN_VALUE + i));

        // When we start the job...
        p.readFrom(source)
         .withoutTimestamps()
         .map(entryValue())
         .writeTo(sink);
        hz().getJet().newJob(p);

        // Then eventually we get all the map values in the sink.
        assertSizeEventually(itemCount, sinkList);

        // When we update all the map items...
        key[0] = 0;
        input.forEach(i -> map.put(String.valueOf(key[0]++), i));

        // Then eventually we get all the updated values in the sink.
        assertSizeEventually(2 * itemCount, sinkList);

        // When we delete all map items...
        input.forEach(i -> map.remove(String.valueOf(key[0]++)));

        // Then we won't get any more events in the sink.
        assertTrueAllTheTime(() -> assertEquals(2 * itemCount, sinkList.size()), 2);

        // The values we got are exactly all the original values
        // and all the updated values.
        List<Integer> expected = Stream.concat(input.stream().map(i -> Integer.MIN_VALUE + i), input.stream())
                                       .collect(toList());
        assertEquals(toBag(expected), sinkToBag());
    }

    @Test
    public void mapJournalByName_withProjectionToNull_then_nullsSkipped() {
        // Given
        String mapName = JOURNALED_MAP_PREFIX + randomName();

        // When
        StreamSource<String> source = Sources.mapJournal(hz().getMap(mapName), START_FROM_OLDEST,
                (EventJournalMapEvent<Integer, Entry<Integer, String>> entry) -> entry.getNewValue().getValue(),
                mapPutEvents()
        );

        // Then
        testMapJournal_withProjectionToNull_then_nullsSkipped(mapName, source);
    }

    @Test
    public void mapJournalByRef_withProjectionToNull_then_nullsSkipped() {
        // Given
        String mapName = JOURNALED_MAP_PREFIX + randomName();

        // When
        StreamSource<String> source = Sources.mapJournal(mapName, START_FROM_OLDEST,
                (EventJournalMapEvent<Integer, Entry<Integer, String>> entry) -> entry.getNewValue().getValue(),
                mapPutEvents()
        );

        // Then
        testMapJournal_withProjectionToNull_then_nullsSkipped(mapName, source);
    }

    private void testMapJournal_withProjectionToNull_then_nullsSkipped(
            String mapName, StreamSource<String> source
    ) {
        // Given
        IMap<Integer, Entry<Integer, String>> sourceMap = hz().getMap(mapName);
        range(0, itemCount).forEach(i -> sourceMap.put(i, entry(i, i % 2 == 0 ? null : String.valueOf(i))));

        // When
        p.readFrom(source).withoutTimestamps().writeTo(sink);
        hz().getJet().newJob(p);

        // Then
        assertTrueEventually(() -> assertEquals(
                range(0, itemCount)
                        .filter(i -> i % 2 != 0)
                        .mapToObj(String::valueOf)
                        .sorted()
                        .collect(joining("\n")),
                hz().<String>getList(sinkName)
                        .stream()
                        .sorted()
                        .collect(joining("\n"))
        ));
    }

    @Test
    public void mapJournal_withDefaultFilter() {
        // Given
        String mapName = JOURNALED_MAP_PREFIX + randomName();
        IMap<Integer, Integer> sourceMap = hz().getMap(mapName);
        sourceMap.put(1, 1); // ADDED
        sourceMap.remove(1); // REMOVED - filtered out
        sourceMap.put(1, 2); // ADDED

        // When
        StreamSource<Entry<Integer, Integer>> source = Sources.mapJournal(mapName, START_FROM_OLDEST);

        // Then
        p.readFrom(source).withoutTimestamps().writeTo(sink);
        hz().getJet().newJob(p);
        IList<Entry<Integer, Integer>> sinkList = hz().getList(sinkName);
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
        // Given
        String cacheName = JOURNALED_CACHE_PREFIX + randomName();
        ICache<Integer, Integer> sourceMap = hz().getCacheManager().getCache(cacheName);
        sourceMap.put(1, 1); // ADDED
        sourceMap.remove(1); // REMOVED - filtered out
        sourceMap.put(1, 2); // ADDED

        // When
        StreamSource<Entry<Integer, Integer>> source = Sources.cacheJournal(cacheName, START_FROM_OLDEST);

        // Then
        p.readFrom(source).withoutTimestamps().writeTo(sink);
        hz().getJet().newJob(p);
        IList<Entry<Integer, Integer>> sinkList = hz().getList(sinkName);
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
        // Given
        String mapName = JOURNALED_MAP_PREFIX + randomName();
        IMap<String, Integer> map = hz().getMap(mapName);
        PredicateEx<EventJournalMapEvent<String, Integer>> p = e -> e.getNewValue() % 2 == 0;

        // When
        StreamSource<Integer> source = Sources.mapJournal(
                mapName, START_FROM_OLDEST, EventJournalMapEvent::getNewValue, p);

        // Then
        testMapJournal_withPredicateAndProjection(map, source);
    }

    @Test
    public void remoteMapJournal_withPredicateAndProjectionFn() {
        // Given
        String mapName = JOURNALED_MAP_PREFIX + randomName();
        IMap<String, Integer> map = remoteHz.getMap(mapName);
        PredicateEx<EventJournalMapEvent<String, Integer>> p = e -> e.getNewValue() % 2 == 0;

        // When
        StreamSource<Integer> source = Sources.remoteMapJournal(
                mapName, clientConfig, START_FROM_OLDEST, EventJournalMapEvent::getNewValue, p);

        // Then
        testMapJournal_withPredicateAndProjection(map, source);
    }

    private void testMapJournal_withPredicateAndProjection(IMap<String, Integer> srcMap, StreamSource<Integer> source) {
        // Given a pre-populated source map...
        List<Integer> input = sequence(itemCount);
        int[] key = {0};
        input.forEach(i -> srcMap.put(String.valueOf(key[0]++), Integer.MIN_VALUE + i));

        // When we start the job...
        p.readFrom(source).withoutTimestamps().writeTo(sink);
        hz().getJet().newJob(p);

        // Then eventually we get all the map values in the sink.
        assertSizeEventually(itemCount / 2, sinkList);

        // When we update all the map items...
        key[0] = 0;
        input.forEach(i -> srcMap.put(String.valueOf(key[0]++), i));

        // Then eventually we get all the updated values in the sink.
        assertSizeEventually(itemCount, sinkList);

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
    public void remoteMapJournal_withUnknownValueClass() throws Exception {
        // Given
        URL jarResource = Thread.currentThread().getContextClassLoader()
                                .getResource("deployment/sample-pojo-1.0-car.jar");
        assertNotNull("jar not found", jarResource);
        ClassLoader cl = new URLClassLoader(new URL[]{jarResource});
        Class<?> carClz = cl.loadClass("com.sample.pojo.car.Car");
        Object car = carClz.getConstructor(String.class, String.class)
                                 .newInstance("make", "model");
        IMap<String, Object> map = remoteHz.getMap(srcName);
        // the class of the value is unknown to the remote IMDG member, it will be only known to Jet
        map.put("key", car);

        // When
        StreamSource<Entry<Object, Object>> source = Sources.remoteMapJournal(srcName, clientConfig, START_FROM_OLDEST);

        // Then
        p.readFrom(source).withoutTimestamps().map(en -> en.getValue().toString()).writeTo(sink);
        JobConfig jobConfig = new JobConfig();
        jobConfig.addJar(jarResource);
        Job job = hz().getJet().newJob(p, jobConfig);
        List<Object> expected = singletonList(car.toString());
        assertTrueEventually(() -> assertEquals(expected, new ArrayList<>(sinkList)), 10);
        job.cancel();
    }

    @Test
    public void cacheJournal_byName() {
        // Given
        String cacheName = JOURNALED_CACHE_PREFIX + randomName();
        ICache<String, Integer> cache = hz().getCacheManager().getCache(cacheName);

        // When
        StreamSource<Entry<String, Integer>> source = Sources.cacheJournal(cacheName, START_FROM_OLDEST);

        // Then
        testCacheJournal(cache, source);
    }

    @Test
    public void remoteCacheJournal() {
        // Given
        String cacheName = JOURNALED_CACHE_PREFIX + randomName();
        ICache<String, Integer> cache = remoteHz.getCacheManager().getCache(cacheName);

        // When
        StreamSource<Entry<String, Integer>> source =
                Sources.remoteCacheJournal(cacheName, clientConfig, START_FROM_OLDEST);

        // Then
        testCacheJournal(cache, source);
    }

    private void testCacheJournal(ICache<String, Integer> cache, StreamSource<Entry<String, Integer>> source) {
        // Given a pre-populated source cache...
        List<Integer> input = sequence(itemCount);
        int[] key = {0};
        input.forEach(i -> cache.put(String.valueOf(key[0]++), Integer.MIN_VALUE + i));

        // When we start the job...
        p.readFrom(source)
         .withoutTimestamps()
         .map(entryValue())
         .writeTo(sink);
        hz().getJet().newJob(p);

        // Then eventually we get all the cache values in the sink.
        assertSizeEventually(itemCount, sinkList);

        // When we update all the cache items...
        key[0] = 0;
        input.forEach(i -> cache.put(String.valueOf(key[0]++), i));

        // Then eventually we get all the updated values in the sink.
        assertSizeEventually(2 * itemCount, sinkList);

        // When we delete all cache items...
        input.forEach(i -> cache.remove(String.valueOf(key[0]++)));

        // Then we won't get any more events in the sink.
        assertTrueAllTheTime(() -> assertEquals(2 * itemCount, sinkList.size()), 2);

        // The values we got are exactly all the original values
        // and all the updated values.
        List<Integer> expected = Stream.concat(input.stream().map(i -> Integer.MIN_VALUE + i), input.stream())
                                       .collect(toList());
        assertEquals(toBag(expected), sinkToBag());
    }

    @Test
    public void cacheJournalByName_withPredicateAndProjectionFn() {
        // Given
        String cacheName = JOURNALED_CACHE_PREFIX + randomName();
        PredicateEx<EventJournalCacheEvent<String, Integer>> p = e -> e.getNewValue() % 2 == 0;
        ICache<String, Integer> cache = hz().getCacheManager().getCache(cacheName);

        // When
        StreamSource<Integer> source = Sources.cacheJournal(
                cacheName, START_FROM_OLDEST, EventJournalCacheEvent::getNewValue, p);

        // Then
        testCacheJournal_withPredicateAndProjection(cache, source);
    }

    @Test
    public void remoteCacheJournal_withPredicateAndProjectionFn() {
        // Given
        String cacheName = JOURNALED_CACHE_PREFIX + randomName();
        PredicateEx<EventJournalCacheEvent<String, Integer>> p = e -> e.getNewValue() % 2 == 0;
        ICache<String, Integer> cache = remoteHz.getCacheManager().getCache(cacheName);

        // When
        StreamSource<Integer> source = Sources.remoteCacheJournal(
                cacheName, clientConfig, START_FROM_OLDEST, EventJournalCacheEvent::getNewValue, p);

        // Then
        testCacheJournal_withPredicateAndProjection(cache, source);
    }

    private void testCacheJournal_withPredicateAndProjection(
            ICache<String, Integer> srcCache, StreamSource<Integer> source
    ) {
        // Given a pre-populated source map...
        List<Integer> input = sequence(itemCount);
        int[] key = {0};
        input.forEach(i -> srcCache.put(String.valueOf(key[0]++), Integer.MIN_VALUE + i));

        // When we start the job...
        p.readFrom(source)
         .withoutTimestamps()
         .writeTo(sink);
        hz().getJet().newJob(p);

        // Then eventually we get all the map values in the sink.
        assertSizeEventually(itemCount / 2, sinkList);

        // When we update all the map items...
        key[0] = 0;
        input.forEach(i -> srcCache.put(String.valueOf(key[0]++), i));

        // Then eventually we get all the updated values in the sink.
        assertSizeEventually(itemCount, sinkList);

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
    public void remoteCacheJournal_withUnknownValueClass() throws Exception {
        // Given
        URL jarResource = Thread.currentThread().getContextClassLoader()
                                .getResource("deployment/sample-pojo-1.0-car.jar");
        assertNotNull("jar not found", jarResource);
        ClassLoader cl = new URLClassLoader(new URL[]{jarResource});
        Class<?> carClz = cl.loadClass("com.sample.pojo.car.Car");
        Object car = carClz.getConstructor(String.class, String.class)
                                 .newInstance("make", "model");
        String cacheName = JOURNALED_CACHE_PREFIX + randomName();
        ICache<String, Object> cache = remoteHz.getCacheManager().getCache(cacheName);
        // the class of the value is unknown to the remote IMDG member, it will be only known to Jet
        cache.put("key", car);

        // When
        StreamSource<Entry<Object, Object>> source =
                Sources.remoteCacheJournal(cacheName, clientConfig, START_FROM_OLDEST);

        // Then
        p.readFrom(source).withoutTimestamps().map(en -> en.getValue().toString()).writeTo(sink);
        JobConfig jobConfig = new JobConfig();
        jobConfig.addJar(jarResource);
        Job job = hz().getJet().newJob(p, jobConfig);
        List<Object> expected = singletonList(car.toString());
        assertTrueEventually(() -> assertEquals(expected, new ArrayList<>(sinkList)), 10);
        job.cancel();
    }
}
