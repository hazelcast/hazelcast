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

package com.hazelcast.jet.impl.connector;

import com.hazelcast.cache.EventJournalCacheEvent;
import com.hazelcast.cache.ICache;
import com.hazelcast.collection.IList;
import com.hazelcast.config.Config;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.ICacheManager;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.TestProcessors;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.processor.SinkProcessors;
import com.hazelcast.jet.core.processor.SourceProcessors;
import com.hazelcast.map.EventJournalMapEvent;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.proxy.NearCachedMapProxyImpl;
import com.hazelcast.projection.Projections;
import com.hazelcast.query.Predicates;
import com.hazelcast.query.impl.predicates.TruePredicate;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.IntStream;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.Util.mapPutEvents;
import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.EventTimePolicy.eventTimePolicy;
import static com.hazelcast.jet.core.EventTimePolicy.noEventTime;
import static com.hazelcast.jet.core.WatermarkPolicy.limitingLag;
import static com.hazelcast.jet.core.processor.SinkProcessors.writeCacheP;
import static com.hazelcast.jet.core.processor.SinkProcessors.writeListP;
import static com.hazelcast.jet.core.processor.SinkProcessors.writeMapP;
import static com.hazelcast.jet.core.processor.SourceProcessors.readCacheP;
import static com.hazelcast.jet.core.processor.SourceProcessors.readListP;
import static com.hazelcast.jet.core.processor.SourceProcessors.readMapP;
import static com.hazelcast.jet.core.processor.SourceProcessors.streamCacheP;
import static com.hazelcast.jet.core.processor.SourceProcessors.streamMapP;
import static com.hazelcast.jet.pipeline.JournalInitialPosition.START_FROM_OLDEST;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Category({QuickTest.class, ParallelJVMTest.class})
public class HazelcastConnectorTest extends SimpleTestInClusterSupport {

    private static final int ENTRY_COUNT = 100;

    private String sourceName;
    private String sinkName;

    private String streamSourceName;
    private String streamSinkName;

    @BeforeClass
    public static void beforeClass() {
        Config config = smallInstanceConfig();
        config.getCacheConfig("*").getEventJournalConfig().setEnabled(true);
        config.getMapConfig("stream*").getEventJournalConfig().setEnabled(true);
        config.getMapConfig("nearCache*").setNearCacheConfig(new NearCacheConfig());

        initialize(2, config);
    }

    @Before
    public void before() {
        sourceName = randomString();
        sinkName = randomString();

        streamSourceName = "stream" + sourceName;
        streamSinkName = "stream" + sinkName;

        // workaround for `cache is not created` exception, create cache locally on all nodes
        ICacheManager cacheManager = instances()[1].getCacheManager();
        cacheManager.getCache(sourceName);
        cacheManager.getCache(sinkName);
        cacheManager.getCache(streamSourceName);
        cacheManager.getCache(streamSinkName);
    }

    @Test
    public void when_readMap_and_writeMap() {
        IMap<Integer, Integer> sourceMap = instance().getMap(sourceName);
        range(0, ENTRY_COUNT).forEach(i -> sourceMap.put(i, i));

        DAG dag = new DAG();
        Vertex source = dag.newVertex("source", readMapP(sourceName));
        Vertex sink = dag.newVertex("sink", writeMapP(sinkName));

        dag.edge(between(source, sink));

        instance().getJet().newJob(dag).join();

        assertEquals(ENTRY_COUNT, instance().getMap(sinkName).size());
    }

    @Test
    public void test_writeMapWithNearCache() {
        List<Integer> items = range(0, ENTRY_COUNT).boxed().collect(toList());
        sinkName = "nearCache-" + randomName();

        DAG dag = new DAG();
        Vertex src = dag.newVertex("src", () -> new TestProcessors.ListSource(items))
                .localParallelism(1);
        Vertex sink = dag.newVertex("sink", SinkProcessors.writeMapP(sinkName, i -> i, i -> i));
        dag.edge(between(src, sink));

        instance().getJet().newJob(dag).join();

        IMap<Object, Object> sinkMap = instance().getMap(sinkName);
        assertInstanceOf(NearCachedMapProxyImpl.class, sinkMap);
        assertEquals(ENTRY_COUNT, sinkMap.size());
    }

    @Test
    public void when_readMap_withNativePredicateAndProjection() {
        IMap<Integer, Integer> sourceMap = instance().getMap(sourceName);
        range(0, ENTRY_COUNT).forEach(i -> sourceMap.put(i, i));

        DAG dag = new DAG();
        Vertex source = dag.newVertex("source",
                readMapP(sourceName,
                        Predicates.greaterThan("this", "0"),
                        Projections.singleAttribute("value")
                )
        );
        Vertex sink = dag.newVertex("sink", writeListP(sinkName));
        dag.edge(between(source, sink));

        instance().getJet().newJob(dag).join();

        IList<Object> list = instance().getList(sinkName);
        assertEquals(ENTRY_COUNT - 1, list.size());
        for (int i = 0; i < ENTRY_COUNT; i++) {
            assertEquals(i != 0, list.contains(i));
        }
    }

    @Test
    public void when_readMap_withProjectionToNull_then_nullsSkipped() {
        IMap<Integer, Entry<Integer, String>> sourceMap = instance().getMap(sourceName);
        range(0, ENTRY_COUNT).forEach(i -> sourceMap.put(i, entry(i, i % 2 == 0 ? null : String.valueOf(i))));

        DAG dag = new DAG();
        Vertex source = dag.newVertex("source", readMapP(sourceName,
                        new TruePredicate<>(),
                        Projections.singleAttribute("value")
                ));
        Vertex sink = dag.newVertex("sink", writeListP(sinkName));
        dag.edge(between(source, sink));

        instance().getJet().newJob(dag).join();

        checkContents_projectedToNull(sinkName);
    }

    public void checkContents_projectedToNull(String sinkName) {
        assertEquals(
                IntStream.range(0, ENTRY_COUNT)
                         .filter(i -> i % 2 != 0)
                         .mapToObj(String::valueOf)
                         .sorted()
                         .collect(joining("\n")),
                instance().<String>getList(sinkName).stream()
                        .sorted()
                        .collect(joining("\n")));
    }

    @Test
    public void when_readMap_withPredicateAndFunction() {
        IMap<Integer, Integer> sourceMap = instance().getMap(sourceName);
        range(0, ENTRY_COUNT).forEach(i -> sourceMap.put(i, i));

        DAG dag = new DAG();
        Vertex source = dag.newVertex("source", readMapP(sourceName, e -> !e.getKey().equals(0), Map.Entry::getKey));
        Vertex sink = dag.newVertex("sink", writeListP(sinkName));

        dag.edge(between(source, sink));

        instance().getJet().newJob(dag).join();

        IList<Object> list = instance().getList(sinkName);
        assertEquals(ENTRY_COUNT - 1, list.size());
        assertFalse(list.contains(0));
        assertTrue(list.contains(1));
    }

    @Test
    public void when_streamMap() {
        DAG dag = new DAG();
        Vertex source = dag.newVertex("source", streamMapP(streamSourceName, START_FROM_OLDEST,
                eventTimePolicy(Entry<Integer, Integer>::getValue, limitingLag(0), 1, 0, 10_000)));
        Vertex sink = dag.newVertex("sink", writeListP(streamSinkName));

        dag.edge(between(source, sink));

        Job job = instance().getJet().newJob(dag);

        IMap<Integer, Integer> sourceMap = instance().getMap(streamSourceName);
        range(0, ENTRY_COUNT).forEach(i -> sourceMap.put(i, i));

        assertSizeEventually(ENTRY_COUNT, instance().getList(streamSinkName));
        job.cancel();
    }

    @Test
    public void when_streamMap_withProjectionToNull_then_nullsSkipped() {
        DAG dag = new DAG();
        Vertex source = dag.newVertex("source", SourceProcessors.streamMapP(streamSourceName,
                mapPutEvents(),
                (EventJournalMapEvent<Integer, Entry<Integer, String>> entry) -> entry.getNewValue().getValue(),
                START_FROM_OLDEST, noEventTime()));
        Vertex sink = dag.newVertex("sink", writeListP(streamSinkName));

        dag.edge(between(source, sink));

        Job job = instance().getJet().newJob(dag);

        IMap<Integer, Entry<Integer, String>> sourceMap = instance().getMap(streamSourceName);
        range(0, ENTRY_COUNT).forEach(i -> sourceMap.put(i, entry(i, i % 2 == 0 ? null : String.valueOf(i))));

        assertTrueEventually(() -> checkContents_projectedToNull(streamSinkName));
        job.cancel();
    }

    @Test
    public void when_streamMap_withFilterAndProjection() {
        DAG dag = new DAG();
        Vertex source = dag.newVertex("source", SourceProcessors.<Integer, Integer, Integer>streamMapP(streamSourceName,
                event -> event.getKey() != 0, EventJournalMapEvent::getKey, START_FROM_OLDEST,
                eventTimePolicy(i -> i, limitingLag(0), 1, 0, 10_000)));
        Vertex sink = dag.newVertex("sink", writeListP(streamSinkName));

        dag.edge(between(source, sink));

        Job job = instance().getJet().newJob(dag);

        IMap<Integer, Integer> sourceMap = instance().getMap(streamSourceName);
        range(0, ENTRY_COUNT).forEach(i -> sourceMap.put(i, i));

        assertSizeEventually(ENTRY_COUNT - 1, instance().getList(streamSinkName));
        assertFalse(instance().getList(streamSinkName).contains(0));
        assertTrue(instance().getList(streamSinkName).contains(1));
        job.cancel();
    }

    @Test
    public void when_readCache_and_writeCache() {
        ICache<Integer, Integer> sourceCache = instance().getCacheManager().getCache(sourceName);
        range(0, ENTRY_COUNT).forEach(i -> sourceCache.put(i, i));

        DAG dag = new DAG();
        Vertex source = dag.newVertex("source", readCacheP(sourceName));
        Vertex sink = dag.newVertex("sink", writeCacheP(sinkName));

        dag.edge(between(source, sink));

        instance().getJet().newJob(dag).join();

        assertEquals(ENTRY_COUNT, instance().getCacheManager().getCache(sinkName).size());
    }

    @Test
    public void when_streamCache() {
        DAG dag = new DAG();
        Vertex source = dag.newVertex("source", streamCacheP(streamSourceName, START_FROM_OLDEST,
                eventTimePolicy(Entry<Integer, Integer>::getValue, limitingLag(0), 1, 0, 10_000)));
        Vertex sink = dag.newVertex("sink", writeListP(streamSinkName));

        dag.edge(between(source, sink));

        Job job = instance().getJet().newJob(dag);

        ICache<Integer, Integer> sourceCache = instance().getCacheManager().getCache(streamSourceName);
        range(0, ENTRY_COUNT).forEach(i -> sourceCache.put(i, i));

        assertSizeEventually(ENTRY_COUNT, instance().getList(streamSinkName));
        job.cancel();
    }

    @Test
    public void when_streamCache_withFilterAndProjection() {
        DAG dag = new DAG();
        Vertex source = dag.newVertex("source", SourceProcessors.<Integer, Integer, Integer>streamCacheP(streamSourceName,
                event -> !event.getKey().equals(0), EventJournalCacheEvent::getKey, START_FROM_OLDEST,
                eventTimePolicy(i -> i, limitingLag(0), 1, 0, 10_000)));
        Vertex sink = dag.newVertex("sink", writeListP(streamSinkName));

        dag.edge(between(source, sink));

        Job job = instance().getJet().newJob(dag);

        ICache<Integer, Integer> sourceCache = instance().getCacheManager().getCache(streamSourceName);
        range(0, ENTRY_COUNT).forEach(i -> sourceCache.put(i, i));

        assertSizeEventually(ENTRY_COUNT - 1, instance().getList(streamSinkName));
        assertFalse(instance().getList(streamSinkName).contains(0));
        assertTrue(instance().getList(streamSinkName).contains(1));
        job.cancel();
    }

    @Test
    public void when_readList_and_writeList() {
        IList<Integer> list = instance().getList(sourceName);
        list.addAll(range(0, ENTRY_COUNT).boxed().collect(toList()));

        DAG dag = new DAG();
        Vertex source = dag.newVertex("source", readListP(sourceName)).localParallelism(1);
        Vertex sink = dag.newVertex("sink", writeListP(sinkName)).localParallelism(1);

        dag.edge(between(source, sink));

        instance().getJet().newJob(dag).join();

        assertEquals(ENTRY_COUNT, instance().getList(sinkName).size());
    }

    @Test
    public void test_defaultFilter_mapJournal() {
        DAG dag = new DAG();
        Vertex source = dag.newVertex("source", streamMapP(streamSourceName, START_FROM_OLDEST,
                eventTimePolicy(Entry<Integer, Integer>::getValue, limitingLag(0), 1, 0, 10_000)));
        Vertex sink = dag.newVertex("sink", writeListP(streamSinkName));

        dag.edge(between(source, sink));

        Job job = instance().getJet().newJob(dag);

        IMap<Integer, Integer> sourceMap = instance().getMap(streamSourceName);
        sourceMap.put(1, 1); // ADDED
        sourceMap.remove(1); // REMOVED - filtered out
        sourceMap.put(1, 2); // ADDED

        IList<Entry<Integer, Integer>> sinkList = instance().getList(streamSinkName);
        assertTrueEventually(() -> {
            assertEquals(2, sinkList.size());

            Entry<Integer, Integer> e = sinkList.get(0);
            assertEquals(Integer.valueOf(1), e.getKey());
            assertEquals(Integer.valueOf(1), e.getValue());

            e = sinkList.get(1);
            assertEquals(Integer.valueOf(1), e.getKey());
            assertEquals(Integer.valueOf(2), e.getValue());
        });

        job.cancel();
    }

    @Test
    public void test_defaultFilter_cacheJournal() {
        DAG dag = new DAG();
        Vertex source = dag.newVertex("source", streamCacheP(streamSourceName, START_FROM_OLDEST,
                eventTimePolicy(Entry<Integer, Integer>::getValue, limitingLag(0), 1, 0, 10_000)));
        Vertex sink = dag.newVertex("sink", writeListP(streamSinkName));

        dag.edge(between(source, sink));

        Job job = instance().getJet().newJob(dag);

        ICache<Object, Object> sourceCache = instance().getCacheManager().getCache(streamSourceName);
        sourceCache.put(1, 1); // ADDED
        sourceCache.remove(1); // REMOVED - filtered out
        sourceCache.put(1, 2); // UPDATED

        IList<Entry<Integer, Integer>> sinkList = instance().getList(streamSinkName);
        assertTrueEventually(() -> {
            assertEquals(2, sinkList.size());

            Entry<Integer, Integer> e = sinkList.get(0);
            assertEquals(Integer.valueOf(1), e.getKey());
            assertEquals(Integer.valueOf(1), e.getValue());

            e = sinkList.get(1);
            assertEquals(Integer.valueOf(1), e.getKey());
            assertEquals(Integer.valueOf(2), e.getValue());
        });

        job.cancel();
    }
}
