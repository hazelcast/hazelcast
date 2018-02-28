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

package com.hazelcast.jet.impl.connector;

import com.hazelcast.cache.ICache;
import com.hazelcast.cache.journal.EventJournalCacheEvent;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.JetTestInstanceFactory;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.processor.SourceProcessors;
import com.hazelcast.jet.IListJet;
import com.hazelcast.map.journal.EventJournalMapEvent;
import com.hazelcast.nio.Address;
import com.hazelcast.projection.Projections;
import com.hazelcast.query.Predicates;
import com.hazelcast.test.HazelcastSerialClassRunner;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.IntStream;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import static com.hazelcast.jet.pipeline.JournalInitialPosition.START_FROM_OLDEST;
import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.WatermarkEmissionPolicy.noThrottling;
import static com.hazelcast.jet.core.WatermarkGenerationParams.wmGenParams;
import static com.hazelcast.jet.core.WatermarkPolicies.limitingLag;
import static com.hazelcast.jet.core.processor.SinkProcessors.writeListP;
import static com.hazelcast.jet.core.processor.SinkProcessors.writeRemoteCacheP;
import static com.hazelcast.jet.core.processor.SinkProcessors.writeRemoteListP;
import static com.hazelcast.jet.core.processor.SinkProcessors.writeRemoteMapP;
import static com.hazelcast.jet.core.processor.SourceProcessors.readCacheP;
import static com.hazelcast.jet.core.processor.SourceProcessors.readListP;
import static com.hazelcast.jet.core.processor.SourceProcessors.readMapP;
import static com.hazelcast.jet.core.processor.SourceProcessors.readRemoteCacheP;
import static com.hazelcast.jet.core.processor.SourceProcessors.readRemoteListP;
import static com.hazelcast.jet.core.processor.SourceProcessors.readRemoteMapP;
import static com.hazelcast.jet.core.processor.SourceProcessors.streamRemoteCacheP;
import static com.hazelcast.jet.core.processor.SourceProcessors.streamRemoteMapP;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.IntStream.range;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
public class HazelcastRemoteConnectorTest extends JetTestSupport {

    private static final int ITEM_COUNT = 20;
    private static String SOURCE_NAME = randomName() + "-source";
    private static String SINK_NAME = randomName() + "-sink";

    private static JetInstance jet;
    private static HazelcastInstance hz;
    private static ClientConfig clientConfig;
    private static final JetTestInstanceFactory factory = new JetTestInstanceFactory();

    @BeforeClass
    public static void setUp() {
        JetConfig jetConfig = new JetConfig();
        Config hazelcastConfig = jetConfig.getHazelcastConfig();
        hazelcastConfig.addCacheConfig(new CacheSimpleConfig().setName("*"));

        jet = factory.newMember(jetConfig);
        JetInstance jet2 = factory.newMember(jetConfig);

        Config config = new Config();
        config.addCacheConfig(new CacheSimpleConfig().setName("*"));
        config.addEventJournalConfig(new EventJournalConfig().setCacheName("default").setMapName("default"));
        config.getGroupConfig().setName(randomName());
        hz = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance hz2 = Hazelcast.newHazelcastInstance(config);

        clientConfig = new ClientConfig();
        clientConfig.getGroupConfig().setName(config.getGroupConfig().getName());
        Address address = hz.getCluster().getLocalMember().getAddress();
        clientConfig.getNetworkConfig().addAddress(address.getHost() + ':' + address.getPort());

        // workaround for `cache is not created` exception, create cache locally on all nodes
        hz2.getCacheManager().getCache(SOURCE_NAME);
        hz2.getCacheManager().getCache(SINK_NAME);
        jet2.getCacheManager().getCache(SOURCE_NAME);
        jet2.getCacheManager().getCache(SINK_NAME);
    }

    @AfterClass
    public static void teardown() {
        Hazelcast.shutdownAll();
        factory.shutdownAll();
    }

    @Before
    public void setup() {
        destroyObjects(jet.getHazelcastInstance());
        destroyObjects(hz);
        SOURCE_NAME = randomName() + "-source";
        SINK_NAME = randomName() + "-sink";
    }

    public void destroyObjects(HazelcastInstance hz) {
        hz.getDistributedObjects().forEach(DistributedObject::destroy);
    }

    @Test
    public void when_readRemoteList() {
        populateList(hz.getList(SOURCE_NAME));

        DAG dag = new DAG();
        Vertex source = dag.newVertex(SOURCE_NAME, readRemoteListP(SOURCE_NAME, clientConfig)).localParallelism(1);
        Vertex sink = dag.newVertex(SINK_NAME, writeListP(SINK_NAME));
        dag.edge(between(source, sink));

        executeAndWait(dag);
        assertEquals(ITEM_COUNT, jet.getList(SINK_NAME).size());
    }

    @Test
    public void when_writeRemoteList() {
        populateList(jet.getList(SOURCE_NAME));

        DAG dag = new DAG();
        Vertex source = dag.newVertex(SOURCE_NAME, readListP(SOURCE_NAME)).localParallelism(1);
        Vertex sink = dag.newVertex(SINK_NAME, writeRemoteListP(SINK_NAME, clientConfig));
        dag.edge(between(source, sink));

        executeAndWait(dag);
        assertEquals(ITEM_COUNT, hz.getList(SINK_NAME).size());
    }

    @Test
    public void when_readRemoteMap_withNativePredicateAndProjection() {
        populateMap(hz.getMap(SOURCE_NAME));

        DAG dag = new DAG();
        Vertex source = dag.newVertex("source",
                readRemoteMapP(SOURCE_NAME, clientConfig,
                        Predicates.greaterThan("this", "0"),
                        Projections.singleAttribute("value")
                )
        ).localParallelism(4);
        Vertex sink = dag.newVertex(SINK_NAME, writeListP(SINK_NAME)).localParallelism(1);
        dag.edge(between(source, sink));

        executeAndWait(dag);
        IListJet<Object> list = jet.getList(SINK_NAME);
        assertEquals(ITEM_COUNT - 1, list.size());
        assertFalse(list.contains(0));
        assertTrue(list.contains(1));
    }

    @Test
    public void when_readRemoteMap_withPredicateAndDistributedFunction() {
        populateMap(hz.getMap(SOURCE_NAME));

        DAG dag = new DAG();
        Vertex source = dag.newVertex(SOURCE_NAME, readRemoteMapP(SOURCE_NAME,
                clientConfig, e -> !e.getKey().equals(0), Entry::getValue)).localParallelism(4);
        Vertex sink = dag.newVertex(SINK_NAME, writeListP(SINK_NAME)).localParallelism(1);
        dag.edge(between(source, sink));

        executeAndWait(dag);
        IListJet<Object> list = jet.getList(SINK_NAME);
        assertEquals(ITEM_COUNT - 1, list.size());
        assertFalse(list.contains(0));
        assertTrue(list.contains(1));
    }

    @Test
    public void when_writeRemoteMap() {
        populateMap(jet.getMap(SOURCE_NAME));

        DAG dag = new DAG();
        Vertex producer = dag.newVertex(SOURCE_NAME, readMapP(SOURCE_NAME));
        Vertex consumer = dag.newVertex(SINK_NAME, writeRemoteMapP(SINK_NAME, clientConfig));
        dag.edge(between(producer, consumer));

        executeAndWait(dag);
        assertEquals(ITEM_COUNT, hz.getMap(SINK_NAME).size());
    }

    @Test
    public void when_readRemoteCache() {
        populateCache(hz.getCacheManager().getCache(SOURCE_NAME));

        DAG dag = new DAG();
        Vertex source = dag.newVertex(SOURCE_NAME, readRemoteCacheP(SOURCE_NAME, clientConfig));
        Vertex sink = dag.newVertex(SINK_NAME, writeListP(SINK_NAME));
        dag.edge(between(source, sink));

        executeAndWait(dag);
        assertEquals(ITEM_COUNT, jet.getList(SINK_NAME).size());
    }

    @Test
    public void when_writeRemoteCache() {
        populateCache(jet.getCacheManager().getCache(SOURCE_NAME));

        DAG dag = new DAG();
        Vertex producer = dag.newVertex(SOURCE_NAME, readCacheP(SOURCE_NAME));
        Vertex consumer = dag.newVertex(SINK_NAME, writeRemoteCacheP(SINK_NAME, clientConfig));
        dag.edge(between(producer, consumer));

        executeAndWait(dag);
        assertEquals(ITEM_COUNT, hz.getCacheManager().getCache(SINK_NAME).size());
    }

    @Test
    public void when_streamRemoteMap() {
        DAG dag = new DAG();
        Vertex source = dag.newVertex(SOURCE_NAME, streamRemoteMapP(SOURCE_NAME, clientConfig, START_FROM_OLDEST,
                wmGenParams(Entry<Integer, Integer>::getValue, limitingLag(0), noThrottling(), 10_000)));
        Vertex sink = dag.newVertex(SINK_NAME, writeListP(SINK_NAME));
        dag.edge(between(source, sink));

        Job job = jet.newJob(dag);

        populateMap(hz.getMap(SOURCE_NAME));

        assertSizeEventually(ITEM_COUNT, jet.getList(SINK_NAME));
        job.cancel();
    }

    @Test
    public void when_streamRemoteMap_withPredicateAndProjection() {
        DAG dag = new DAG();
        Vertex source = dag.newVertex(SOURCE_NAME, SourceProcessors.<Integer, Integer, Integer>streamRemoteMapP(
                SOURCE_NAME, clientConfig, event -> event.getKey() != 0, EventJournalMapEvent::getKey, START_FROM_OLDEST,
                wmGenParams(i -> i, limitingLag(0), noThrottling(), 10_000)));
        Vertex sink = dag.newVertex(SINK_NAME, writeListP(SINK_NAME));
        dag.edge(between(source, sink));

        Job job = jet.newJob(dag);

        populateMap(hz.getMap(SOURCE_NAME));

        assertSizeEventually(ITEM_COUNT - 1, jet.getList(SINK_NAME));
        assertFalse(jet.getList(SINK_NAME).contains(0));
        assertTrue(jet.getList(SINK_NAME).contains(1));
        job.cancel();
    }

    @Test
    public void when_streamRemoteCache() {
        DAG dag = new DAG();
        Vertex source = dag.newVertex(SOURCE_NAME,
                streamRemoteCacheP(SOURCE_NAME, clientConfig, START_FROM_OLDEST,
                        wmGenParams(Entry<Integer, Integer>::getValue, limitingLag(0), noThrottling(), 10_000))
        ).localParallelism(4);
        Vertex sink = dag.newVertex(SINK_NAME, writeListP(SINK_NAME)).localParallelism(1);
        dag.edge(between(source, sink));

        Job job = jet.newJob(dag);

        populateCache(hz.getCacheManager().getCache(SOURCE_NAME));

        assertSizeEventually(ITEM_COUNT, jet.getList(SINK_NAME));
        job.cancel();
    }

    @Test
    public void when_streamRemoteCache_withPredicateAndProjection() {
        DAG dag = new DAG();
        Vertex source = dag.newVertex(SOURCE_NAME, SourceProcessors.<Integer, Integer, Integer>streamRemoteCacheP(
                SOURCE_NAME, clientConfig, event -> !event.getKey().equals(0), EventJournalCacheEvent::getKey,
                START_FROM_OLDEST,
                wmGenParams(i -> i, limitingLag(0), noThrottling(), 10_000)));
        Vertex sink = dag.newVertex(SINK_NAME, writeListP(SINK_NAME));
        dag.edge(between(source, sink));

        Job job = jet.newJob(dag);

        populateCache(hz.getCacheManager().getCache(SOURCE_NAME));

        assertSizeEventually(ITEM_COUNT - 1, jet.getList(SINK_NAME));
        assertFalse(jet.getList(SINK_NAME).contains(0));
        assertTrue(jet.getList(SINK_NAME).contains(1));
        job.cancel();
    }

    private void executeAndWait(DAG dag) {
        assertCompletesEventually(jet.newJob(dag).getFuture());
    }

    private static void populateList(List<Object> list) {
        list.addAll(range(0, ITEM_COUNT).boxed().collect(toList()));
    }

    private static void populateMap(Map<Object, Object> map) {
        map.putAll(IntStream.range(0, ITEM_COUNT).boxed().collect(toMap(m -> m, m -> m)));
    }

    private static void populateCache(ICache<Object, Object> cache) {
        cache.putAll(IntStream.range(0, ITEM_COUNT).boxed().collect(toMap(m -> m, m -> m)));
    }
}
