/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.cluster.Address;
import com.hazelcast.collection.IList;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.core.DAGImpl;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.processor.SourceProcessors;
import com.hazelcast.jet.test.SerialTest;
import com.hazelcast.map.EventJournalMapEvent;
import com.hazelcast.projection.Projections;
import com.hazelcast.query.Predicates;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.IntStream;

import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.EventTimePolicy.eventTimePolicy;
import static com.hazelcast.jet.core.WatermarkPolicy.limitingLag;
import static com.hazelcast.jet.core.processor.SinkProcessors.writeListP;
import static com.hazelcast.jet.core.processor.SinkProcessors.writeRemoteCacheP;
import static com.hazelcast.jet.core.processor.SinkProcessors.writeRemoteMapP;
import static com.hazelcast.jet.core.processor.SourceProcessors.readCacheP;
import static com.hazelcast.jet.core.processor.SourceProcessors.readMapP;
import static com.hazelcast.jet.core.processor.SourceProcessors.readRemoteCacheP;
import static com.hazelcast.jet.core.processor.SourceProcessors.readRemoteMapP;
import static com.hazelcast.jet.core.processor.SourceProcessors.streamRemoteCacheP;
import static com.hazelcast.jet.core.processor.SourceProcessors.streamRemoteMapP;
import static com.hazelcast.jet.pipeline.JournalInitialPosition.START_FROM_OLDEST;
import static java.util.stream.Collectors.toMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SerialTest.class)
public class HazelcastRemoteConnectorTest extends JetTestSupport {

    private static final int ITEM_COUNT = 20;
    private static String sourceName = randomName() + "-source";
    private static String sinkName = randomName() + "-sink";

    private static HazelcastInstance jet;
    private static HazelcastInstance hz;
    private static ClientConfig clientConfig;
    private static final TestHazelcastFactory factory = new TestHazelcastFactory();

    @BeforeClass
    public static void setUp() {
        Config jetConfig = new Config();
        jetConfig.addCacheConfig(new CacheSimpleConfig().setName("*"));

        jet = factory.newHazelcastInstance(jetConfig);
        HazelcastInstance jet2 = factory.newHazelcastInstance(jetConfig);

        Config config = new Config();
        config.getNetworkConfig().getJoin().getMulticastConfig().setLoopbackModeEnabled(true);
        CacheSimpleConfig cacheConfig = new CacheSimpleConfig().setName("*");
        cacheConfig.getEventJournalConfig().setEnabled(true);
        config.addCacheConfig(cacheConfig);
        config.setClusterName(randomName());
        MapConfig mapConfig = new MapConfig();
        mapConfig.setName("*").getEventJournalConfig().setEnabled(true);
        config.addMapConfig(mapConfig);
        hz = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance hz2 = Hazelcast.newHazelcastInstance(config);

        clientConfig = new ClientConfig();
        clientConfig.setClusterName(config.getClusterName());
        Address address = hz.getCluster().getLocalMember().getAddress();
        clientConfig.getNetworkConfig().addAddress(address.getHost() + ':' + address.getPort());

        // workaround for `cache is not created` exception, create cache locally on all nodes
        hz2.getCacheManager().getCache(sourceName);
        hz2.getCacheManager().getCache(sinkName);
        jet2.getCacheManager().getCache(sourceName);
        jet2.getCacheManager().getCache(sinkName);
    }

    @AfterClass
    public static void teardown() {
        HazelcastInstanceFactory.terminateAll();
        factory.terminateAll();
    }

    @Before
    public void setup() {
        destroyObjects(jet);
        destroyObjects(hz);
        sourceName = randomName() + "-source";
        sinkName = randomName() + "-sink";
    }

    public void destroyObjects(HazelcastInstance hz) {
        hz.getDistributedObjects().forEach(DistributedObject::destroy);
    }

    @Test
    public void when_readRemoteMap_withNativePredicateAndProjection() {
        populateMap(hz.getMap(sourceName));

        DAGImpl dag = new DAGImpl();
        Vertex source = dag.newVertex("source",
                readRemoteMapP(sourceName, clientConfig,
                        Predicates.greaterThan("this", "0"),
                        Projections.singleAttribute("value")
                )
        ).localParallelism(4);
        Vertex sink = dag.newVertex(sinkName, writeListP(sinkName)).localParallelism(1);
        dag.edge(between(source, sink));

        executeAndWait(dag);
        IList<Object> list = jet.getList(sinkName);
        assertEquals(ITEM_COUNT - 1, list.size());
        assertFalse(list.contains(0));
        assertTrue(list.contains(1));
    }

    @Test
    public void when_readRemoteMap_withPredicateAndFunction() {
        populateMap(hz.getMap(sourceName));

        DAGImpl dag = new DAGImpl();
        Vertex source = dag.newVertex(sourceName, readRemoteMapP(sourceName,
                clientConfig, e -> !e.getKey().equals(0), Entry::getValue)).localParallelism(4);
        Vertex sink = dag.newVertex(sinkName, writeListP(sinkName)).localParallelism(1);
        dag.edge(between(source, sink));

        executeAndWait(dag);
        IList<Object> list = jet.getList(sinkName);
        assertEquals(ITEM_COUNT - 1, list.size());
        assertFalse(list.contains(0));
        assertTrue(list.contains(1));
    }

    @Test
    public void when_writeRemoteMap() {
        populateMap(jet.getMap(sourceName));

        DAGImpl dag = new DAGImpl();
        Vertex producer = dag.newVertex(sourceName, readMapP(sourceName));
        Vertex consumer = dag.newVertex(sinkName, writeRemoteMapP(sinkName, clientConfig));
        dag.edge(between(producer, consumer));

        executeAndWait(dag);
        assertEquals(ITEM_COUNT, hz.getMap(sinkName).size());
    }

    @Test
    public void when_readRemoteCache() {
        populateCache(hz.getCacheManager().getCache(sourceName));

        DAGImpl dag = new DAGImpl();
        Vertex source = dag.newVertex(sourceName, readRemoteCacheP(sourceName, clientConfig));
        Vertex sink = dag.newVertex(sinkName, writeListP(sinkName));
        dag.edge(between(source, sink));

        executeAndWait(dag);
        assertEquals(ITEM_COUNT, jet.getList(sinkName).size());
    }

    @Test
    public void when_writeRemoteCache() {
        populateCache(jet.getCacheManager().getCache(sourceName));

        DAGImpl dag = new DAGImpl();
        Vertex producer = dag.newVertex(sourceName, readCacheP(sourceName));
        Vertex consumer = dag.newVertex(sinkName, writeRemoteCacheP(sinkName, clientConfig));
        dag.edge(between(producer, consumer));

        executeAndWait(dag);
        assertEquals(ITEM_COUNT, hz.getCacheManager().getCache(sinkName).size());
    }

    @Test
    public void when_streamRemoteMap() {
        DAGImpl dag = new DAGImpl();
        Vertex source = dag.newVertex(sourceName, streamRemoteMapP(sourceName, clientConfig, START_FROM_OLDEST,
                eventTimePolicy(Entry<Integer, Integer>::getValue, limitingLag(0), 1, 0, 10_000)));
        Vertex sink = dag.newVertex(sinkName, writeListP(sinkName));
        dag.edge(between(source, sink));

        Job job = jet.getJetInstance().newJob(dag);

        populateMap(hz.getMap(sourceName));

        assertSizeEventually(ITEM_COUNT, jet.getList(sinkName));
        job.cancel();
    }

    @Test
    public void when_streamRemoteMap_withPredicateAndProjection() {
        DAGImpl dag = new DAGImpl();
        Vertex source = dag.newVertex(sourceName, SourceProcessors.<Integer, Integer, Integer>streamRemoteMapP(
                sourceName, clientConfig, event -> event.getKey() != 0, EventJournalMapEvent::getKey, START_FROM_OLDEST,
                eventTimePolicy(i -> i, limitingLag(0), 1, 0, 10_000)));
        Vertex sink = dag.newVertex(sinkName, writeListP(sinkName));
        dag.edge(between(source, sink));

        Job job = jet.getJetInstance().newJob(dag);

        populateMap(hz.getMap(sourceName));

        assertSizeEventually(ITEM_COUNT - 1, jet.getList(sinkName));
        assertFalse(jet.getList(sinkName).contains(0));
        assertTrue(jet.getList(sinkName).contains(1));
        job.cancel();
    }

    @Test
    public void when_streamRemoteCache() {
        DAGImpl dag = new DAGImpl();
        Vertex source = dag.newVertex(sourceName,
                streamRemoteCacheP(sourceName, clientConfig, START_FROM_OLDEST,
                        eventTimePolicy(Entry<Integer, Integer>::getValue, limitingLag(0), 1, 0, 10_000))
        ).localParallelism(4);
        Vertex sink = dag.newVertex(sinkName, writeListP(sinkName)).localParallelism(1);
        dag.edge(between(source, sink));

        Job job = jet.getJetInstance().newJob(dag);

        populateCache(hz.getCacheManager().getCache(sourceName));

        assertSizeEventually(ITEM_COUNT, jet.getList(sinkName));
        job.cancel();
    }

    @Test
    public void when_streamRemoteCache_withPredicateAndProjection() {
        DAGImpl dag = new DAGImpl();
        Vertex source = dag.newVertex(sourceName, SourceProcessors.<Integer, Integer, Integer>streamRemoteCacheP(
                sourceName, clientConfig, event -> !event.getKey().equals(0), EventJournalCacheEvent::getKey,
                START_FROM_OLDEST,
                eventTimePolicy(i -> i, limitingLag(0), 1, 0, 10_000)));
        Vertex sink = dag.newVertex(sinkName, writeListP(sinkName));
        dag.edge(between(source, sink));

        Job job = jet.getJetInstance().newJob(dag);

        populateCache(hz.getCacheManager().getCache(sourceName));

        assertSizeEventually(ITEM_COUNT - 1, jet.getList(sinkName));
        assertFalse(jet.getList(sinkName).contains(0));
        assertTrue(jet.getList(sinkName).contains(1));
        job.cancel();
    }

    private void executeAndWait(DAGImpl dag) {
        assertCompletesEventually(jet.getJetInstance().newJob(dag).getFuture());
    }


    private static void populateMap(Map<Object, Object> map) {
        map.putAll(IntStream.range(0, ITEM_COUNT).boxed().collect(toMap(m -> m, m -> m)));
    }

    private static void populateCache(ICache<Object, Object> cache) {
        cache.putAll(IntStream.range(0, ITEM_COUNT).boxed().collect(toMap(m -> m, m -> m)));
    }
}
