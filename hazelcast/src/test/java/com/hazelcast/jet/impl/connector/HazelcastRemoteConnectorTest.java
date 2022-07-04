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
import com.hazelcast.client.HazelcastClient;
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
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.processor.SourceProcessors;
import com.hazelcast.map.EventJournalMapEvent;
import com.hazelcast.projection.Projections;
import com.hazelcast.query.Predicates;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
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
@Category(QuickTest.class)
public class HazelcastRemoteConnectorTest extends JetTestSupport {

    private static final int ITEM_COUNT = 20;
    private static String SOURCE_NAME = randomName() + "-source";
    private static String SINK_NAME = randomName() + "-sink";

    private static HazelcastInstance localHz;
    private static HazelcastInstance remoteHz;
    private static ClientConfig clientConfig;
    private static final TestHazelcastFactory factory = new TestHazelcastFactory();

    @BeforeClass
    public static void setUp() {
        Config config = smallInstanceConfig();
        config.addCacheConfig(new CacheSimpleConfig().setName("*"));

        localHz = factory.newHazelcastInstance(config);
        HazelcastInstance localHz2 = factory.newHazelcastInstance(config);

        Config remoteClusterConfig = smallInstanceConfig();
        CacheSimpleConfig cacheConfig = new CacheSimpleConfig().setName("*");
        cacheConfig.getEventJournalConfig().setEnabled(true);
        remoteClusterConfig.addCacheConfig(cacheConfig);
        remoteClusterConfig.setClusterName(randomName());
        MapConfig mapConfig = new MapConfig();
        mapConfig.setName("*").getEventJournalConfig().setEnabled(true);
        remoteClusterConfig.addMapConfig(mapConfig);
        remoteHz = Hazelcast.newHazelcastInstance(remoteClusterConfig);
        HazelcastInstance hz2 = Hazelcast.newHazelcastInstance(remoteClusterConfig);

        clientConfig = new ClientConfig();
        clientConfig.setClusterName(remoteClusterConfig.getClusterName());
        Address address = remoteHz.getCluster().getLocalMember().getAddress();
        clientConfig.getNetworkConfig().addAddress(address.getHost() + ':' + address.getPort());

        // workaround for `cache is not created` exception, create cache locally on all nodes
        hz2.getCacheManager().getCache(SOURCE_NAME);
        hz2.getCacheManager().getCache(SINK_NAME);
        localHz2.getCacheManager().getCache(SOURCE_NAME);
        localHz2.getCacheManager().getCache(SINK_NAME);
    }

    @AfterClass
    public static void teardown() {
        HazelcastClient.shutdownAll();
        HazelcastInstanceFactory.terminateAll();
        factory.terminateAll();
    }

    @Before
    public void setup() {
        destroyObjects(localHz);
        destroyObjects(remoteHz);
        SOURCE_NAME = randomName() + "-source";
        SINK_NAME = randomName() + "-sink";
    }

    public void destroyObjects(HazelcastInstance hz) {
        hz.getDistributedObjects().forEach(DistributedObject::destroy);
    }

    @Test
    public void when_readRemoteMap_withNativePredicateAndProjection() {
        populateMap(remoteHz.getMap(SOURCE_NAME));

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
        IList<Object> list = localHz.getList(SINK_NAME);
        assertEquals(ITEM_COUNT - 1, list.size());
        assertFalse(list.contains(0));
        assertTrue(list.contains(1));
    }

    @Test
    public void when_readRemoteMap_withPredicateAndFunction() {
        populateMap(remoteHz.getMap(SOURCE_NAME));

        DAG dag = new DAG();
        Vertex source = dag.newVertex(SOURCE_NAME, readRemoteMapP(SOURCE_NAME,
                clientConfig, e -> !e.getKey().equals(0), Entry::getValue)).localParallelism(4);
        Vertex sink = dag.newVertex(SINK_NAME, writeListP(SINK_NAME)).localParallelism(1);
        dag.edge(between(source, sink));

        executeAndWait(dag);
        IList<Object> list = localHz.getList(SINK_NAME);
        assertEquals(ITEM_COUNT - 1, list.size());
        assertFalse(list.contains(0));
        assertTrue(list.contains(1));
    }

    @Test
    public void when_writeRemoteMap() {
        populateMap(localHz.getMap(SOURCE_NAME));

        DAG dag = new DAG();
        Vertex producer = dag.newVertex(SOURCE_NAME, readMapP(SOURCE_NAME));
        Vertex consumer = dag.newVertex(SINK_NAME, writeRemoteMapP(SINK_NAME, clientConfig));
        dag.edge(between(producer, consumer));

        executeAndWait(dag);
        assertEquals(ITEM_COUNT, remoteHz.getMap(SINK_NAME).size());
    }

    @Test
    public void when_readRemoteCache() {
        populateCache(remoteHz.getCacheManager().getCache(SOURCE_NAME));

        DAG dag = new DAG();
        Vertex source = dag.newVertex(SOURCE_NAME, readRemoteCacheP(SOURCE_NAME, clientConfig));
        Vertex sink = dag.newVertex(SINK_NAME, writeListP(SINK_NAME));
        dag.edge(between(source, sink));

        executeAndWait(dag);
        assertEquals(ITEM_COUNT, localHz.getList(SINK_NAME).size());
    }

    @Test
    public void when_writeRemoteCache() {
        populateCache(localHz.getCacheManager().getCache(SOURCE_NAME));

        DAG dag = new DAG();
        Vertex producer = dag.newVertex(SOURCE_NAME, readCacheP(SOURCE_NAME));
        Vertex consumer = dag.newVertex(SINK_NAME, writeRemoteCacheP(SINK_NAME, clientConfig));
        dag.edge(between(producer, consumer));

        executeAndWait(dag);
        assertEquals(ITEM_COUNT, remoteHz.getCacheManager().getCache(SINK_NAME).size());
    }

    @Test
    public void when_streamRemoteMap() {
        DAG dag = new DAG();
        Vertex source = dag.newVertex(SOURCE_NAME, streamRemoteMapP(SOURCE_NAME, clientConfig, START_FROM_OLDEST,
                eventTimePolicy(Entry<Integer, Integer>::getValue, limitingLag(0), 1, 0, 10_000)));
        Vertex sink = dag.newVertex(SINK_NAME, writeListP(SINK_NAME));
        dag.edge(between(source, sink));

        Job job = localHz.getJet().newJob(dag);

        populateMap(remoteHz.getMap(SOURCE_NAME));

        assertSizeEventually(ITEM_COUNT, localHz.getList(SINK_NAME));
        job.cancel();
    }

    @Test
    public void when_streamRemoteMap_withPredicateAndProjection() {
        DAG dag = new DAG();
        Vertex source = dag.newVertex(SOURCE_NAME, SourceProcessors.<Integer, Integer, Integer>streamRemoteMapP(
                SOURCE_NAME, clientConfig, event -> event.getKey() != 0, EventJournalMapEvent::getKey, START_FROM_OLDEST,
                eventTimePolicy(i -> i, limitingLag(0), 1, 0, 10_000)));
        Vertex sink = dag.newVertex(SINK_NAME, writeListP(SINK_NAME));
        dag.edge(between(source, sink));

        Job job = localHz.getJet().newJob(dag);

        populateMap(remoteHz.getMap(SOURCE_NAME));

        assertSizeEventually(ITEM_COUNT - 1, localHz.getList(SINK_NAME));
        assertFalse(localHz.getList(SINK_NAME).contains(0));
        assertTrue(localHz.getList(SINK_NAME).contains(1));
        job.cancel();
    }

    @Test
    public void when_streamRemoteCache() {
        DAG dag = new DAG();
        Vertex source = dag.newVertex(SOURCE_NAME,
                streamRemoteCacheP(SOURCE_NAME, clientConfig, START_FROM_OLDEST,
                        eventTimePolicy(Entry<Integer, Integer>::getValue, limitingLag(0), 1, 0, 10_000))
        ).localParallelism(4);
        Vertex sink = dag.newVertex(SINK_NAME, writeListP(SINK_NAME)).localParallelism(1);
        dag.edge(between(source, sink));

        Job job = localHz.getJet().newJob(dag);

        populateCache(remoteHz.getCacheManager().getCache(SOURCE_NAME));

        assertSizeEventually(ITEM_COUNT, localHz.getList(SINK_NAME));
        job.cancel();
    }

    @Test
    public void when_streamRemoteCache_withPredicateAndProjection() {
        DAG dag = new DAG();
        Vertex source = dag.newVertex(SOURCE_NAME, SourceProcessors.<Integer, Integer, Integer>streamRemoteCacheP(
                SOURCE_NAME, clientConfig, event -> !event.getKey().equals(0), EventJournalCacheEvent::getKey,
                START_FROM_OLDEST,
                eventTimePolicy(i -> i, limitingLag(0), 1, 0, 10_000)));
        Vertex sink = dag.newVertex(SINK_NAME, writeListP(SINK_NAME));
        dag.edge(between(source, sink));

        Job job = localHz.getJet().newJob(dag);

        populateCache(remoteHz.getCacheManager().getCache(SOURCE_NAME));

        assertSizeEventually(ITEM_COUNT - 1, localHz.getList(SINK_NAME));
        assertFalse(localHz.getList(SINK_NAME).contains(0));
        assertTrue(localHz.getList(SINK_NAME).contains(1));
        job.cancel();
    }

    private void executeAndWait(DAG dag) {
        assertCompletesEventually(localHz.getJet().newJob(dag).getFuture());
    }


    private static void populateMap(Map<Object, Object> map) {
        map.putAll(IntStream.range(0, ITEM_COUNT).boxed().collect(toMap(m -> m, m -> m)));
    }

    private static void populateCache(ICache<Object, Object> cache) {
        cache.putAll(IntStream.range(0, ITEM_COUNT).boxed().collect(toMap(m -> m, m -> m)));
    }
}
