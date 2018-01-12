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

package com.hazelcast.jet.impl.connector;

import com.hazelcast.cache.ICache;
import com.hazelcast.cache.journal.EventJournalCacheEvent;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.JetTestInstanceFactory;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.stream.IStreamList;
import com.hazelcast.map.journal.EventJournalMapEvent;
import com.hazelcast.nio.Address;
import com.hazelcast.projection.Projections;
import com.hazelcast.query.Predicates;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.IntStream;

import static com.hazelcast.jet.JournalInitialPosition.START_FROM_OLDEST;
import static com.hazelcast.jet.core.Edge.between;
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
    private static final String SOURCE_NAME = "source";
    private static final String SINK_NAME = "sink";

    private JetTestInstanceFactory factory;
    private JetInstance jet;
    private HazelcastInstance hz;
    private ClientConfig clientConfig;

    @Before
    public void before() {
        JetConfig jetConfig = new JetConfig();
        Config hazelcastConfig = jetConfig.getHazelcastConfig();
        hazelcastConfig.addCacheConfig(new CacheSimpleConfig().setName("*"));
        factory = new JetTestInstanceFactory();
        jet = factory.newMember(jetConfig);
        JetInstance jet2 = factory.newMember(jetConfig);

        Config config = new Config();
        config.addCacheConfig(new CacheSimpleConfig().setName("*"));
        config.addEventJournalConfig(new EventJournalConfig().setCacheName("default").setMapName("default"));
        hz = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance hz2 = Hazelcast.newHazelcastInstance(config);

        clientConfig = new ClientConfig();
        clientConfig.getGroupConfig().setName("dev");
        clientConfig.getGroupConfig().setPassword("dev-pass");
        Address address = hz.getCluster().getLocalMember().getAddress();
        clientConfig.getNetworkConfig().addAddress(address.getHost() + ':' + address.getPort());

        // workaround for `cache is not created` exception, create cache locally on all nodes
        jet2.getCacheManager().getCache(SOURCE_NAME);
        jet2.getCacheManager().getCache(SINK_NAME);
        hz2.getCacheManager().getCache(SOURCE_NAME);
        hz2.getCacheManager().getCache(SINK_NAME);
    }

    @After
    public void after() throws Exception {
        Hazelcast.shutdownAll();
        factory.terminateAll();
    }

    @Test
    public void when_readRemoteList() throws Exception {
        populateList(hz.getList(SOURCE_NAME));

        DAG dag = new DAG();
        Vertex source = dag.newVertex(SOURCE_NAME, readRemoteListP(SOURCE_NAME, clientConfig)).localParallelism(1);
        Vertex sink = dag.newVertex(SINK_NAME, writeListP(SINK_NAME));
        dag.edge(between(source, sink));

        executeAndWait(dag);
        assertEquals(ITEM_COUNT, jet.getList(SINK_NAME).size());
    }

    @Test
    public void when_writeRemoteList() throws Exception {
        populateList(jet.getList(SOURCE_NAME));

        DAG dag = new DAG();
        Vertex source = dag.newVertex(SOURCE_NAME, readListP(SOURCE_NAME)).localParallelism(1);
        Vertex sink = dag.newVertex(SINK_NAME, writeRemoteListP(SINK_NAME, clientConfig));
        dag.edge(between(source, sink));

        executeAndWait(dag);
        assertEquals(ITEM_COUNT, hz.getList(SINK_NAME).size());
    }

    @Test
    public void when_readRemoteMap_withNativePredicateAndProjection()
            throws Exception {
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
        IStreamList<Object> list = jet.getList(SINK_NAME);
        assertEquals(ITEM_COUNT - 1, list.size());
        assertFalse(list.contains(0));
        assertTrue(list.contains(1));
    }

    @Test
    public void when_readRemoteMap_withPredicateAndDistributedFunction()
            throws Exception {
        populateMap(hz.getMap(SOURCE_NAME));

        DAG dag = new DAG();
        Vertex source = dag.newVertex(SOURCE_NAME, readRemoteMapP(SOURCE_NAME,
                clientConfig, e -> !e.getKey().equals(0), Entry::getValue)).localParallelism(4);
        Vertex sink = dag.newVertex(SINK_NAME, writeListP(SINK_NAME)).localParallelism(1);
        dag.edge(between(source, sink));

        executeAndWait(dag);
        IStreamList<Object> list = jet.getList(SINK_NAME);
        assertEquals(ITEM_COUNT - 1, list.size());
        assertFalse(list.contains(0));
        assertTrue(list.contains(1));
    }

    @Test
    public void when_writeRemoteMap() throws Exception {
        populateMap(jet.getMap(SOURCE_NAME));

        DAG dag = new DAG();
        Vertex producer = dag.newVertex(SOURCE_NAME, readMapP(SOURCE_NAME));
        Vertex consumer = dag.newVertex(SINK_NAME, writeRemoteMapP(SINK_NAME, clientConfig));
        dag.edge(between(producer, consumer));

        executeAndWait(dag);
        assertEquals(ITEM_COUNT, hz.getMap(SINK_NAME).size());
    }

    @Test
    public void when_readRemoteCache() throws Exception {
        populateCache(hz.getCacheManager().getCache(SOURCE_NAME));

        DAG dag = new DAG();
        Vertex source = dag.newVertex(SOURCE_NAME, readRemoteCacheP(SOURCE_NAME, clientConfig));
        Vertex sink = dag.newVertex(SINK_NAME, writeListP(SINK_NAME));
        dag.edge(between(source, sink));

        executeAndWait(dag);
        assertEquals(ITEM_COUNT, jet.getList(SINK_NAME).size());
    }

    @Test
    public void when_writeRemoteCache() throws Exception {
        populateCache(jet.getCacheManager().getCache(SOURCE_NAME));

        DAG dag = new DAG();
        Vertex producer = dag.newVertex(SOURCE_NAME, readCacheP(SOURCE_NAME));
        Vertex consumer = dag.newVertex(SINK_NAME, writeRemoteCacheP(SINK_NAME, clientConfig));
        dag.edge(between(producer, consumer));

        executeAndWait(dag);
        assertEquals(ITEM_COUNT, hz.getCacheManager().getCache(SINK_NAME).size());
    }

    @Test
    public void when_streamRemoteMap() throws Exception {
        DAG dag = new DAG();
        Vertex source = dag.newVertex(SOURCE_NAME, streamRemoteMapP(SOURCE_NAME, clientConfig, START_FROM_OLDEST));
        Vertex sink = dag.newVertex(SINK_NAME, writeListP(SINK_NAME));
        dag.edge(between(source, sink));

        Job job = jet.newJob(dag);

        populateMap(hz.getMap(SOURCE_NAME));

        assertSizeEventually(ITEM_COUNT, jet.getList(SINK_NAME));
        job.cancel();
    }

    @Test
    public void when_streamRemoteMap_withPredicateAndProjection() throws Exception {
        DAG dag = new DAG();
        Vertex source = dag.newVertex(SOURCE_NAME, streamRemoteMapP(SOURCE_NAME, clientConfig,
                event -> !event.getKey().equals(0), EventJournalMapEvent::getKey, START_FROM_OLDEST));
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
    public void when_streamRemoteCache() throws Exception {
        DAG dag = new DAG();
        Vertex source = dag.newVertex(SOURCE_NAME,
                streamRemoteCacheP(SOURCE_NAME, clientConfig, START_FROM_OLDEST)
        ).localParallelism(4);
        Vertex sink = dag.newVertex(SINK_NAME, writeListP(SINK_NAME)).localParallelism(1);
        dag.edge(between(source, sink));

        Job job = jet.newJob(dag);

        populateCache(hz.getCacheManager().getCache(SOURCE_NAME));

        assertSizeEventually(ITEM_COUNT, jet.getList(SINK_NAME));
        job.cancel();
    }

    @Test
    public void when_streamRemoteCache_withPredicateAndProjection()
            throws Exception {
        DAG dag = new DAG();
        Vertex source = dag.newVertex(SOURCE_NAME, streamRemoteCacheP(SOURCE_NAME, clientConfig,
                event -> !event.getKey().equals(0), EventJournalCacheEvent::getKey, START_FROM_OLDEST));
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
