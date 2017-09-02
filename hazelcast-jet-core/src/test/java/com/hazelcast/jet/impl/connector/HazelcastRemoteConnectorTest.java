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
import com.hazelcast.jet.DAG;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.JetTestInstanceFactory;
import com.hazelcast.jet.JetTestSupport;
import com.hazelcast.jet.Vertex;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.processor.Sinks;
import com.hazelcast.jet.stream.IStreamList;
import com.hazelcast.map.journal.EventJournalMapEvent;
import com.hazelcast.nio.Address;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.hazelcast.jet.Edge.between;
import static com.hazelcast.jet.processor.Sinks.writeCache;
import static com.hazelcast.jet.processor.Sinks.writeList;
import static com.hazelcast.jet.processor.Sinks.writeMap;
import static com.hazelcast.jet.processor.Sources.readCache;
import static com.hazelcast.jet.processor.Sources.readList;
import static com.hazelcast.jet.processor.Sources.readMap;
import static com.hazelcast.jet.processor.Sources.streamCache;
import static com.hazelcast.jet.processor.Sources.streamMap;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Category(QuickTest.class)
@RunWith(HazelcastSerialClassRunner.class)
public class HazelcastRemoteConnectorTest extends JetTestSupport {

    private static final int ITEM_COUNT = 20;

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
        factory.newMember(jetConfig);

        Config config = new Config();
        config.addCacheConfig(new CacheSimpleConfig().setName("*"));
        config.addEventJournalConfig(new EventJournalConfig().setCacheName("default").setMapName("default"));
        hz = Hazelcast.newHazelcastInstance(config);
        Hazelcast.newHazelcastInstance(config);

        clientConfig = new ClientConfig();
        clientConfig.getGroupConfig().setName("dev");
        clientConfig.getGroupConfig().setPassword("dev-pass");
        Address address = hz.getCluster().getLocalMember().getAddress();
        clientConfig.getNetworkConfig().addAddress(address.getHost() + ':' + address.getPort());
    }

    @After
    public void after() throws Exception {
        Hazelcast.shutdownAll();
        factory.terminateAll();
    }

    @Test
    public void when_listReaderConfiguredWithClientConfig_then_readFromRemoteCluster() throws Exception {
        populateList(hz.getList("source"));

        DAG dag = new DAG();
        Vertex source = dag.newVertex("source", readList("source", clientConfig)).localParallelism(1);
        Vertex sink = dag.newVertex("sink", writeList("sink"));
        dag.edge(between(source, sink));

        executeAndWait(dag);
        assertEquals(ITEM_COUNT, jet.getList("sink").size());
    }

    @Test
    public void when_listWriterConfiguredWithClientConfig_then_writeToRemoteCluster() throws Exception {
        populateList(jet.getList("source"));

        DAG dag = new DAG();
        Vertex source = dag.newVertex("source", readList("source")).localParallelism(1);
        Vertex sink = dag.newVertex("sink", writeList("sink", clientConfig));
        dag.edge(between(source, sink));

        executeAndWait(dag);
        assertEquals(ITEM_COUNT, hz.getList("sink").size());
    }

    @Test
    public void when_mapReaderConfiguredWithClientConfig_then_readFromRemoteCluster_withProjectionAndPredicate()
            throws Exception {
        populateMap(hz.getMap("source"));

        DAG dag = new DAG();
        Vertex source = dag.newVertex("source", readMap("source",
                e -> !e.getKey().equals(0), Map.Entry::getKey, clientConfig)).localParallelism(4);
        Vertex sink = dag.newVertex("sink", Sinks.writeList("sink")).localParallelism(1);
        dag.edge(between(source, sink));

        executeAndWait(dag);
        IStreamList<Object> list = jet.getList("sink");
        assertEquals(ITEM_COUNT - 1, list.size());
        assertFalse(list.contains(0));
        assertTrue(list.contains(1));
    }

    @Test
    public void when_mapWriterConfiguredWithClientConfig_then_writeToRemoteCluster() throws Exception {
        populateMap(jet.getMap("producer"));

        DAG dag = new DAG();
        Vertex producer = dag.newVertex("producer", readMap("producer"));
        Vertex consumer = dag.newVertex("consumer", writeMap("consumer", clientConfig));
        dag.edge(between(producer, consumer));

        executeAndWait(dag);
        assertEquals(ITEM_COUNT, hz.getMap("consumer").size());
    }

    @Test
    public void when_cacheReaderConfiguredWithClientConfig_then_readFromRemoteCluster() throws Exception {
        populateCache(hz.getCacheManager().getCache("source"));

        DAG dag = new DAG();
        Vertex source = dag.newVertex("source", readCache("source", clientConfig));
        Vertex sink = dag.newVertex("sink", writeList("sink"));
        dag.edge(between(source, sink));

        executeAndWait(dag);
        assertEquals(ITEM_COUNT, jet.getList("sink").size());
    }

    @Test
    public void when_cacheWriterConfiguredWithClientConfig_then_writeToRemoteCluster() throws Exception {
        populateCache(jet.getCacheManager().getCache("producer"));

        DAG dag = new DAG();
        Vertex producer = dag.newVertex("producer", readCache("producer"));
        Vertex consumer = dag.newVertex("consumer", writeCache("consumer", clientConfig));
        dag.edge(between(producer, consumer));

        executeAndWait(dag);
        assertEquals(ITEM_COUNT, hz.getCacheManager().getCache("consumer").size());
    }

    @Test
    public void when_mapStreamerConfiguredWithClientConfig_then_streamFromRemoteCluster() throws Exception {
        DAG dag = new DAG();
        Vertex source = dag.newVertex("source", streamMap("source", clientConfig));
        Vertex sink = dag.newVertex("sink", writeList("sink"));
        dag.edge(between(source, sink));

        Future<Void> future = jet.newJob(dag).getFuture();

        populateMap(hz.getMap("source"));

        assertSizeEventually(ITEM_COUNT, jet.getList("sink"));
        future.cancel(true);
    }

    @Test
    public void when_mapStreamerConfiguredWithClientConfig_withFilter_then_streamFromRemoteCluster() throws Exception {
        DAG dag = new DAG();
        Vertex source = dag.newVertex("source", streamMap("source", clientConfig,
                event -> !event.getKey().equals(0), null, false));
        Vertex sink = dag.newVertex("sink", writeList("sink"));
        dag.edge(between(source, sink));

        Future<Void> future = jet.newJob(dag).getFuture();

        populateMap(hz.getMap("source"));

        assertSizeEventually(ITEM_COUNT - 1, jet.getList("sink"));
        future.cancel(true);
    }

    @Test
    public void when_mapStreamerConfiguredWithClientConfig_withProjection_then_streamFromRemoteCluster() throws Exception {
        DAG dag = new DAG();
        Vertex source = dag.newVertex("source", streamMap("source", clientConfig,
                null, EventJournalMapEvent::getKey, false));
        Vertex sink = dag.newVertex("sink", writeList("sink"));
        dag.edge(between(source, sink));

        Future<Void> future = jet.newJob(dag).getFuture();

        populateMap(hz.getMap("source"));

        assertSizeEventually(ITEM_COUNT, jet.getList("sink"));
        assertTrue(jet.getList("sink").contains(0));
        future.cancel(true);
    }

    @Test
    public void when_mapStreamerConfiguredWithClientConfig_withFilter_withProjection_then_streamFromRemoteCluster()
            throws Exception {
        DAG dag = new DAG();
        Vertex source = dag.newVertex("source", streamMap("source", clientConfig,
                event -> !event.getKey().equals(0), EventJournalMapEvent::getKey, false));
        Vertex sink = dag.newVertex("sink", writeList("sink"));
        dag.edge(between(source, sink));

        Future<Void> future = jet.newJob(dag).getFuture();

        populateMap(hz.getMap("source"));

        assertSizeEventually(ITEM_COUNT - 1, jet.getList("sink"));
        assertFalse(jet.getList("sink").contains(0));
        assertTrue(jet.getList("sink").contains(1));
        future.cancel(true);
    }

    @Test
    public void when_cacheStreamerConfiguredWithClientConfig_then_streamFromRemoteCluster() throws Exception {
        DAG dag = new DAG();
        Vertex source = dag.newVertex("source", streamCache("source", clientConfig)).localParallelism(4);
        Vertex sink = dag.newVertex("sink", writeList("sink")).localParallelism(1);
        dag.edge(between(source, sink));

        Future<Void> future = jet.newJob(dag).getFuture();

        populateCache(hz.getCacheManager().getCache("source"));

        assertSizeEventually(ITEM_COUNT, jet.getList("sink"));
        future.cancel(true);
    }

    @Test
    public void when_cacheStreamerConfiguredWithClientConfig_withFilter_then_streamFromRemoteCluster() throws Exception {
        DAG dag = new DAG();
        Vertex source = dag.newVertex("source", streamCache("source", clientConfig,
                event -> !event.getKey().equals(0), null, false));
        Vertex sink = dag.newVertex("sink", writeList("sink"));
        dag.edge(between(source, sink));

        Future<Void> future = jet.newJob(dag).getFuture();

        populateCache(hz.getCacheManager().getCache("source"));

        assertSizeEventually(ITEM_COUNT - 1, jet.getList("sink"));
        future.cancel(true);
    }

    @Test
    public void when_cacheStreamerConfiguredWithClientConfig_withProjection_then_streamFromRemoteCluster()
            throws Exception {
        DAG dag = new DAG();
        Vertex source = dag.newVertex("source", streamCache("source", clientConfig,
                null, EventJournalCacheEvent::getKey, false));
        Vertex sink = dag.newVertex("sink", writeList("sink"));
        dag.edge(between(source, sink));

        Future<Void> future = jet.newJob(dag).getFuture();

        populateCache(hz.getCacheManager().getCache("source"));

        assertSizeEventually(ITEM_COUNT, jet.getList("sink"));
        assertTrue(jet.getList("sink").contains(0));
        future.cancel(true);
    }

    @Test
    public void when_cacheStreamerConfiguredWithClientConfig_withFilter_withProjection_then_streamFromRemoteCluster()
            throws Exception {
        DAG dag = new DAG();
        Vertex source = dag.newVertex("source", streamCache("source", clientConfig,
                event -> !event.getKey().equals(0), EventJournalCacheEvent::getKey, false));
        Vertex sink = dag.newVertex("sink", writeList("sink"));
        dag.edge(between(source, sink));

        Future<Void> future = jet.newJob(dag).getFuture();

        populateCache(hz.getCacheManager().getCache("source"));

        assertSizeEventually(ITEM_COUNT - 1, jet.getList("sink"));
        assertFalse(jet.getList("sink").contains(0));
        assertTrue(jet.getList("sink").contains(1));
        future.cancel(true);
    }

    private void executeAndWait(DAG dag) {
        assertCompletesEventually(jet.newJob(dag).getFuture());
    }

    private static void populateList(List<Object> list) {
        list.addAll(range(0, ITEM_COUNT).boxed().collect(toList()));
    }

    private static void populateMap(Map<Object, Object> map) {
        map.putAll(IntStream.range(0, ITEM_COUNT).boxed().collect(Collectors.toMap(m -> m, m -> m)));
    }

    private static void populateCache(ICache<Object, Object> cache) {
        cache.putAll(IntStream.range(0, ITEM_COUNT).boxed().collect(Collectors.toMap(m -> m, m -> m)));
    }
}
