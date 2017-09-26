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
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.JetTestInstanceFactory;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.core.processor.SinkProcessors;
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
import java.util.Map.Entry;
import java.util.concurrent.Future;
import java.util.stream.IntStream;

import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.processor.SinkProcessors.writeCache;
import static com.hazelcast.jet.core.processor.SinkProcessors.writeList;
import static com.hazelcast.jet.core.processor.SinkProcessors.writeMap;
import static com.hazelcast.jet.core.processor.SourceProcessors.readCache;
import static com.hazelcast.jet.core.processor.SourceProcessors.readList;
import static com.hazelcast.jet.core.processor.SourceProcessors.readMap;
import static com.hazelcast.jet.core.processor.SourceProcessors.streamCache;
import static com.hazelcast.jet.core.processor.SourceProcessors.streamMap;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.IntStream.range;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Category(QuickTest.class)
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
    public void when_listReaderConfiguredWithClientConfig_then_readFromRemoteCluster() throws Exception {
        populateList(hz.getList(SOURCE_NAME));

        DAG dag = new DAG();
        Vertex source = dag.newVertex(SOURCE_NAME, readList(SOURCE_NAME, clientConfig)).localParallelism(1);
        Vertex sink = dag.newVertex(SINK_NAME, writeList(SINK_NAME));
        dag.edge(between(source, sink));

        executeAndWait(dag);
        assertEquals(ITEM_COUNT, jet.getList(SINK_NAME).size());
    }

    @Test
    public void when_listWriterConfiguredWithClientConfig_then_writeToRemoteCluster() throws Exception {
        populateList(jet.getList(SOURCE_NAME));

        DAG dag = new DAG();
        Vertex source = dag.newVertex(SOURCE_NAME, readList(SOURCE_NAME)).localParallelism(1);
        Vertex sink = dag.newVertex(SINK_NAME, writeList(SINK_NAME, clientConfig));
        dag.edge(between(source, sink));

        executeAndWait(dag);
        assertEquals(ITEM_COUNT, hz.getList(SINK_NAME).size());
    }

    @Test
    public void when_mapReaderConfiguredWithClientConfigAndFilter_then_readFromRemoteCluster_withPredicate()
            throws Exception {
        populateMap(hz.getMap(SOURCE_NAME));

        DAG dag = new DAG();
        Vertex source = dag.newVertex(SOURCE_NAME, readMap(SOURCE_NAME,
                e -> !e.getKey().equals(0), Entry::getValue, clientConfig)).localParallelism(4);
        Vertex sink = dag.newVertex(SINK_NAME, SinkProcessors.writeList(SINK_NAME)).localParallelism(1);
        dag.edge(between(source, sink));

        executeAndWait(dag);
        IStreamList<Object> list = jet.getList(SINK_NAME);
        assertEquals(ITEM_COUNT - 1, list.size());
        assertFalse(list.contains(0));
        assertTrue(list.contains(1));
    }

    @Test
    public void when_mapWriterConfiguredWithClientConfig_then_writeToRemoteCluster() throws Exception {
        populateMap(jet.getMap(SOURCE_NAME));

        DAG dag = new DAG();
        Vertex producer = dag.newVertex(SOURCE_NAME, readMap(SOURCE_NAME));
        Vertex consumer = dag.newVertex(SINK_NAME, writeMap(SINK_NAME, clientConfig));
        dag.edge(between(producer, consumer));

        executeAndWait(dag);
        assertEquals(ITEM_COUNT, hz.getMap(SINK_NAME).size());
    }

    @Test
    public void when_cacheReaderConfiguredWithClientConfig_then_readFromRemoteCluster() throws Exception {
        populateCache(hz.getCacheManager().getCache(SOURCE_NAME));

        DAG dag = new DAG();
        Vertex source = dag.newVertex(SOURCE_NAME, readCache(SOURCE_NAME, clientConfig));
        Vertex sink = dag.newVertex(SINK_NAME, writeList(SINK_NAME));
        dag.edge(between(source, sink));

        executeAndWait(dag);
        assertEquals(ITEM_COUNT, jet.getList(SINK_NAME).size());
    }

    @Test
    public void when_cacheWriterConfiguredWithClientConfig_then_writeToRemoteCluster() throws Exception {
        populateCache(jet.getCacheManager().getCache(SOURCE_NAME));

        DAG dag = new DAG();
        Vertex producer = dag.newVertex(SOURCE_NAME, readCache(SOURCE_NAME));
        Vertex consumer = dag.newVertex(SINK_NAME, writeCache(SINK_NAME, clientConfig));
        dag.edge(between(producer, consumer));

        executeAndWait(dag);
        assertEquals(ITEM_COUNT, hz.getCacheManager().getCache(SINK_NAME).size());
    }

    @Test
    public void when_mapStreamerConfiguredWithClientConfig_then_streamFromRemoteCluster() throws Exception {
        DAG dag = new DAG();
        Vertex source = dag.newVertex(SOURCE_NAME, streamMap(SOURCE_NAME, clientConfig, false));
        Vertex sink = dag.newVertex(SINK_NAME, writeList(SINK_NAME));
        dag.edge(between(source, sink));

        Future<Void> future = jet.newJob(dag).getFuture();

        populateMap(hz.getMap(SOURCE_NAME));

        assertSizeEventually(ITEM_COUNT, jet.getList(SINK_NAME));
        future.cancel(true);
    }

    @Test
    public void when_mapStreamerConfiguredWithClientConfig_withFilter_then_streamFromRemoteCluster() throws Exception {
        DAG dag = new DAG();
        Vertex source = dag.newVertex(SOURCE_NAME, streamMap(SOURCE_NAME, clientConfig,
                event -> !event.getKey().equals(0), null, false));
        Vertex sink = dag.newVertex(SINK_NAME, writeList(SINK_NAME));
        dag.edge(between(source, sink));

        Future<Void> future = jet.newJob(dag).getFuture();

        populateMap(hz.getMap(SOURCE_NAME));

        assertSizeEventually(ITEM_COUNT - 1, jet.getList(SINK_NAME));
        future.cancel(true);
    }

    @Test
    public void when_mapStreamerConfiguredWithClientConfig_withProjection_then_streamFromRemoteCluster() throws Exception {
        DAG dag = new DAG();
        Vertex source = dag.newVertex(SOURCE_NAME, streamMap(SOURCE_NAME, clientConfig,
                null, EventJournalMapEvent::getKey, false));
        Vertex sink = dag.newVertex(SINK_NAME, writeList(SINK_NAME));
        dag.edge(between(source, sink));

        Future<Void> future = jet.newJob(dag).getFuture();

        populateMap(hz.getMap(SOURCE_NAME));

        assertSizeEventually(ITEM_COUNT, jet.getList(SINK_NAME));
        assertTrue(jet.getList(SINK_NAME).contains(0));
        future.cancel(true);
    }

    @Test
    public void when_mapStreamerConfiguredWithClientConfig_withFilter_withProjection_then_streamFromRemoteCluster()
            throws Exception {
        DAG dag = new DAG();
        Vertex source = dag.newVertex(SOURCE_NAME, streamMap(SOURCE_NAME, clientConfig,
                event -> !event.getKey().equals(0), EventJournalMapEvent::getKey, false));
        Vertex sink = dag.newVertex(SINK_NAME, writeList(SINK_NAME));
        dag.edge(between(source, sink));

        Future<Void> future = jet.newJob(dag).getFuture();

        populateMap(hz.getMap(SOURCE_NAME));

        assertSizeEventually(ITEM_COUNT - 1, jet.getList(SINK_NAME));
        assertFalse(jet.getList(SINK_NAME).contains(0));
        assertTrue(jet.getList(SINK_NAME).contains(1));
        future.cancel(true);
    }

    @Test
    public void when_cacheStreamerConfiguredWithClientConfig_then_streamFromRemoteCluster() throws Exception {
        DAG dag = new DAG();
        Vertex source = dag.newVertex(SOURCE_NAME, streamCache(SOURCE_NAME, clientConfig, false)).localParallelism(4);
        Vertex sink = dag.newVertex(SINK_NAME, writeList(SINK_NAME)).localParallelism(1);
        dag.edge(between(source, sink));

        Future<Void> future = jet.newJob(dag).getFuture();

        populateCache(hz.getCacheManager().getCache(SOURCE_NAME));

        assertSizeEventually(ITEM_COUNT, jet.getList(SINK_NAME));
        future.cancel(true);
    }

    @Test
    public void when_cacheStreamerConfiguredWithClientConfig_withFilter_then_streamFromRemoteCluster() throws Exception {
        DAG dag = new DAG();
        Vertex source = dag.newVertex(SOURCE_NAME, streamCache(SOURCE_NAME, clientConfig,
                event -> !event.getKey().equals(0), null, false));
        Vertex sink = dag.newVertex(SINK_NAME, writeList(SINK_NAME));
        dag.edge(between(source, sink));

        Future<Void> future = jet.newJob(dag).getFuture();

        populateCache(hz.getCacheManager().getCache(SOURCE_NAME));

        assertSizeEventually(ITEM_COUNT - 1, jet.getList(SINK_NAME));
        future.cancel(true);
    }

    @Test
    public void when_cacheStreamerConfiguredWithClientConfig_withProjection_then_streamFromRemoteCluster()
            throws Exception {
        DAG dag = new DAG();
        Vertex source = dag.newVertex(SOURCE_NAME, streamCache(SOURCE_NAME, clientConfig,
                null, EventJournalCacheEvent::getKey, false));
        Vertex sink = dag.newVertex(SINK_NAME, writeList(SINK_NAME));
        dag.edge(between(source, sink));

        Future<Void> future = jet.newJob(dag).getFuture();

        populateCache(hz.getCacheManager().getCache(SOURCE_NAME));

        assertSizeEventually(ITEM_COUNT, jet.getList(SINK_NAME));
        assertTrue(jet.getList(SINK_NAME).contains(0));
        future.cancel(true);
    }

    @Test
    public void when_cacheStreamerConfiguredWithClientConfig_withFilter_withProjection_then_streamFromRemoteCluster()
            throws Exception {
        DAG dag = new DAG();
        Vertex source = dag.newVertex(SOURCE_NAME, streamCache(SOURCE_NAME, clientConfig,
                event -> !event.getKey().equals(0), EventJournalCacheEvent::getKey, false));
        Vertex sink = dag.newVertex(SINK_NAME, writeList(SINK_NAME));
        dag.edge(between(source, sink));

        Future<Void> future = jet.newJob(dag).getFuture();

        populateCache(hz.getCacheManager().getCache(SOURCE_NAME));

        assertSizeEventually(ITEM_COUNT - 1, jet.getList(SINK_NAME));
        assertFalse(jet.getList(SINK_NAME).contains(0));
        assertTrue(jet.getList(SINK_NAME).contains(1));
        future.cancel(true);
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
