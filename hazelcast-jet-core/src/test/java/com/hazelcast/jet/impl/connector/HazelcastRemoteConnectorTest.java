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

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.DAG;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.JetTestInstanceFactory;
import com.hazelcast.jet.JetTestSupport;
import com.hazelcast.jet.Vertex;
import com.hazelcast.jet.processor.Sinks;
import com.hazelcast.jet.stream.IStreamList;
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
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.hazelcast.jet.Edge.between;
import static com.hazelcast.jet.processor.Sinks.writeList;
import static com.hazelcast.jet.processor.Sinks.writeMap;
import static com.hazelcast.jet.processor.Sources.readList;
import static com.hazelcast.jet.processor.Sources.readMap;
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
        factory = new JetTestInstanceFactory();
        jet = factory.newMember();
        factory.newMember();
        hz = Hazelcast.newHazelcastInstance();
        Hazelcast.newHazelcastInstance();

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
        Vertex sink = dag.newVertex("sink", writeList("sink")).localParallelism(1);
        dag.edge(between(source, sink));

        executeAndWait(dag);
        assertEquals(ITEM_COUNT, jet.getList("sink").size());
    }

    @Test
    public void when_listWriterConfiguredWithClientConfig_then_writeToRemoteCluster() throws Exception {
        populateList(jet.getList("source"));

        DAG dag = new DAG();
        Vertex source = dag.newVertex("source", readList("source")).localParallelism(1);
        Vertex sink = dag.newVertex("sink", writeList("sink", clientConfig)).localParallelism(4);
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
        Vertex producer = dag.newVertex("producer", readMap("producer")).localParallelism(4);
        Vertex consumer = dag.newVertex("consumer", writeMap("consumer", clientConfig)).localParallelism(4);
        dag.edge(between(producer, consumer));

        executeAndWait(dag);
        assertEquals(ITEM_COUNT, hz.getMap("consumer").size());
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
}
