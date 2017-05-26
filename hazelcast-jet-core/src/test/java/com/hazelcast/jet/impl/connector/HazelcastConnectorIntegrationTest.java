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
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.jet.DAG;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.JetTestSupport;
import com.hazelcast.jet.Vertex;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.stream.IStreamList;
import com.hazelcast.jet.stream.IStreamMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.ExecutionException;

import static com.hazelcast.jet.Edge.between;
import static com.hazelcast.jet.processor.Sources.readCache;
import static com.hazelcast.jet.processor.Sources.readList;
import static com.hazelcast.jet.processor.Sources.readMap;
import static com.hazelcast.jet.processor.Sinks.writeCache;
import static com.hazelcast.jet.processor.Sinks.writeList;
import static com.hazelcast.jet.processor.Sinks.writeMap;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;
import static org.junit.Assert.assertEquals;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class HazelcastConnectorIntegrationTest extends JetTestSupport {

    private static final int ENTRY_COUNT = 100;

    private JetInstance jetInstance;

    private String sourceName;
    private String sinkName;

    @Before
    public void setup() {
        JetConfig jetConfig = new JetConfig();
        jetConfig.getHazelcastConfig().addCacheConfig(new CacheSimpleConfig().setName("*"));
        jetInstance = createJetMember(jetConfig);
        createJetMember(jetConfig);

        sourceName = randomString();
        sinkName = randomString();
    }

    @Test
    public void testMap() throws ExecutionException, InterruptedException {
        IStreamMap<Integer, Integer> sourceMap = jetInstance.getMap(sourceName);
        range(0, ENTRY_COUNT).forEach(i -> sourceMap.put(i, i));

        DAG dag = new DAG();
        Vertex source = dag.newVertex("source", readMap(sourceName));
        Vertex sink = dag.newVertex("sink", writeMap(sinkName));

        dag.edge(between(source, sink));

        jetInstance.newJob(dag).execute().get();

        assertEquals(ENTRY_COUNT, jetInstance.getMap(sinkName).size());
    }

    @Test
    public void testCache() throws ExecutionException, InterruptedException {
        ICache<Integer, Integer> sourceCache = getCache(jetInstance, sourceName);
        range(0, ENTRY_COUNT).forEach(i -> sourceCache.put(i, i));

        DAG dag = new DAG();
        Vertex source = dag.newVertex("source", readCache(sourceName));
        Vertex sink = dag.newVertex("sink", writeCache(sinkName));

        dag.edge(between(source, sink));

        jetInstance.newJob(dag).execute().get();

        assertEquals(ENTRY_COUNT, getCache(jetInstance, sinkName).size());
    }

    @Test
    public void testList() throws ExecutionException, InterruptedException {
        IStreamList<Integer> list = jetInstance.getList(sourceName);
        list.addAll(range(0, ENTRY_COUNT).boxed().collect(toList()));

        DAG dag = new DAG();
        Vertex source = dag.newVertex("source", readList(sourceName)).localParallelism(1);
        Vertex sink = dag.newVertex("sink", writeList(sinkName)).localParallelism(1);

        dag.edge(between(source, sink));

        jetInstance.newJob(dag).execute().get();

        assertEquals(ENTRY_COUNT, jetInstance.getList(sinkName).size());
    }

    private static <K, V> ICache<K, V> getCache(JetInstance jetInstance, String name) {
        return jetInstance.getHazelcastInstance().getCacheManager().getCache(name);
    }

}
