/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.util;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.config.EdgeConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.map.IMap;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;

import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.processor.SinkProcessors.writeMapP;
import static com.hazelcast.jet.core.processor.SourceProcessors.readMapP;
import static java.util.stream.Collectors.toMap;
import static org.junit.Assert.assertEquals;

@Category({QuickTest.class, ParallelJVMTest.class})
public class ImdgUtilTest extends SimpleTestInClusterSupport {

    private static final String NEAR_CACHED_SERIALIZED_MAP = "nearCachedSerialized";
    private static final String NEAR_CACHED_NON_SERIALIZED_MAP = "nearCachedNonSerialized";

    @BeforeClass
    public static void setupCluster() {
        Config config = new Config();
        config.getJetConfig().setEnabled(true);
        config.getMapConfig(NEAR_CACHED_SERIALIZED_MAP).setNearCacheConfig(
                new NearCacheConfig().setInMemoryFormat(InMemoryFormat.BINARY)
        );
        config.getMapConfig(NEAR_CACHED_NON_SERIALIZED_MAP).setNearCacheConfig(
                new NearCacheConfig().setInMemoryFormat(InMemoryFormat.OBJECT)
        );

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.addNearCacheConfig(new NearCacheConfig(NEAR_CACHED_SERIALIZED_MAP)
                .setInMemoryFormat(InMemoryFormat.BINARY));
        clientConfig.addNearCacheConfig(new NearCacheConfig(NEAR_CACHED_NON_SERIALIZED_MAP)
                .setInMemoryFormat(InMemoryFormat.OBJECT));

        initializeWithClient(2, config, clientConfig);
    }

    @Test
    public void test_copyMap() throws Exception {
        logger.info("Populating source map...");
        IMap<Object, Object> srcMap = instance().getMap("src");
        Map<Integer, Integer> testData = IntStream.range(0, 100_000).boxed().collect(toMap(e -> e, e -> e));
        srcMap.putAll(testData);

        logger.info("Copying using job...");
        copyMapUsingJob(instance(), 128, srcMap.getName(), "target").get();
        logger.info("Done copying");

        assertEquals(testData, new HashMap<>(instance().getMap("target")));
    }

    private static CompletableFuture<Void> copyMapUsingJob(HazelcastInstance instance, int queueSize,
                                                           String sourceMap, String targetMap) {
        DAG dag = new DAG();
        Vertex source = dag.newVertex("readMap(" + sourceMap + ')', readMapP(sourceMap));
        Vertex sink = dag.newVertex("writeMap(" + targetMap + ')', writeMapP(targetMap));
        dag.edge(between(source, sink).setConfig(new EdgeConfig().setQueueSize(queueSize)));
        JobConfig jobConfig = new JobConfig()
                .setName("copy-" + sourceMap + "-to-" + targetMap);
        return instance.getJet().newJob(dag, jobConfig).getFuture();
    }
}
