/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.IMap;
import com.hazelcast.jet.IMapJet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.JetTestInstanceFactory;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JetClientConfig;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.impl.JetClientInstanceImpl;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toMap;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
public class ImdgUtilTest extends JetTestSupport {

    private static final String NEAR_CACHED_SERIALIZED_MAP = "nearCachedSerialized";
    private static final String NEAR_CACHED_NON_SERIALIZED_MAP = "nearCachedNonSerialized";

    private static JetTestInstanceFactory factory = new JetTestInstanceFactory();
    private static JetInstance instance1;
    private static JetInstance instance2;
    private static JetClientInstanceImpl client;

    @BeforeClass
    public static void setupCluster() {
        JetConfig jetConfig = new JetConfig();
        Config hzConfig = jetConfig.getHazelcastConfig();
        hzConfig.getMapConfig(NEAR_CACHED_SERIALIZED_MAP).setNearCacheConfig(
                new NearCacheConfig().setInMemoryFormat(InMemoryFormat.BINARY)
        );
        hzConfig.getMapConfig(NEAR_CACHED_NON_SERIALIZED_MAP).setNearCacheConfig(
                new NearCacheConfig().setInMemoryFormat(InMemoryFormat.OBJECT)
        );
        instance1 = factory.newMember(jetConfig);
        instance2 = factory.newMember(jetConfig);

        ClientConfig clientConfig = new JetClientConfig();
        clientConfig.addNearCacheConfig(new NearCacheConfig(NEAR_CACHED_SERIALIZED_MAP)
                .setInMemoryFormat(InMemoryFormat.BINARY));
        clientConfig.addNearCacheConfig(new NearCacheConfig(NEAR_CACHED_NON_SERIALIZED_MAP)
                .setInMemoryFormat(InMemoryFormat.OBJECT));
        client = factory.newClient(clientConfig);
    }

    @AfterClass
    public static void tearDown() {
        factory.terminateAll();
        factory = null;
    }

    @After
    public void after() {
        for (Job job : instance1.getJobs()) {
            ditchJob(job, instance1, instance2);
        }
        for (DistributedObject o : instance1.getHazelcastInstance().getDistributedObjects()) {
            o.destroy();
        }
    }

    @Test
    public void test_copyMap() throws Exception {
        logger.info("Populating source map...");
        IMapJet<Object, Object> srcMap = instance1.getMap("src");
        Map<Integer, Integer> testData = IntStream.range(0, 100_000).boxed().collect(toMap(e -> e, e -> e));
        srcMap.putAll(testData);

        logger.info("Copying using job...");
        Util.copyMapUsingJob(instance1, 128, srcMap.getName(), "target").get();
        logger.info("Done copying");

        assertEquals(testData, new HashMap<>(instance1.getMap("target")));
    }

    @Test
    public void mapPutAllAsync_noNearCache_member() throws Exception {
        IMap<Object, Object> map = instance1.getHazelcastInstance().getMap(randomMapName());
        Map<String, String> tmpMap = new HashMap<>();
        tmpMap.put("k1", "v1");
        tmpMap.put("k2", "v1");
        ImdgUtil.mapPutAllAsync(map, tmpMap)
                .toCompletableFuture().get();

        assertEquals(tmpMap, new HashMap<>(map));
    }

    @Test
    public void mapPutAllAsync_noNearCache_client() throws Exception {
        IMap<Object, Object> map = client.getHazelcastInstance().getMap(randomMapName());
        Map<String, String> tmpMap = new HashMap<>();
        tmpMap.put("k1", "v1");
        tmpMap.put("k2", "v1");
        ImdgUtil.mapPutAllAsync(map, tmpMap)
                .toCompletableFuture().get();

        assertEquals(tmpMap, new HashMap<>(map));
    }

    @Test
    public void mapPutAllAsync_large_member() throws Exception {
        IMap<Object, Object> map = instance1.getHazelcastInstance().getMap(randomMapName());
        Map<Integer, Integer> tmpMap = new HashMap<>();
        for (int i = 0; i < 32_768; i++) {
            tmpMap.put(i, i);
        }
        ImdgUtil.mapPutAllAsync(map, tmpMap)
                .toCompletableFuture().get();

        assertEquals(tmpMap, new HashMap<>(map));
    }

    @Test
    public void mapPutAllAsync_large_client() throws Exception {
        IMap<Object, Object> map = client.getHazelcastInstance().getMap(randomMapName());
        Map<Integer, Integer> tmpMap = new HashMap<>();
        for (int i = 0; i < 32_768; i++) {
            tmpMap.put(i, i);
        }
        ImdgUtil.mapPutAllAsync(map, tmpMap)
                .toCompletableFuture().get();

        assertEquals(tmpMap, new HashMap<>(map));
    }

    @Test
    public void mapPutAllAsync_withNearCache_serialized_member() throws Exception {
        IMap<Object, Object> map = instance1.getHazelcastInstance().getMap(NEAR_CACHED_SERIALIZED_MAP);
        map.put("key", "value");
        map.get("key"); // populate the near cache

        ImdgUtil.mapPutAllAsync(map, Collections.singletonMap("key", "newValue"))
                .toCompletableFuture().get();

        assertEquals("newValue", map.get("key"));
    }

    @Test
    public void mapPutAllAsync_withNearCache_nonSerialized_member() throws Exception {
        IMap<Object, Object> map = instance1.getHazelcastInstance().getMap(NEAR_CACHED_NON_SERIALIZED_MAP);
        map.put("key", "value");
        map.get("key"); // populate the near cache

        ImdgUtil.mapPutAllAsync(map, Collections.singletonMap("key", "newValue"))
            .toCompletableFuture().get();

        assertEquals("newValue", map.get("key"));
    }

    @Test
    public void mapPutAllAsync_withNearCache_serialized_client() throws Exception {
        IMap<Object, Object> map = client.getHazelcastInstance().getMap(NEAR_CACHED_SERIALIZED_MAP);
        map.put("key", "value");
        map.get("key"); // populate the near cache

        ImdgUtil.mapPutAllAsync(map, Collections.singletonMap("key", "newValue"))
            .toCompletableFuture().get();

        assertEquals("newValue", map.get("key"));
    }

    @Test
    public void mapPutAllAsync_withNearCache_nonSerialized_client() throws Exception {
        IMap<Object, Object> map = client.getHazelcastInstance().getMap(NEAR_CACHED_NON_SERIALIZED_MAP);
        map.put("key", "value");
        map.get("key"); // populate the near cache

        ImdgUtil.mapPutAllAsync(map, Collections.singletonMap("key", "newValue"))
            .toCompletableFuture().get();

        assertEquals("newValue", map.get("key"));
    }
}
