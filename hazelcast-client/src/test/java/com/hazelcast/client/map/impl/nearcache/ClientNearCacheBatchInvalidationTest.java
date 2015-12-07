
/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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


package com.hazelcast.client.map.impl.nearcache;

import com.hazelcast.cache.impl.nearcache.NearCache;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.proxy.NearCachedClientMapProxy;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.config.InMemoryFormat.OBJECT;
import static com.hazelcast.instance.GroupProperty.MAP_INVALIDATION_MESSAGE_BATCH_ENABLED;
import static com.hazelcast.instance.GroupProperty.MAP_INVALIDATION_MESSAGE_BATCH_FREQUENCY_SECONDS;
import static com.hazelcast.instance.GroupProperty.MAP_INVALIDATION_MESSAGE_BATCH_SIZE;
import static java.lang.String.valueOf;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientNearCacheBatchInvalidationTest extends HazelcastTestSupport {

    protected TestHazelcastFactory factory = new TestHazelcastFactory();
    protected String mapName = randomMapName();

    @After
    public void tearDown() throws Exception {
        factory.shutdownAll();
    }

    @Test
    public void testBatchInvalidationRemovesEntries() throws Exception {
        Config config = getConfig();
        configureBatching(config, true, 10, 1);
        HazelcastInstance server = factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);

        ClientConfig clientConfig = newClientConfig(mapName);

        HazelcastInstance client = factory.newHazelcastClient(clientConfig);

        IMap<Integer, Integer> serverMap = server.getMap(mapName);
        IMap<Integer, Integer> clientMap = client.getMap(mapName);

        int size = 1000;

        // fill serverMap
        for (int i = 0; i < size; i++) {
            serverMap.put(i, i);
        }

        // fill near-cache on client
        for (int i = 0; i < size; i++) {
            clientMap.get(i);
        }

        // generate invalidation data.
        for (int i = 0; i < size; i++) {
            serverMap.put(i, i);
        }

        assertNearCacheSizeEventually(clientMap, 0);

    }

    @Test
    public void testHigherBatchSize_shouldNotCauseAnyInvalidation_onClient() throws Exception {
        Config config = getConfig();
        configureBatching(config, true, Integer.MAX_VALUE, Integer.MAX_VALUE);
        HazelcastInstance server = factory.newHazelcastInstance(config);

        factory.newHazelcastInstance(config);

        ClientConfig clientConfig = newClientConfig(mapName);

        HazelcastInstance client = factory.newHazelcastClient(clientConfig);

        IMap<Integer, Integer> serverMap = server.getMap(mapName);
        IMap<Integer, Integer> clientMap = client.getMap(mapName);

        int size = 1000;

        // fill serverMap
        for (int i = 0; i < size; i++) {
            serverMap.put(i, i);
        }

        // fill near-cache on client
        for (int i = 0; i < size; i++) {
            clientMap.get(i);
        }

        // generate invalidation data.
        for (int i = 0; i < size; i++) {
            serverMap.put(i, i);
        }

        assertNearCacheSizeEventually(clientMap, size);
    }

    @Test
    public void testMapClear_shouldClearNearCaches_onOwnerAndBackupNodes() throws Exception {
        Config config = getConfig();
        configureBatching(config, true, 10, 1);
        HazelcastInstance server = factory.newHazelcastInstance(config);

        factory.newHazelcastInstance(config);

        ClientConfig clientConfig = newClientConfig(mapName);

        HazelcastInstance client = factory.newHazelcastClient(clientConfig);

        IMap<Integer, Integer> serverMap = server.getMap(mapName);
        IMap<Integer, Integer> clientMap = client.getMap(mapName);

        int size = 1000;

        // fill serverMap
        for (int i = 0; i < size; i++) {
            serverMap.put(i, i);
        }

        // fill near-cache on client
        for (int i = 0; i < size; i++) {
            clientMap.get(i);
        }

        serverMap.clear();

        assertNearCacheSizeEventually(clientMap, 0);
    }


    @Test
    public void testMapEvictAll_shouldClearNearCaches_onOwnerAndBackupNodes() throws Exception {
        Config config = getConfig();
        configureBatching(config, true, 10, 1);
        HazelcastInstance server = factory.newHazelcastInstance(config);

        factory.newHazelcastInstance(config);

        ClientConfig clientConfig = newClientConfig(mapName);

        HazelcastInstance client = factory.newHazelcastClient(clientConfig);

        IMap<Integer, Integer> serverMap = server.getMap(mapName);
        IMap<Integer, Integer> clientMap = client.getMap(mapName);

        int size = 1000;

        // fill serverMap
        for (int i = 0; i < size; i++) {
            serverMap.put(i, i);
        }

        // fill near-cache on client
        for (int i = 0; i < size; i++) {
            clientMap.get(i);
        }

        serverMap.evictAll();

        assertNearCacheSizeEventually(clientMap, 0);
    }

    protected ClientConfig newClientConfig(String mapName) {
        NearCacheConfig nearCacheConfig = new NearCacheConfig();
        nearCacheConfig.setInMemoryFormat(getNearCacheInMemoryFormat());
        nearCacheConfig.setName(mapName);
        nearCacheConfig.setInvalidateOnChange(true);
        nearCacheConfig.setCacheLocalEntries(true);


        ClientConfig clientConfig = new ClientConfig();
        clientConfig.addNearCacheConfig(nearCacheConfig);

        return clientConfig;
    }

    protected InMemoryFormat getNearCacheInMemoryFormat() {
        return OBJECT;
    }

    protected void configureBatching(Config config, boolean enableBatching, int batchSize, int period) {
        config.setProperty(MAP_INVALIDATION_MESSAGE_BATCH_ENABLED, valueOf(enableBatching));
        config.setProperty(MAP_INVALIDATION_MESSAGE_BATCH_SIZE, valueOf(batchSize));
        config.setProperty(MAP_INVALIDATION_MESSAGE_BATCH_FREQUENCY_SECONDS, valueOf(period));
    }

    protected void assertNearCacheSizeEventually(final IMap map, final int nearCacheSize) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                NearCache nearCache = ((NearCachedClientMapProxy) map).getNearCache();

                assertEquals(nearCacheSize, nearCache.size());
            }
        });
    }

}