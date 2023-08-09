/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.proxy.ClientMapProxy;
import com.hazelcast.client.test.ClientTestSupport;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.EntryView;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.nearcache.NearCache;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.nearcache.TestReadOnlyProcessor;
import com.hazelcast.nearcache.NearCacheStats;
import com.hazelcast.query.Predicates;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.hazelcast.config.InMemoryFormat.OBJECT;
import static com.hazelcast.internal.nearcache.impl.NearCacheTestUtils.getBaseConfig;
import static com.hazelcast.spi.properties.ClusterProperty.MAP_INVALIDATION_MESSAGE_BATCH_ENABLED;
import static com.hazelcast.spi.properties.ClusterProperty.MAP_INVALIDATION_MESSAGE_BATCH_FREQUENCY_SECONDS;
import static com.hazelcast.spi.properties.ClusterProperty.MAP_INVALIDATION_MESSAGE_BATCH_SIZE;
import static java.lang.String.valueOf;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientMapNearCacheInvalidationTest extends ClientTestSupport {

    private final TestHazelcastFactory factory = new TestHazelcastFactory();
    private final String mapName = randomMapName();

    @After
    public void tearDown() {
        factory.shutdownAll();
    }

    @Test
    public void testBatchInvalidationRemovesEntries() {
        Config config = getConfig();
        configureBatching(config, 10, 1);

        ClientConfig clientConfig = getClientConfig(mapName);

        HazelcastInstance server = factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);

        HazelcastInstance client = factory.newHazelcastClient(clientConfig);
        makeSureConnectedToServers(client, 2);

        IMap<Integer, Integer> serverMap = server.getMap(mapName);
        IMap<Integer, Integer> clientMap = client.getMap(mapName);

        int size = 1000;

        // fill serverMap
        for (int i = 0; i < size; i++) {
            serverMap.put(i, i);
        }

        // fill Near Cache on client
        for (int i = 0; i < size; i++) {
            clientMap.get(i);
        }

        // generate invalidation data
        for (int i = 0; i < size; i++) {
            serverMap.put(i, i);
        }

        assertNearCacheSizeEventually(clientMap, 0);
    }

    @Test
    public void testHigherBatchSize_shouldNotCauseAnyInvalidation_onClient() {
        Config config = getConfig();
        configureBatching(config, Integer.MAX_VALUE, Integer.MAX_VALUE);

        ClientConfig clientConfig = getClientConfig(mapName);

        HazelcastInstance server = factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);

        HazelcastInstance client = factory.newHazelcastClient(clientConfig);
        makeSureConnectedToServers(client, 2);

        IMap<Integer, Integer> serverMap = server.getMap(mapName);
        IMap<Integer, Integer> clientMap = client.getMap(mapName);

        int size = 1000;

        // fill serverMap
        for (int i = 0; i < size; i++) {
            serverMap.put(i, i);
        }

        // fill Near Cache on client
        for (int i = 0; i < size; i++) {
            clientMap.get(i);
        }

        // generate invalidation data
        for (int i = 0; i < size; i++) {
            serverMap.put(i, i);
        }

        assertNearCacheSizeEventually(clientMap, size);
    }

    @Test
    public void testMapClear_shouldClearNearCaches_onOwnerAndBackupNodes() {
        Config config = getConfig();
        configureBatching(config, 10, 1);

        ClientConfig clientConfig = getClientConfig(mapName);

        HazelcastInstance server = factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);

        HazelcastInstance client = factory.newHazelcastClient(clientConfig);
        makeSureConnectedToServers(client, 2);

        IMap<Integer, Integer> serverMap = server.getMap(mapName);
        IMap<Integer, Integer> clientMap = client.getMap(mapName);

        int size = 1000;

        // fill serverMap
        for (int i = 0; i < size; i++) {
            serverMap.put(i, i);
        }

        // fill Near Cache on client
        for (int i = 0; i < size; i++) {
            clientMap.get(i);
        }

        serverMap.clear();

        assertNearCacheSizeEventually(clientMap, 0);
    }

    @Test
    public void testPutAllWithMetadata_shouldClearNearCaches_onOwnerAndBackupNodes() {
        Config config = getConfig();
        configureBatching(config, 10, 1);

        ClientConfig clientConfig = getClientConfig(mapName);

        HazelcastInstance server = factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);

        HazelcastInstance client = factory.newHazelcastClient(clientConfig);
        makeSureConnectedToServers(client, 2);

        IMap<Integer, Integer> serverMap = server.getMap(mapName);
        IMap<Integer, Integer> clientMap = client.getMap(mapName);

        int size = 1000;

        // fill serverMap
        for (int i = 0; i < size; i++) {
            serverMap.put(i, i);
        }

        List<EntryView<Integer, Integer>> entries = new ArrayList<>(size);

        // fill Near Cache on client
        for (int i = 0; i < size; i++) {
            clientMap.get(i);
            entries.add(clientMap.getEntryView(i));
        }

        ((ClientMapProxy<Integer, Integer>) clientMap).putAllWithMetadataAsync(entries);

        assertNearCacheSizeEventually(clientMap, 0);
    }

    @Test
    public void testPutAll_shouldClearNearCaches_onOwnerAndBackupNodes() {
        Config config = getConfig();
        configureBatching(config, 10, 1);

        ClientConfig clientConfig = getClientConfig(mapName);

        HazelcastInstance server = factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);

        HazelcastInstance client = factory.newHazelcastClient(clientConfig);
        makeSureConnectedToServers(client, 2);

        IMap<Integer, Integer> serverMap = server.getMap(mapName);
        IMap<Integer, Integer> clientMap = client.getMap(mapName);

        int size = 1000;

        // fill serverMap
        for (int i = 0; i < size; i++) {
            serverMap.put(i, i);
        }

        Map<Integer, Integer> entries = new HashMap<>(size);

        // fill Near Cache on client
        for (int i = 0; i < size; i++) {
            clientMap.get(i);
            entries.put(i, i);
        }

        clientMap.putAll(entries);

        assertNearCacheSizeEventually(clientMap, 0);
    }

    @Test
    public void testMapSubmitToKey_withReadOnlyProcessor_noInvalidations() {
        verifyNoInvalidationsWith((map, size) -> {
            for (int i = 0; i < size; i++) {
                map.submitToKey(i, new TestReadOnlyProcessor());
            }
        });
    }

    @Test
    public void testMapExecuteOnKey_withReadOnlyProcessor_noInvalidations() {
        verifyNoInvalidationsWith((map, size) -> {
            for (int i = 0; i < size; i++) {
                map.executeOnKey(i, new TestReadOnlyProcessor());
            }
        });
    }

    @Test
    public void testMapSubmitToKeys_withReadOnlyProcessor_noInvalidations() {
        verifyNoInvalidationsWith((map, size) -> {
            Set<Integer> keys = IntStream.range(0, size).boxed().collect(Collectors.toSet());
            map.submitToKeys(keys, new TestReadOnlyProcessor());
        });
    }

    @Test
    public void testMapExecuteOnKeys_withReadOnlyProcessor_noInvalidations() {
        verifyNoInvalidationsWith((map, size) -> {
            Set<Integer> keys = IntStream.range(0, size).boxed().collect(Collectors.toSet());
            map.executeOnKeys(keys, new TestReadOnlyProcessor());
        });
    }

    @Test
    public void testMapExecuteOnEntriesWithPredicate_withReadOnlyProcessor_noInvalidations() {
        verifyNoInvalidationsWith((map, size) -> {
            map.executeOnEntries(new TestReadOnlyProcessor(), Predicates.alwaysTrue());
        });
    }

    @Test
    public void testMapExecuteOnEntriesWithoutPredicate_withReadOnlyProcessor_noInvalidations() {
        verifyNoInvalidationsWith((map, size) -> {
            map.executeOnEntries(new TestReadOnlyProcessor());
        });
    }

    @Test
    public void testMapEvictAll_shouldClearNearCaches_onOwnerAndBackupNodes() {
        Config config = getConfig();
        configureBatching(config, 10, 1);

        ClientConfig clientConfig = getClientConfig(mapName);

        HazelcastInstance server = factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);

        HazelcastInstance client = factory.newHazelcastClient(clientConfig);
        makeSureConnectedToServers(client, 2);

        IMap<Integer, Integer> serverMap = server.getMap(mapName);
        IMap<Integer, Integer> clientMap = client.getMap(mapName);

        int size = 1000;

        // fill serverMap
        for (int i = 0; i < size; i++) {
            serverMap.put(i, i);
        }

        // fill Near Cache on client
        for (int i = 0; i < size; i++) {
            clientMap.get(i);
        }

        serverMap.evictAll();

        assertNearCacheSizeEventually(clientMap, 0);
    }

    @Override
    protected Config getConfig() {
        return getBaseConfig();
    }

    protected ClientConfig getClientConfig(String mapName) {
        NearCacheConfig nearCacheConfig = getNearCacheConfig(mapName);

        return new ClientConfig()
                .addNearCacheConfig(nearCacheConfig);
    }

    protected NearCacheConfig getNearCacheConfig(String mapName) {
        return new NearCacheConfig(mapName)
                    .setInMemoryFormat(OBJECT)
                    .setInvalidateOnChange(true);
    }

    private void verifyNoInvalidationsWith(BiConsumer<IMap<Integer, Integer>, Integer> operation) {
        Config config = getConfig();

        factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);

        ClientConfig clientConfig = getClientConfig(mapName);
        HazelcastInstance client = factory.newHazelcastClient(clientConfig);

        makeSureConnectedToServers(client, 2);

        int size = 100;

        IMap<Integer, Integer> map = client.getMap(mapName);

        for (int i = 0; i < size; i++) {
            map.put(i, i);
            map.get(i);
        }

        long invalidationsBefore = getInvalidationsFrom(map);

        operation.accept(map, size);

        assertTrueAllTheTime(() -> {
            long invalidationsAfter = getInvalidationsFrom(map);
            assertEquals(invalidationsBefore, invalidationsAfter);
        }, 3);
    }

    private static void configureBatching(Config config, int batchSize, int period) {
        config.setProperty(MAP_INVALIDATION_MESSAGE_BATCH_ENABLED.getName(), "true");
        config.setProperty(MAP_INVALIDATION_MESSAGE_BATCH_SIZE.getName(), valueOf(batchSize));
        config.setProperty(MAP_INVALIDATION_MESSAGE_BATCH_FREQUENCY_SECONDS.getName(), valueOf(period));
    }

    private static long getInvalidationsFrom(IMap<Integer, Integer> map) {
        NearCacheStats stats = ((NearCachedClientMapProxy<Integer, Integer>) map).getNearCache().getNearCacheStats();
        return stats.getInvalidations();
    }

    private static void assertNearCacheSizeEventually(final IMap map, final int nearCacheSize) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                NearCache nearCache = ((NearCachedClientMapProxy) map).getNearCache();

                assertEquals(nearCacheSize, nearCache.size());
            }
        });
    }
}
