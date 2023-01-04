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

package com.hazelcast.client.map.impl.nearcache;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.ClientTestSupport;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.nearcache.NearCache;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.nearcache.TestReadOnlyProcessor;
import com.hazelcast.nearcache.NearCacheStats;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.PredicateBuilder;
import com.hazelcast.query.Predicates;
import com.hazelcast.query.SampleTestObjects;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;

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

    private TestHazelcastFactory factory = new TestHazelcastFactory();
    private String mapName = randomMapName();

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
    public void testMapSubmitToKey_withReadOnlyProcessor_noInvalidations() {
        Config config = getConfig();
        configureBatching(config, 10, 1);

        ClientConfig clientConfig = getClientConfig(mapName);

        HazelcastInstance server = factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);

        HazelcastInstance client = factory.newHazelcastClient(clientConfig);
        makeSureConnectedToServers(client, 1);

        IMap<Integer, Integer> serverMap = server.getMap(mapName);
        IMap<Integer, Integer> clientMap = client.getMap(mapName);

        int size = 1000;

        // fill serverMap
        for (int i = 0; i < size; i++) {
            serverMap.put(i, i);
        }

        NearCacheStats nearCachestats = ((NearCachedClientMapProxy) clientMap).getNearCache().getNearCacheStats();
        long invalidationsBefore = nearCachestats.getInvalidations();
        System.out.println(invalidationsBefore);
        // fill Near Cache on client
        for (int i = 0; i < size; i++) {
            clientMap.submitToKey(i, new TestReadOnlyProcessor());
        }

        long invalidationsAfter = nearCachestats.getInvalidations();
        System.out.println(invalidationsAfter);
        assertEquals("No Invalidation after getting keys from read only entry processor ", invalidationsBefore, invalidationsAfter);

    }

    @Test
    public void testMapExecuteOnEntries_noInvalidations() {
        Config config = getConfig();
        configureBatching(config, 10, 1);

        ClientConfig clientConfig = getClientConfig(mapName);

        HazelcastInstance server = factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);

        HazelcastInstance client = factory.newHazelcastClient(clientConfig);
        makeSureConnectedToServers(client, 1);

        int mapSize = 100;

        IMap<Integer, SampleTestObjects.Employee> map = server.getMap(mapName);
        IMap<Integer, SampleTestObjects.Employee> clientMap = client.getMap(mapName);

        for (int i = 0; i < mapSize; i++) {
            map.put(i, new SampleTestObjects.Employee(i, "", 0, true, 0D));
        }

        populateNearCache(map, mapSize);
        NearCacheStats nearCachestats = ((NearCachedClientMapProxy) clientMap).getNearCache().getNearCacheStats();
        long invalidationsBefore = nearCachestats.getInvalidations();
        PredicateBuilder.EntryObject e = Predicates.newPredicateBuilder().getEntryObject();
        Predicate predicate = e.get("salary").equal(0);
        map.executeOnEntries(
                entry -> {
                    SampleTestObjects.Employee employee = entry.getValue();
                    double currentSalary = employee.getSalary();
                    double newSalary = currentSalary + 10;
                    employee.setSalary(newSalary);
                    return newSalary;
                }, predicate);
        long invalidationsAfter = nearCachestats.getInvalidations();
//        assertEquals("No Invalidation after getting keys from read only entry processor ", hitsBefore, hitsAfter);
        assertEquals("No Invalidation after getting keys from read only entry processor ", invalidationsBefore, invalidationsAfter);

    }

    protected void populateNearCache(Map<Integer, ?> map, int mapSize) {
        for (int i = 0; i < mapSize; i++) {
            map.get(i);
        }
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

    private static void configureBatching(Config config, int batchSize, int period) {
        config.setProperty(MAP_INVALIDATION_MESSAGE_BATCH_ENABLED.getName(), "true");
        config.setProperty(MAP_INVALIDATION_MESSAGE_BATCH_SIZE.getName(), valueOf(batchSize));
        config.setProperty(MAP_INVALIDATION_MESSAGE_BATCH_FREQUENCY_SECONDS.getName(), valueOf(period));
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
