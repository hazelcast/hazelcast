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

package com.hazelcast.client.statistics;

import com.hazelcast.cache.ICache;
import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.ClientEngineImpl;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.statistics.Statistics;
import com.hazelcast.client.test.ClientTestSupport;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICacheManager;
import com.hazelcast.core.IMap;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.CacheManager;
import javax.cache.spi.CachingProvider;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientStatisticsTest
        extends ClientTestSupport {
    private static final String testMapName = "StatTestMapFirst.First";
    private static final String testCacheName = "StatTestICache,First";
    private static final int statsPeriodSeconds = 1;

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    @After
    public void cleanup() {
        hazelcastFactory.terminateAll();
    }

    @Test
    public void testStatisticsCollectionNonDefaultPeriod()
            throws Exception {
        HazelcastInstance hazelcastInstance = hazelcastFactory.newHazelcastInstance();

        HazelcastClientInstanceImpl client = createHazelcastClient();

        long clientConnectionTime = System.currentTimeMillis();

        ClientEngineImpl clientEngine = getClientEngineImpl(hazelcastInstance);

        // Wait enough time for statistics collection
        sleepSeconds(statsPeriodSeconds + 1);
        Map<String, String> stats = getStats(client, clientEngine);

        String connStat = stats.get("clusterConnectionTimestamp");
        assertNotNull(connStat);
        Long connectionTimeStat = Long.valueOf(connStat);
        // time measured by us after client connection should be greater than the connection time reported by the statistics and
        // the difference should not be more than a statistics collection period
        Assert.assertTrue(clientConnectionTime >= connectionTimeStat
                && clientConnectionTime - connectionTimeStat < statsPeriodSeconds * 1000);

        String queueSize = stats.get("executionService.userExecutorQueueSize");
        assertNotNull(queueSize);

        String mapHits = stats.get("nc." + testMapName + ".hits");
        assertNull(mapHits);
        String cacheHits = stats.get("nc." + testCacheName + ".hits");
        assertNull(cacheHits);

        IMap<Integer, Integer> map = client.getMap(testMapName);

        // Wait enough time for statistics collection
        sleepSeconds(statsPeriodSeconds + 1);

        stats = getStats(client, clientEngine);
        mapHits = stats.get("nc." + testMapName + ".hits");
        assertNotNull(mapHits);
        assertEquals("0", mapHits);
        cacheHits = stats.get("nc.hz/" + testCacheName + ".hits");
        assertNull(cacheHits);

        // produce map and cache stat
        produceSomeStats(hazelcastInstance, client, map);

        // Wait enough time for statistics collection
        sleepSeconds(statsPeriodSeconds + 1);

        stats = getStats(client, clientEngine);
        mapHits = stats.get("nc." + testMapName + ".hits");
        assertNotNull(mapHits);
        assertEquals("1", mapHits);
        cacheHits = stats.get("nc.hz/" + testCacheName + ".hits");
        assertNotNull(cacheHits);
        assertEquals("1", cacheHits);
    }

    @Test
    public void testStatisticsPeriod() {
        HazelcastInstance hazelcastInstance = hazelcastFactory.newHazelcastInstance();

        HazelcastClientInstanceImpl client = createHazelcastClient();

        ClientEngineImpl clientEngine = getClientEngineImpl(hazelcastInstance);

        // Wait enough time for statistics collection
        sleepSeconds(statsPeriodSeconds + 1);

        Map<String, String> initialStats = getStats(client, clientEngine);

        IMap<Integer, Integer> map = client.getMap(testMapName);

        // produce map and cache stat
        produceSomeStats(hazelcastInstance, client, map);

        // Wait enough time for statistics collection
        sleepSeconds(statsPeriodSeconds + 1);

        assertNotEquals(initialStats, getStats(client, clientEngine));
    }

    @Test
    public void testStatisticsClusterReconnect() {
        HazelcastInstance hazelcastInstance = hazelcastFactory.newHazelcastInstance();

        HazelcastClientInstanceImpl client = createHazelcastClient();

        hazelcastInstance.getLifecycleService().terminate();

        final CountDownLatch latch = new CountDownLatch(1);
        client.getLifecycleService().addLifecycleListener(new LifecycleListener() {
            @Override
            public void stateChanged(LifecycleEvent event) {
                if (LifecycleEvent.LifecycleState.CLIENT_CONNECTED.equals(event.getState())) {
                    latch.countDown();
                }
            }
        });

        hazelcastInstance = hazelcastFactory.newHazelcastInstance();

        assertOpenEventually(latch);

        // Wait enough time for statistics collection
        sleepSeconds(statsPeriodSeconds + 1);

        ClientEngineImpl clientEngine = getClientEngineImpl(hazelcastInstance);
        getStats(client, clientEngine);
    }

    @Test
    public void testStatisticsTwoClients() {
        HazelcastInstance hazelcastInstance = hazelcastFactory.newHazelcastInstance();

        HazelcastClientInstanceImpl client1 = createHazelcastClient();

        HazelcastClientInstanceImpl client2 = createHazelcastClient();

        ClientEngineImpl clientEngine = getClientEngineImpl(hazelcastInstance);

        // Wait enough time for statistics collection
        sleepSeconds(statsPeriodSeconds + 1);

        Map<String, String> clientStatistics = clientEngine.getClientStatistics();
        assertNotNull(clientStatistics);
        assertEquals(2, clientStatistics.size());
        List<String> expectedUUIDs = new ArrayList<String>(2);
        expectedUUIDs.add(client1.getClientClusterService().getLocalClient().getUuid());
        expectedUUIDs.add(client2.getClientClusterService().getLocalClient().getUuid());
        for (Map.Entry<String, String> clientEntry : clientStatistics.entrySet()) {
            Assert.assertTrue(expectedUUIDs.contains(clientEntry.getKey()));
            String stats = clientEntry.getValue();
            assertNotNull(stats);
            expectedUUIDs.remove(clientEntry.getKey());
        }
    }

    @Test
    public void testNoUpdateWhenDisabled() {
        HazelcastInstance hazelcastInstance = hazelcastFactory.newHazelcastInstance();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setProperty(Statistics.ENABLED.getName(), "false");
        clientConfig.setProperty(Statistics.PERIOD_SECONDS.getName(), Integer.toString(statsPeriodSeconds));

        HazelcastInstance clientInstance = hazelcastFactory.newHazelcastClient(clientConfig);

        // Wait enough time for statistics collection
        sleepSeconds(statsPeriodSeconds + 1);

        ClientEngineImpl clientEngine = getClientEngineImpl(hazelcastInstance);
        Map<String, String> statistics = clientEngine.getClientStatistics();
        assertEquals(0, statistics.size());
    }

    @Test
    public void testEscapeSpecialCharacter() {
        String originalString = "stat1=value1.lastName,stat2=value2\\hello==";
        String escapedString = "stat1\\=value1\\.lastName\\,stat2\\=value2\\\\hello\\=\\=";
        StringBuilder buffer = new StringBuilder(originalString);
        Statistics.escapeSpecialCharacters(buffer);
        assertEquals(escapedString, buffer.toString());
        assertEquals(originalString, Statistics.unescapeSpecialCharacters(escapedString));
    }

    @Test
    public void testSplit() {
        String escapedString = "stat1=value1.lastName,stat2=full\\name==hazel\\,ali,";
        String[] expectedStrings = { "stat1=value1.lastName", "stat2=full\\name==hazel\\,ali"};
        List<String> strings = Statistics.split(escapedString);
        assertArrayEquals(expectedStrings, strings.toArray());
    }

    private <K, V> CacheConfig<K, V> createCacheConfig() {
        CacheConfig<K, V> cacheConfig = new CacheConfig<K, V>();
        cacheConfig.setInMemoryFormat(InMemoryFormat.BINARY);
        return cacheConfig;
    }

    private CachingProvider getCachingProvider(HazelcastInstance instance) {
        return HazelcastServerCachingProvider.createCachingProvider(instance);
    }

    private void produceSomeStats(HazelcastInstance hazelcastInstance, HazelcastClientInstanceImpl client,
                                  IMap<Integer, Integer> map) {
        map.put(5, 10);
        assertEquals(10, map.get(5).intValue());
        assertEquals(10, map.get(5).intValue());
        ICache<Integer, Integer> cache = createCache(hazelcastInstance, testCacheName, client);
        cache.put(9, 20);
        assertEquals(20, cache.get(9).intValue());
        assertEquals(20, cache.get(9).intValue());
    }

    private HazelcastClientInstanceImpl createHazelcastClient() {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setProperty(Statistics.ENABLED.getName(), "true");
        clientConfig.setProperty(Statistics.PERIOD_SECONDS.getName(), Integer.toString(statsPeriodSeconds));

        // Add map and icache with near cache config
        clientConfig.addNearCacheConfig(new NearCacheConfig(testMapName));
        clientConfig.addNearCacheConfig(new NearCacheConfig(testCacheName));

        HazelcastInstance clientInstance = hazelcastFactory.newHazelcastClient(clientConfig);
        return getHazelcastClientInstanceImpl(clientInstance);
    }

    private ICache<Integer, Integer> createCache(HazelcastInstance hazelcastInstance, String testCacheName,
                                                 HazelcastInstance clientInstance) {
        CachingProvider cachingProvider = getCachingProvider(hazelcastInstance);
        CacheManager cacheManager = cachingProvider.getCacheManager();
        cacheManager.createCache(testCacheName, this.createCacheConfig());
        ICacheManager clientCacheManager = clientInstance.getCacheManager();
        return clientCacheManager.getCache(testCacheName);
    }

    private Map<String, String> getStats(HazelcastClientInstanceImpl client, ClientEngineImpl clientEngine) {
        Map<String, String> clientStatistics = clientEngine.getClientStatistics();
        assertNotNull(clientStatistics);
        assertEquals(1, clientStatistics.size());
        Set<Map.Entry<String, String>> entries = clientStatistics.entrySet();
        Map.Entry<String, String> statEntry = entries.iterator().next();
        assertEquals(client.getClientClusterService().getLocalClient().getUuid(), statEntry.getKey());
        return parseStatValue(statEntry.getValue());
    }

    private Map<String,String> parseStatValue(String value) {
        Map<String,String> result = new HashMap<String, String>();

        List<String> strings = Statistics.split(value);

        for (String stat : strings) {
            List<String> keyValue = Statistics.split(stat, 0, '=');
            result.put(Statistics.unescapeSpecialCharacters(keyValue.get(0)),
                    Statistics.unescapeSpecialCharacters(keyValue.get(1)));
        }

        return result;
    }
}
