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
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
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

import static com.hazelcast.client.impl.statistics.Statistics.split;
import static com.hazelcast.client.impl.statistics.Statistics.unescapeSpecialCharacters;
import static java.lang.String.format;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientStatisticsTest extends ClientTestSupport {

    private static final int STATS_PERIOD_SECONDS = 1;
    private static final String MAP_NAME = "StatTestMapFirst.First";
    private static final String CACHE_NAME = "StatTestICache,First";
    private static final String MAP_HITS_KEY = "nc." + MAP_NAME + ".hits";
    private static final String CACHE_HITS_KEY = "nc.hz/" + CACHE_NAME + ".hits";

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    @After
    public void cleanup() {
        hazelcastFactory.terminateAll();
    }

    @Test
    public void testStatisticsCollectionNonDefaultPeriod() {
        HazelcastInstance hazelcastInstance = hazelcastFactory.newHazelcastInstance();
        HazelcastClientInstanceImpl client = createHazelcastClient();
        ClientEngineImpl clientEngine = getClientEngineImpl(hazelcastInstance);

        long clientConnectionTime = System.currentTimeMillis();

        // wait enough time for statistics collection
        waitForFirstStatisticsCollection(client, clientEngine);

        Map<String, String> stats = getStats(client, clientEngine);

        String connStat = stats.get("clusterConnectionTimestamp");
        assertNotNull(format("clusterConnectionTimestamp should not be null (%s)", stats), connStat);
        Long connectionTimeStat = Long.valueOf(connStat);
        assertNotNull(format("connectionTimeStat should not be null (%s)", stats), connStat);

        // time measured by us after client connection should be greater than the connection time reported by the statistics
        assertTrue(format("connectionTimeStat was %d, clientConnectionTime was %d (%s)",
                connectionTimeStat, clientConnectionTime, stats), clientConnectionTime >= connectionTimeStat);

        String queueSize = stats.get("executionService.userExecutorQueueSize");
        assertNotNull(format("executionService.userExecutorQueueSize should not be null (%s)", stats), queueSize);

        String mapHits = stats.get(MAP_HITS_KEY);
        assertNull(format("%s should be null (%s)", MAP_HITS_KEY, stats), mapHits);
        String cacheHits = stats.get(CACHE_HITS_KEY);
        assertNull(format("%s should be null (%s)", CACHE_HITS_KEY, stats), cacheHits);

        String lastStatisticsCollectionTimeString = stats.get("lastStatisticsCollectionTime");
        long lastCollectionTime = Long.parseLong(lastStatisticsCollectionTimeString);

        IMap<Integer, Integer> map = client.getMap(MAP_NAME);

        // wait enough time for statistics collection
        waitForNextStatsCollection(client, clientEngine, lastStatisticsCollectionTimeString);

        stats = getStats(client, clientEngine);
        mapHits = stats.get(MAP_HITS_KEY);
        assertNotNull(format("%s should not be null (%s)", MAP_HITS_KEY, stats), mapHits);
        assertEquals("Expected 0 map hits", "0", mapHits);
        cacheHits = stats.get(CACHE_HITS_KEY);
        assertNull(format("%s should be null (%s)", CACHE_HITS_KEY, stats), cacheHits);

        // verfiy that collection is periodic
        String newStatisticsCollectionTimeString = verifyThatCollectionIsPeriodic(stats, lastCollectionTime);

        lastStatisticsCollectionTimeString = newStatisticsCollectionTimeString;

        // produce map and cache stat
        produceSomeStats(hazelcastInstance, client, map);

        // wait enough time for statistics collection
        waitForNextStatsCollection(client, clientEngine, lastStatisticsCollectionTimeString);

        stats = getStats(client, clientEngine);
        mapHits = stats.get(MAP_HITS_KEY);
        assertNotNull(format("%s should not be null (%s)", MAP_HITS_KEY, stats), mapHits);
        assertEquals("Expected 1 map hits", "1", mapHits);
        cacheHits = stats.get(CACHE_HITS_KEY);
        assertNotNull(format("%s should not be null (%s)", CACHE_HITS_KEY, stats), cacheHits);
        assertEquals("Expected 1 cache hits", "1", cacheHits);
    }

    @Test
    public void testStatisticsPeriod() {
        HazelcastInstance hazelcastInstance = hazelcastFactory.newHazelcastInstance();
        HazelcastClientInstanceImpl client = createHazelcastClient();
        ClientEngineImpl clientEngine = getClientEngineImpl(hazelcastInstance);

        // wait enough time for statistics collection
        waitForFirstStatisticsCollection(client, clientEngine);

        Map<String, String> initialStats = getStats(client, clientEngine);

        IMap<Integer, Integer> map = client.getMap(MAP_NAME);

        // produce map and cache stat
        produceSomeStats(hazelcastInstance, client, map);

        // wait enough time for statistics collection
        waitForNextStatsCollection(client, clientEngine, initialStats.get("lastStatisticsCollectionTime"));

        assertNotEquals("initial statistics should not be the same as current stats",
                initialStats, getStats(client, clientEngine));
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
        ClientEngineImpl clientEngine = getClientEngineImpl(hazelcastInstance);

        assertOpenEventually(latch);

        // wait enough time for statistics collection
        waitForFirstStatisticsCollection(client, clientEngine);

        getStats(client, clientEngine);
    }

    @Test
    public void testStatisticsTwoClients() {
        HazelcastInstance hazelcastInstance = hazelcastFactory.newHazelcastInstance();
        HazelcastClientInstanceImpl client1 = createHazelcastClient();
        HazelcastClientInstanceImpl client2 = createHazelcastClient();
        ClientEngineImpl clientEngine = getClientEngineImpl(hazelcastInstance);

        // wait enough time for statistics collection
        sleepSeconds(STATS_PERIOD_SECONDS + 1);

        Map<String, String> clientStatistics = clientEngine.getClientStatistics();
        assertNotNull(clientStatistics);
        assertEquals(2, clientStatistics.size());
        List<String> expectedUUIDs = new ArrayList<String>(2);
        expectedUUIDs.add(client1.getClientClusterService().getLocalClient().getUuid());
        expectedUUIDs.add(client2.getClientClusterService().getLocalClient().getUuid());
        for (Map.Entry<String, String> clientEntry : clientStatistics.entrySet()) {
            assertTrue(expectedUUIDs.contains(clientEntry.getKey()));
            String stats = clientEntry.getValue();
            assertNotNull(stats);
            expectedUUIDs.remove(clientEntry.getKey());
        }
    }

    @Test
    public void testNoUpdateWhenDisabled() {
        HazelcastInstance hazelcastInstance = hazelcastFactory.newHazelcastInstance();
        final ClientEngineImpl clientEngine = getClientEngineImpl(hazelcastInstance);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setProperty(Statistics.ENABLED.getName(), "false");
        clientConfig.setProperty(Statistics.PERIOD_SECONDS.getName(), Integer.toString(STATS_PERIOD_SECONDS));

        hazelcastFactory.newHazelcastClient(clientConfig);

        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                Map<String, String> statistics = clientEngine.getClientStatistics();
                assertEquals(0, statistics.size());
            }
        }, STATS_PERIOD_SECONDS * 3);
    }

    @Test
    public void testEscapeSpecialCharacter() {
        String originalString = "stat1=value1.lastName,stat2=value2\\hello==";
        String escapedString = "stat1\\=value1\\.lastName\\,stat2\\=value2\\\\hello\\=\\=";
        StringBuilder buffer = new StringBuilder(originalString);
        Statistics.escapeSpecialCharacters(buffer);
        assertEquals(escapedString, buffer.toString());
        assertEquals(originalString, unescapeSpecialCharacters(escapedString));
    }

    @Test
    public void testSplit() {
        String escapedString = "stat1=value1.lastName,stat2=full\\name==hazel\\,ali,";
        String[] expectedStrings = {"stat1=value1.lastName", "stat2=full\\name==hazel\\,ali"};
        List<String> strings = split(escapedString);
        assertArrayEquals(expectedStrings, strings.toArray());
    }

    private String verifyThatCollectionIsPeriodic(Map<String, String> stats, long lastCollectionTime) {
        String newStatisticsCollectionTimeString = stats.get("lastStatisticsCollectionTime");
        long newCollectionTime = Long.parseLong(newStatisticsCollectionTimeString);
        long timeDifferenceInMillis = newCollectionTime - lastCollectionTime;
        assertTrue("Time difference between two collections is " + timeDifferenceInMillis + " msecs. It is too high!",
                timeDifferenceInMillis < 3 * STATS_PERIOD_SECONDS * 1000);
        assertTrue(
                "Time difference between two collections is " + timeDifferenceInMillis + " msecs but it should be greater than "
                        + STATS_PERIOD_SECONDS * 1000 + " msecs", timeDifferenceInMillis >= STATS_PERIOD_SECONDS * 1000);
        return newStatisticsCollectionTimeString;
    }

    private void waitForFirstStatisticsCollection(final HazelcastClientInstanceImpl client, final ClientEngineImpl clientEngine) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                getStats(client, clientEngine);
            }
        }, 3 * STATS_PERIOD_SECONDS);
    }

    private void waitForNextStatsCollection(final HazelcastClientInstanceImpl client, final ClientEngineImpl clientEngine,
                                            final String lastStatisticsCollectionTime) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                Map<String, String> stats = getStats(client, clientEngine);
                assertNotEquals(lastStatisticsCollectionTime, stats.get("lastStatisticsCollectionTime"));
            }
        });
    }

    private <K, V> CacheConfig<K, V> createCacheConfig() {
        return new CacheConfig<K, V>()
                .setInMemoryFormat(InMemoryFormat.BINARY);
    }

    private CachingProvider getCachingProvider(HazelcastInstance instance) {
        return HazelcastServerCachingProvider.createCachingProvider(instance);
    }

    private void produceSomeStats(HazelcastInstance hazelcastInstance, HazelcastClientInstanceImpl client,
                                  IMap<Integer, Integer> map) {
        map.put(5, 10);
        assertEquals(10, map.get(5).intValue());
        assertEquals(10, map.get(5).intValue());
        ICache<Integer, Integer> cache = createCache(hazelcastInstance, CACHE_NAME, client);
        cache.put(9, 20);
        assertEquals(20, cache.get(9).intValue());
        assertEquals(20, cache.get(9).intValue());
    }

    private HazelcastClientInstanceImpl createHazelcastClient() {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setProperty(Statistics.ENABLED.getName(), "true");
        clientConfig.setProperty(Statistics.PERIOD_SECONDS.getName(), Integer.toString(STATS_PERIOD_SECONDS));

        // add IMap and ICache with Near Cache config
        clientConfig.addNearCacheConfig(new NearCacheConfig(MAP_NAME));
        clientConfig.addNearCacheConfig(new NearCacheConfig(CACHE_NAME));

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
        assertNotNull("clientStatistics should not be null", clientStatistics);
        assertEquals("clientStatistics.size() should be 1", 1, clientStatistics.size());
        Set<Map.Entry<String, String>> entries = clientStatistics.entrySet();
        Map.Entry<String, String> statEntry = entries.iterator().next();
        assertEquals(client.getClientClusterService().getLocalClient().getUuid(), statEntry.getKey());
        return parseStatValue(statEntry.getValue());
    }

    private Map<String, String> parseStatValue(String value) {
        Map<String, String> result = new HashMap<String, String>();
        for (String stat : split(value)) {
            List<String> keyValue = split(stat, 0, '=');
            assertNotNull(format("keyValue should not be null (%s)", stat), keyValue);
            result.put(unescapeSpecialCharacters(keyValue.get(0)), unescapeSpecialCharacters(keyValue.get(1)));
        }
        return result;
    }
}
