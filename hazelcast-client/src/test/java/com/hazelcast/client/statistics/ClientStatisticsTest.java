/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.connection.nio.ClientConnection;
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
import com.hazelcast.instance.BuildInfoProvider;
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
import java.util.concurrent.TimeUnit;

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
    private static final long STATS_PERIOD_MILLIS = TimeUnit.SECONDS.toMillis(STATS_PERIOD_SECONDS);

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
    public void testStatisticsCollectionNonDefaultPeriod() throws InterruptedException {
        HazelcastInstance hazelcastInstance = hazelcastFactory.newHazelcastInstance();
        final HazelcastClientInstanceImpl client = createHazelcastClient();
        final ClientEngineImpl clientEngine = getClientEngineImpl(hazelcastInstance);

        long clientConnectionTime = System.currentTimeMillis();

        // wait enough time for statistics collection
        waitForFirstStatisticsCollection(client, clientEngine);

        Map<String, String> stats = getStats(client, clientEngine);

        String connStat = stats.get("clusterConnectionTimestamp");
        assertNotNull(format("clusterConnectionTimestamp should not be null (%s)", stats), connStat);
        Long connectionTimeStat = Long.valueOf(connStat);
        assertNotNull(format("connectionTimeStat should not be null (%s)", stats), connStat);

        ClientConnection ownerConnection = client.getConnectionManager().getOwnerConnection();
        String expectedClientAddress = format("%s:%d", ownerConnection.getLocalSocketAddress().getAddress().getHostAddress(),
                ownerConnection.getLocalSocketAddress().getPort());
        assertEquals(expectedClientAddress, stats.get("clientAddress"));
        assertEquals(BuildInfoProvider.getBuildInfo().getVersion(), stats.get("clientVersion"));

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
        final long lastCollectionTime = Long.parseLong(lastStatisticsCollectionTimeString);

        // this creates empty map statistics
        client.getMap(MAP_NAME);

        // wait enough time for statistics collection
        waitForNextStatsCollection(client, clientEngine, lastStatisticsCollectionTimeString);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                Map<String, String> stats = getStats(client, clientEngine);
                String mapHits = stats.get(MAP_HITS_KEY);
                assertNotNull(format("%s should not be null (%s)", MAP_HITS_KEY, stats), mapHits);
                assertEquals(format("Expected 0 map hits (%s)", stats), "0", mapHits);
                String cacheHits = stats.get(CACHE_HITS_KEY);
                assertNull(format("%s should be null (%s)", CACHE_HITS_KEY, stats), cacheHits);

                // verify that collection is periodic
                verifyThatCollectionIsPeriodic(stats, lastCollectionTime);
            }
        });

        // produce map and cache stat
        produceSomeStats(hazelcastInstance, client);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                Map<String, String> stats = getStats(client, clientEngine);
                String mapHits = stats.get(MAP_HITS_KEY);
                assertNotNull(format("%s should not be null (%s)", MAP_HITS_KEY, stats), mapHits);
                assertEquals(format("Expected 1 map hits (%s)", stats), "1", mapHits);
                String cacheHits = stats.get(CACHE_HITS_KEY);
                assertNotNull(format("%s should not be null (%s)", CACHE_HITS_KEY, stats), cacheHits);
                assertEquals(format("Expected 1 cache hits (%s)", stats), "1", cacheHits);
            }
        });

    }

    @Test
    public void testStatisticsPeriod() {
        HazelcastInstance hazelcastInstance = hazelcastFactory.newHazelcastInstance();
        HazelcastClientInstanceImpl client = createHazelcastClient();
        ClientEngineImpl clientEngine = getClientEngineImpl(hazelcastInstance);

        // wait enough time for statistics collection
        waitForFirstStatisticsCollection(client, clientEngine);

        Map<String, String> initialStats = getStats(client, clientEngine);

        // produce map and cache stat
        produceSomeStats(hazelcastInstance, client);

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

    private HazelcastClientInstanceImpl createHazelcastClient() {
        ClientConfig clientConfig = new ClientConfig()
                .setProperty(Statistics.ENABLED.getName(), "true")
                .setProperty(Statistics.PERIOD_SECONDS.getName(), Integer.toString(STATS_PERIOD_SECONDS))
                // add IMap and ICache with Near Cache config
                .addNearCacheConfig(new NearCacheConfig(MAP_NAME))
                .addNearCacheConfig(new NearCacheConfig(CACHE_NAME));

        clientConfig.getNetworkConfig().setConnectionAttemptLimit(20);

        HazelcastInstance clientInstance = hazelcastFactory.newHazelcastClient(clientConfig);
        return getHazelcastClientInstanceImpl(clientInstance);
    }

    private static void produceSomeStats(HazelcastInstance hazelcastInstance, HazelcastClientInstanceImpl client) {
        IMap<Integer, Integer> map = client.getMap(MAP_NAME);
        map.put(5, 10);
        assertEquals(10, map.get(5).intValue());
        assertEquals(10, map.get(5).intValue());

        ICache<Integer, Integer> cache = createCache(hazelcastInstance, CACHE_NAME, client);
        cache.put(9, 20);
        assertEquals(20, cache.get(9).intValue());
        assertEquals(20, cache.get(9).intValue());
    }

    private static ICache<Integer, Integer> createCache(HazelcastInstance hazelcastInstance, String testCacheName,
                                                        HazelcastInstance clientInstance) {
        CachingProvider cachingProvider = getCachingProvider(hazelcastInstance);
        CacheManager cacheManager = cachingProvider.getCacheManager();
        cacheManager.createCache(testCacheName, createCacheConfig());
        ICacheManager clientCacheManager = clientInstance.getCacheManager();
        return clientCacheManager.getCache(testCacheName);
    }

    private static CachingProvider getCachingProvider(HazelcastInstance instance) {
        return HazelcastServerCachingProvider.createCachingProvider(instance);
    }

    private static <K, V> CacheConfig<K, V> createCacheConfig() {
        return new CacheConfig<K, V>()
                .setInMemoryFormat(InMemoryFormat.BINARY);
    }

    private static Map<String, String> getStats(HazelcastClientInstanceImpl client, ClientEngineImpl clientEngine) {
        Map<String, String> clientStatistics = clientEngine.getClientStatistics();
        assertNotNull("clientStatistics should not be null", clientStatistics);
        assertEquals("clientStatistics.size() should be 1", 1, clientStatistics.size());
        Set<Map.Entry<String, String>> entries = clientStatistics.entrySet();
        Map.Entry<String, String> statEntry = entries.iterator().next();
        assertEquals(client.getClientClusterService().getLocalClient().getUuid(), statEntry.getKey());
        return parseStatValue(statEntry.getValue());
    }

    private static Map<String, String> parseStatValue(String value) {
        Map<String, String> result = new HashMap<String, String>();
        for (String stat : split(value)) {
            List<String> keyValue = split(stat, 0, '=');
            assertNotNull(format("keyValue should not be null (%s)", stat), keyValue);
            result.put(unescapeSpecialCharacters(keyValue.get(0)), unescapeSpecialCharacters(keyValue.get(1)));
        }
        return result;
    }

    private static void waitForFirstStatisticsCollection(final HazelcastClientInstanceImpl client,
                                                         final ClientEngineImpl clientEngine) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                getStats(client, clientEngine);
            }
        }, STATS_PERIOD_SECONDS * 3);
    }

    private static void waitForNextStatsCollection(final HazelcastClientInstanceImpl client, final ClientEngineImpl clientEngine,
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

    private static String verifyThatCollectionIsPeriodic(Map<String, String> stats, long lastCollectionTime) {
        String lastStatisticsCollectionTime = stats.get("lastStatisticsCollectionTime");
        long newCollectionTime = Long.parseLong(lastStatisticsCollectionTime);
        long timeDifferenceMillis = newCollectionTime - lastCollectionTime;

        double lowerThreshold = STATS_PERIOD_MILLIS * 0.9;
        /**
         * It is seen during the tests that the collection time may be much larger, up to 9 seconds is seen, hence we will keep
         * the max threshold a lot higher
         */
        double upperThreshold = STATS_PERIOD_MILLIS * 20.0;
        assertTrue("Time difference between two collections is " + timeDifferenceMillis
                        + " ms but, but it should be greater than " + lowerThreshold + " ms",
                timeDifferenceMillis >= lowerThreshold);
        assertTrue("Time difference between two collections is " + timeDifferenceMillis
                        + " ms, but it should be less than " + upperThreshold + " ms",
                timeDifferenceMillis <= upperThreshold);

        return lastStatisticsCollectionTime;
    }
}
