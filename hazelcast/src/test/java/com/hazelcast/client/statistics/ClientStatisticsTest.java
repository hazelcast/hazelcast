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

package com.hazelcast.client.statistics;

import com.hazelcast.cache.ICache;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.ClientEngineImpl;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.connection.tcp.TcpClientConnection;
import com.hazelcast.client.impl.statistics.ClientStatistics;
import com.hazelcast.client.impl.statistics.ClientStatisticsService;
import com.hazelcast.client.test.ClientTestSupport;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICacheManager;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
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
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.cache.CacheTestSupport.createServerCachingProvider;
import static com.hazelcast.client.impl.statistics.ClientStatisticsService.split;
import static com.hazelcast.client.impl.statistics.ClientStatisticsService.unescapeSpecialCharacters;
import static com.hazelcast.test.Accessors.getClientEngineImpl;
import static java.lang.String.format;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientStatisticsTest extends ClientTestSupport {

    private static final int STATS_PERIOD_SECONDS = 1;
    private static final long STATS_PERIOD_MILLIS = TimeUnit.SECONDS.toMillis(STATS_PERIOD_SECONDS);

    private static final String MAP_NAME = "StatTestMapFirst.First";
    private static final String CACHE_NAME = "StatTestICache.First";
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

        TcpClientConnection aConnection = (TcpClientConnection) client.getConnectionManager().getActiveConnections().iterator().next();
        String expectedClientAddress = aConnection.getLocalSocketAddress().getAddress().getHostAddress();
        assertEquals(expectedClientAddress, stats.get("clientAddress"));
        assertEquals(BuildInfoProvider.getBuildInfo().getVersion(), stats.get("clientVersion"));
        assertEquals(client.getName(), stats.get("clientName"));

        // time measured by us after client connection should be greater than the connection time reported by the statistics
        assertTrue(format("connectionTimeStat was %d, clientConnectionTime was %d (%s)",
                connectionTimeStat, clientConnectionTime, stats), clientConnectionTime >= connectionTimeStat);

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

        assertTrueEventually(() -> {
            Map<String, String> stats12 = getStats(client, clientEngine);
            String mapHits12 = stats12.get(MAP_HITS_KEY);
            assertNotNull(format("%s should not be null (%s)", MAP_HITS_KEY, stats12), mapHits12);
            assertEquals(format("Expected 0 map hits (%s)", stats12), "0", mapHits12);
            String cacheHits12 = stats12.get(CACHE_HITS_KEY);
            assertNull(format("%s should be null (%s)", CACHE_HITS_KEY, stats12), cacheHits12);

            // verify that collection is periodic
            verifyThatCollectionIsPeriodic(stats12, lastCollectionTime);
        });

        // produce map and cache stat
        produceSomeStats(hazelcastInstance, client);

        assertTrueEventually(() -> {
            Map<String, String> stats1 = getStats(client, clientEngine);
            String mapHits1 = stats1.get(MAP_HITS_KEY);
            assertNotNull(format("%s should not be null (%s)", MAP_HITS_KEY, stats1), mapHits1);
            assertEquals(format("Expected 1 map hits (%s)", stats1), "1", mapHits1);
            String cacheHits1 = stats1.get(CACHE_HITS_KEY);
            assertNotNull(format("%s should not be null (%s)", CACHE_HITS_KEY, stats1), cacheHits1);
            assertEquals(format("Expected 1 cache hits (%s)", stats1), "1", cacheHits1);
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

        ReconnectListener reconnectListener = new ReconnectListener();
        client.getLifecycleService().addLifecycleListener(reconnectListener);

        hazelcastInstance.getLifecycleService().terminate();
        hazelcastInstance = hazelcastFactory.newHazelcastInstance();
        ClientEngineImpl clientEngine = getClientEngineImpl(hazelcastInstance);

        assertOpenEventually(reconnectListener.reconnectedLatch);

        // wait enough time for statistics collection
        waitForFirstStatisticsCollection(client, clientEngine);

        getStats(client, clientEngine);
    }

    @Test
    public void testStatisticsTwoClients() {
        HazelcastInstance hazelcastInstance = hazelcastFactory.newHazelcastInstance();
        final HazelcastClientInstanceImpl client1 = createHazelcastClient();
        final HazelcastClientInstanceImpl client2 = createHazelcastClient();
        final ClientEngineImpl clientEngine = getClientEngineImpl(hazelcastInstance);

        assertTrueEventually(() -> {
            Map<UUID, ClientStatistics> clientStatistics = clientEngine.getClientStatistics();
            assertNotNull(clientStatistics);
            assertEquals(2, clientStatistics.size());
            List<UUID> expectedUUIDs = new ArrayList<>(2);
            expectedUUIDs.add(client1.getLocalEndpoint().getUuid());
            expectedUUIDs.add(client2.getLocalEndpoint().getUuid());
            for (Map.Entry<UUID, ClientStatistics> clientEntry : clientStatistics.entrySet()) {
                assertTrue(expectedUUIDs.contains(clientEntry.getKey()));
                String clientAttributes = clientEntry.getValue().clientAttributes();
                assertNotNull(clientAttributes);
            }
        });

    }

    @Test
    public void testNoUpdateWhenDisabled() {
        HazelcastInstance hazelcastInstance = hazelcastFactory.newHazelcastInstance();
        final ClientEngineImpl clientEngine = getClientEngineImpl(hazelcastInstance);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getMetricsConfig()
                .setEnabled(false)
                .setCollectionFrequencySeconds(STATS_PERIOD_SECONDS);

        hazelcastFactory.newHazelcastClient(clientConfig);

        assertTrueAllTheTime(() -> {
            Map<UUID, ClientStatistics> statistics = clientEngine.getClientStatistics();
            assertEquals(0, statistics.size());
        }, STATS_PERIOD_SECONDS * 3);
    }

    @Test
    public void testEscapeSpecialCharacter() {
        String originalString = "stat1=value1.lastName,stat2=value2\\hello==";
        String escapedString = "stat1\\=value1\\.lastName\\,stat2\\=value2\\\\hello\\=\\=";
        StringBuilder buffer = new StringBuilder(originalString);
        ClientStatisticsService.escapeSpecialCharacters(buffer);
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
                // add IMap and ICache with Near Cache config
                .addNearCacheConfig(new NearCacheConfig(MAP_NAME))
                .addNearCacheConfig(new NearCacheConfig(CACHE_NAME));

        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(Long.MAX_VALUE);
        clientConfig.getMetricsConfig()
                .setCollectionFrequencySeconds(STATS_PERIOD_SECONDS);

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
        return createServerCachingProvider(instance);
    }

    private static <K, V> CacheConfig<K, V> createCacheConfig() {
        return new CacheConfig<K, V>()
                .setInMemoryFormat(InMemoryFormat.BINARY);
    }

    private static Map<String, String> getStats(HazelcastClientInstanceImpl client, ClientEngineImpl clientEngine) {
        Map<UUID, ClientStatistics> clientStatistics = clientEngine.getClientStatistics();
        assertNotNull("clientStatistics should not be null", clientStatistics);
        assertEquals("clientStatistics.size() should be 1", 1, clientStatistics.size());
        Set<Map.Entry<UUID, ClientStatistics>> entries = clientStatistics.entrySet();
        Map.Entry<UUID, ClientStatistics> statEntry = entries.iterator().next();
        assertEquals(client.getLocalEndpoint().getUuid(), statEntry.getKey());
        return parseClientAttributeValue(statEntry.getValue().clientAttributes());
    }

    private static Map<String, String> parseClientAttributeValue(String value) {
        Map<String, String> result = new HashMap<>();
        for (String stat : split(value)) {
            List<String> keyValue = split(stat, 0, '=');
            assertNotNull(format("keyValue should not be null (%s)", stat), keyValue);
            result.put(unescapeSpecialCharacters(keyValue.get(0)), unescapeSpecialCharacters(keyValue.get(1)));
        }
        return result;
    }

    private static void waitForFirstStatisticsCollection(final HazelcastClientInstanceImpl client,
                                                         final ClientEngineImpl clientEngine) {
        assertTrueEventually(() -> getStats(client, clientEngine));
    }

    private static void waitForNextStatsCollection(final HazelcastClientInstanceImpl client, final ClientEngineImpl clientEngine,
                                                   final String lastStatisticsCollectionTime) {
        assertTrueEventually(() -> {
            Map<String, String> stats = getStats(client, clientEngine);
            assertNotEquals(lastStatisticsCollectionTime, stats.get("lastStatisticsCollectionTime"));
        });
    }

    private static void verifyThatCollectionIsPeriodic(Map<String, String> stats, long lastCollectionTime) {
        String lastStatisticsCollectionTime = stats.get("lastStatisticsCollectionTime");
        long newCollectionTime = Long.parseLong(lastStatisticsCollectionTime);
        long timeDifferenceMillis = newCollectionTime - lastCollectionTime;

        // it's seen during the tests that the collection time may be much larger (up to 9 seconds),
        // hence we will keep the upperThreshold a lot higher
        double lowerThreshold = STATS_PERIOD_MILLIS * 0.9;
        assertTrue("Time difference between two collections is " + timeDifferenceMillis
                        + " ms but, but it should be greater than " + lowerThreshold + " ms",
                timeDifferenceMillis >= lowerThreshold);
    }
}
