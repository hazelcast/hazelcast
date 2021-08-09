/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.internal.util.phonehome;

import com.hazelcast.config.AttributeConfig;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.QueryCacheConfig;
import com.hazelcast.config.WanReplicationRef;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.monitor.impl.LocalMapStatsImpl;
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.spi.CachingProvider;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.RuntimeMXBean;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;

import static com.hazelcast.cache.CacheTestSupport.createServerCachingProvider;
import static com.hazelcast.test.Accessors.getNode;
import static java.lang.Math.min;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class PhoneHomeTest extends HazelcastTestSupport {

    private Node node;
    private PhoneHome phoneHome;

    @Before
    public void initialise() {
        HazelcastInstance hz = createHazelcastInstance();
        node = getNode(hz);
        phoneHome = new PhoneHome(node);
    }

    @Test
    public void testPhoneHomeParameters() {
        sleepAtLeastMillis(1);
        Map<String, String> parameters = phoneHome.phoneHome(true);
        RuntimeMXBean runtimeMxBean = ManagementFactory.getRuntimeMXBean();
        OperatingSystemMXBean osMxBean = ManagementFactory.getOperatingSystemMXBean();
        assertEquals(parameters.get(PhoneHomeMetrics.BUILD_VERSION.getRequestParameterName()), BuildInfoProvider.getBuildInfo().getVersion());
        String expectedCP = System.getProperty("java.class.path");
        assertEquals(
                parameters.get(PhoneHomeMetrics.JAVA_CLASSPATH.getRequestParameterName()),
                expectedCP.substring(0, min(expectedCP.length(), BuildInfoCollector.CLASSPATH_MAX_LENGTH))
        );
        assertEquals(UUID.fromString(parameters.get(PhoneHomeMetrics.UUID_OF_CLUSTER.getRequestParameterName())), node.getLocalMember().getUuid());
        assertNull(parameters.get("e"));
        assertNull(parameters.get("oem"));
        assertNull(parameters.get("l"));
        assertNull(parameters.get("hdgb"));
        assertEquals(parameters.get(PhoneHomeMetrics.HAZELCAST_DOWNLOAD_ID.getRequestParameterName()), "source");
        assertEquals(parameters.get(PhoneHomeMetrics.CLUSTER_SIZE.getRequestParameterName()), "1");
        assertEquals(parameters.get(PhoneHomeMetrics.CLIENT_ENDPOINT_COUNT.getRequestParameterName()), "A");

        assertEquals(parameters.get(PhoneHomeMetrics.ACTIVE_CPP_CLIENTS_COUNT.getRequestParameterName()), "0");
        assertEquals(parameters.get(PhoneHomeMetrics.ACTIVE_CSHARP_CLIENTS_COUNT.getRequestParameterName()), "0");
        assertEquals(parameters.get(PhoneHomeMetrics.ACTIVE_JAVA_CLIENTS_COUNT.getRequestParameterName()), "0");
        assertEquals(parameters.get(PhoneHomeMetrics.ACTIVE_NODEJS_CLIENTS_COUNT.getRequestParameterName()), "0");
        assertEquals(parameters.get(PhoneHomeMetrics.ACTIVE_PYTHON_CLIENTS_COUNT.getRequestParameterName()), "0");
        assertEquals(parameters.get(PhoneHomeMetrics.ACTIVE_GO_CLIENTS_COUNT.getRequestParameterName()), "0");

        assertEquals(parameters.get(PhoneHomeMetrics.OPENED_CPP_CLIENT_CONNECTIONS_COUNT.getRequestParameterName()), "0");
        assertEquals(parameters.get(PhoneHomeMetrics.OPENED_CSHARP_CLIENT_CONNECTIONS_COUNT.getRequestParameterName()), "0");
        assertEquals(parameters.get(PhoneHomeMetrics.OPENED_JAVA_CLIENT_CONNECTIONS_COUNT.getRequestParameterName()), "0");
        assertEquals(parameters.get(PhoneHomeMetrics.OPENED_NODEJS_CLIENT_CONNECTIONS_COUNT.getRequestParameterName()), "0");
        assertEquals(parameters.get(PhoneHomeMetrics.OPENED_PYTHON_CLIENT_CONNECTIONS_COUNT.getRequestParameterName()), "0");
        assertEquals(parameters.get(PhoneHomeMetrics.OPENED_GO_CLIENT_CONNECTIONS_COUNT.getRequestParameterName()), "0");

        assertEquals(parameters.get(PhoneHomeMetrics.CLOSED_CPP_CLIENT_CONNECTIONS_COUNT.getRequestParameterName()), "0");
        assertEquals(parameters.get(PhoneHomeMetrics.CLOSED_CSHARP_CLIENT_CONNECTIONS_COUNT.getRequestParameterName()), "0");
        assertEquals(parameters.get(PhoneHomeMetrics.CLOSED_JAVA_CLIENT_CONNECTIONS_COUNT.getRequestParameterName()), "0");
        assertEquals(parameters.get(PhoneHomeMetrics.CLOSED_NODEJS_CLIENT_CONNECTIONS_COUNT.getRequestParameterName()), "0");
        assertEquals(parameters.get(PhoneHomeMetrics.CLOSED_PYTHON_CLIENT_CONNECTIONS_COUNT.getRequestParameterName()), "0");
        assertEquals(parameters.get(PhoneHomeMetrics.CLOSED_GO_CLIENT_CONNECTIONS_COUNT.getRequestParameterName()), "0");

        assertEquals(parameters.get(PhoneHomeMetrics.TOTAL_CPP_CLIENT_CONNECTION_DURATION.getRequestParameterName()), "0");
        assertEquals(parameters.get(PhoneHomeMetrics.TOTAL_CSHARP_CLIENT_CONNECTION_DURATION.getRequestParameterName()), "0");
        assertEquals(parameters.get(PhoneHomeMetrics.TOTAL_JAVA_CLIENT_CONNECTION_DURATION.getRequestParameterName()), "0");
        assertEquals(parameters.get(PhoneHomeMetrics.TOTAL_NODEJS_CLIENT_CONNECTION_DURATION.getRequestParameterName()), "0");
        assertEquals(parameters.get(PhoneHomeMetrics.TOTAL_PYTHON_CLIENT_CONNECTION_DURATION.getRequestParameterName()), "0");
        assertEquals(parameters.get(PhoneHomeMetrics.TOTAL_GO_CLIENT_CONNECTION_DURATION.getRequestParameterName()), "0");

        assertEquals(parameters.get(PhoneHomeMetrics.CPP_CLIENT_VERSIONS.getRequestParameterName()), "");
        assertEquals(parameters.get(PhoneHomeMetrics.CSHARP_CLIENT_VERSIONS.getRequestParameterName()), "");
        assertEquals(parameters.get(PhoneHomeMetrics.JAVA_CLIENT_VERSIONS.getRequestParameterName()), "");
        assertEquals(parameters.get(PhoneHomeMetrics.NODEJS_CLIENT_VERSIONS.getRequestParameterName()), "");
        assertEquals(parameters.get(PhoneHomeMetrics.PYTHON_CLIENT_VERSIONS.getRequestParameterName()), "");
        assertEquals(parameters.get(PhoneHomeMetrics.GO_CLIENT_VERSIONS.getRequestParameterName()), "");

        assertFalse(Integer.parseInt(parameters.get(PhoneHomeMetrics.TIME_TAKEN_TO_CLUSTER_UP.getRequestParameterName())) < 0);
        assertNotEquals(parameters.get(PhoneHomeMetrics.UPTIME_OF_RUNTIME_MXBEAN.getRequestParameterName()), "0");
        assertNotEquals(parameters.get(PhoneHomeMetrics.UPTIME_OF_RUNTIME_MXBEAN.getRequestParameterName()), parameters.get(PhoneHomeMetrics.TIME_TAKEN_TO_CLUSTER_UP.getRequestParameterName()));
        assertEquals(parameters.get(PhoneHomeMetrics.OPERATING_SYSTEM_NAME.getRequestParameterName()), osMxBean.getName());
        assertEquals(parameters.get(PhoneHomeMetrics.OPERATING_SYSTEM_ARCH.getRequestParameterName()), osMxBean.getArch());
        assertEquals(parameters.get(PhoneHomeMetrics.OPERATING_SYSTEM_VERSION.getRequestParameterName()), osMxBean.getVersion());
        assertEquals(parameters.get(PhoneHomeMetrics.RUNTIME_MXBEAN_VM_NAME.getRequestParameterName()), runtimeMxBean.getVmName());
        assertEquals(parameters.get(PhoneHomeMetrics.JAVA_VERSION_OF_SYSTEM.getRequestParameterName()), System.getProperty("java.version"));
    }

    @Test
    public void testConvertToLetter() {
        assertEquals("A", MetricsCollector.convertToLetter(4));
        assertEquals("B", MetricsCollector.convertToLetter(9));
        assertEquals("C", MetricsCollector.convertToLetter(19));
        assertEquals("D", MetricsCollector.convertToLetter(39));
        assertEquals("E", MetricsCollector.convertToLetter(59));
        assertEquals("F", MetricsCollector.convertToLetter(99));
        assertEquals("G", MetricsCollector.convertToLetter(149));
        assertEquals("H", MetricsCollector.convertToLetter(299));
        assertEquals("J", MetricsCollector.convertToLetter(599));
        assertEquals("I", MetricsCollector.convertToLetter(1000));
    }

    @Test
    public void testMapCount() {
        testCounts(node.hazelcastInstance::getMap, 0,
                PhoneHomeMetrics.COUNT_OF_MAPS,
                PhoneHomeMetrics.COUNT_OF_MAPS_ALL_TIME);
    }

    @Test
    public void testMapCountWithBackupReadEnabled() {
        Map<String, String> parameters;
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get(PhoneHomeMetrics.MAP_COUNT_WITH_READ_ENABLED.getRequestParameterName()), "0");

        Map<String, String> map1 = node.hazelcastInstance.getMap("hazelcast");
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get(PhoneHomeMetrics.MAP_COUNT_WITH_READ_ENABLED.getRequestParameterName()), "0");

        node.getConfig().getMapConfig("hazelcast").setReadBackupData(true);
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get(PhoneHomeMetrics.MAP_COUNT_WITH_READ_ENABLED.getRequestParameterName()), "1");
    }

    @Test
    public void testMapCountWithMapStoreEnabled() {
        Map<String, String> parameters;
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get(PhoneHomeMetrics.MAP_COUNT_WITH_MAP_STORE_ENABLED.getRequestParameterName()), "0");

        Map<String, String> map1 = node.hazelcastInstance.getMap("hazelcast");
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get(PhoneHomeMetrics.MAP_COUNT_WITH_MAP_STORE_ENABLED.getRequestParameterName()), "0");

        node.getConfig().getMapConfig("hazelcast").getMapStoreConfig().setEnabled(true);
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get(PhoneHomeMetrics.MAP_COUNT_WITH_MAP_STORE_ENABLED.getRequestParameterName()), "1");
    }

    @Test
    public void testMapCountWithAtleastOneQueryCache() {
        Map<String, String> parameters;
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get(PhoneHomeMetrics.MAP_COUNT_WITH_ATLEAST_ONE_QUERY_CACHE.getRequestParameterName()), "0");

        Map<String, String> map1 = node.hazelcastInstance.getMap("hazelcast");
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get(PhoneHomeMetrics.MAP_COUNT_WITH_ATLEAST_ONE_QUERY_CACHE.getRequestParameterName()), "0");

        QueryCacheConfig cacheConfig = new QueryCacheConfig();
        cacheConfig.setName("hazelcastconfig");
        node.getConfig().getMapConfig("hazelcast").addQueryCacheConfig(cacheConfig);
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get(PhoneHomeMetrics.MAP_COUNT_WITH_ATLEAST_ONE_QUERY_CACHE.getRequestParameterName()), "1");

        cacheConfig = new QueryCacheConfig();
        cacheConfig.setName("hazelcastconfig2");
        node.getConfig().getMapConfig("hazelcast").addQueryCacheConfig(cacheConfig);
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get(PhoneHomeMetrics.MAP_COUNT_WITH_ATLEAST_ONE_QUERY_CACHE.getRequestParameterName()), "1");
    }

    @Test
    public void testMapCountWithAtleastOneIndex() {
        Map<String, String> parameters;
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get(PhoneHomeMetrics.MAP_COUNT_WITH_ATLEAST_ONE_INDEX.getRequestParameterName()), "0");

        Map<String, String> map1 = node.hazelcastInstance.getMap("hazelcast");
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get(PhoneHomeMetrics.MAP_COUNT_WITH_ATLEAST_ONE_INDEX.getRequestParameterName()), "0");

        IndexConfig config = new IndexConfig(IndexType.SORTED, "hazelcast");
        node.getConfig().getMapConfig("hazelcast").getIndexConfigs().add(config);
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get(PhoneHomeMetrics.MAP_COUNT_WITH_ATLEAST_ONE_INDEX.getRequestParameterName()), "1");

        config = new IndexConfig(IndexType.HASH, "phonehome");
        node.getConfig().getMapConfig("hazelcast").getIndexConfigs().add(config);
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get(PhoneHomeMetrics.MAP_COUNT_WITH_ATLEAST_ONE_INDEX.getRequestParameterName()), "1");
    }

    @Test
    public void testMapCountWithHotRestartEnabled() {
        Map<String, String> parameters;
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get(PhoneHomeMetrics.MAP_COUNT_WITH_HOT_RESTART_ENABLED.getRequestParameterName()), "0");

        Map<String, String> map1 = node.hazelcastInstance.getMap("hazelcast");
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get(PhoneHomeMetrics.MAP_COUNT_WITH_HOT_RESTART_ENABLED.getRequestParameterName()), "0");

        node.getConfig().getMapConfig("hazelcast").getHotRestartConfig().setEnabled(true);
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get(PhoneHomeMetrics.MAP_COUNT_WITH_HOT_RESTART_ENABLED.getRequestParameterName()), "1");
    }

    @Test
    public void testMapCountWithWANReplication() {
        Map<String, String> parameters;
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get(PhoneHomeMetrics.MAP_COUNT_WITH_WAN_REPLICATION.getRequestParameterName()), "0");

        Map<String, String> map1 = node.hazelcastInstance.getMap("hazelcast");
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get(PhoneHomeMetrics.MAP_COUNT_WITH_WAN_REPLICATION.getRequestParameterName()), "0");

        node.getConfig().getMapConfig("hazelcast").setWanReplicationRef(new WanReplicationRef());
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get(PhoneHomeMetrics.MAP_COUNT_WITH_WAN_REPLICATION.getRequestParameterName()), "1");
    }

    @Test
    public void testMapCountWithAtleastOneAttribute() {
        Map<String, String> parameters;
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get(PhoneHomeMetrics.MAP_COUNT_WITH_ATLEAST_ONE_ATTRIBUTE.getRequestParameterName()), "0");

        Map<String, String> map1 = node.hazelcastInstance.getMap("hazelcast");
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get(PhoneHomeMetrics.MAP_COUNT_WITH_ATLEAST_ONE_ATTRIBUTE.getRequestParameterName()), "0");

        node.getConfig().getMapConfig("hazelcast").getAttributeConfigs().add(new AttributeConfig());
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get(PhoneHomeMetrics.MAP_COUNT_WITH_ATLEAST_ONE_ATTRIBUTE.getRequestParameterName()), "1");

        node.getConfig().getMapConfig("hazelcast").getAttributeConfigs().add(new AttributeConfig());
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get(PhoneHomeMetrics.MAP_COUNT_WITH_ATLEAST_ONE_ATTRIBUTE.getRequestParameterName()), "1");
    }

    @Test
    public void testMapCountUsingEviction() {
        Map<String, String> parameters;
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get(PhoneHomeMetrics.MAP_COUNT_USING_EVICTION.getRequestParameterName()), "0");

        Map<String, String> map1 = node.hazelcastInstance.getMap("hazelcast");
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get(PhoneHomeMetrics.MAP_COUNT_USING_EVICTION.getRequestParameterName()), "0");

        EvictionConfig config = new EvictionConfig();
        config.setEvictionPolicy(EvictionPolicy.LRU);
        node.getConfig().getMapConfig("hazelcast").setEvictionConfig(config);
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get(PhoneHomeMetrics.MAP_COUNT_USING_EVICTION.getRequestParameterName()), "1");

        config.setEvictionPolicy(EvictionPolicy.NONE);
        node.getConfig().getMapConfig("hazelcast").setEvictionConfig(config);
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get(PhoneHomeMetrics.MAP_COUNT_USING_EVICTION.getRequestParameterName()), "0");
    }

    @Test
    public void testMapCountWithNativeInMemory() {
        Map<String, String> parameters;
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get(PhoneHomeMetrics.MAP_COUNT_USING_NATIVE_INMEMORY_FORMAT.getRequestParameterName()), "0");

        Map<String, String> map1 = node.hazelcastInstance.getMap("hazelcast");
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get(PhoneHomeMetrics.MAP_COUNT_USING_NATIVE_INMEMORY_FORMAT.getRequestParameterName()), "0");

        node.getConfig().getMapConfig("hazelcast").setInMemoryFormat(InMemoryFormat.NATIVE);
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get(PhoneHomeMetrics.MAP_COUNT_USING_NATIVE_INMEMORY_FORMAT.getRequestParameterName()), "1");
    }

    @Test
    public void testSetCount() {
        testCounts(node.hazelcastInstance::getSet, 0,
                PhoneHomeMetrics.COUNT_OF_SETS, PhoneHomeMetrics.COUNT_OF_SETS_ALL_TIME);
    }

    @Test
    public void testQueueCount() {
        testCounts(node.hazelcastInstance::getQueue, 0,
                PhoneHomeMetrics.COUNT_OF_QUEUES, PhoneHomeMetrics.COUNT_OF_QUEUES_ALL_TIME);
    }

    @Test
    public void testMultimapCount() {
        testCounts(node.hazelcastInstance::getMultiMap, 0,
                PhoneHomeMetrics.COUNT_OF_MULTIMAPS, PhoneHomeMetrics.COUNT_OF_MULTIMAPS_ALL_TIME);
    }

    @Test
    public void testListCount() {
        testCounts(node.hazelcastInstance::getList, 0,
                PhoneHomeMetrics.COUNT_OF_LISTS, PhoneHomeMetrics.COUNT_OF_LISTS_ALL_TIME);
    }

    @Test
    public void testRingBufferCount() {
        testCounts(node.hazelcastInstance::getRingbuffer, 0,
                PhoneHomeMetrics.COUNT_OF_RING_BUFFERS, PhoneHomeMetrics.COUNT_OF_RING_BUFFERS_ALL_TIME);
    }

    @Test
    public void testCacheCount() {
        Map<String, String> parameters;
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get(PhoneHomeMetrics.COUNT_OF_CACHES.getRequestParameterName()), "0");
        assertEquals(parameters.get(PhoneHomeMetrics.COUNT_OF_CACHES_ALL_TIME.getRequestParameterName()), "0");

        CachingProvider cachingProvider = createServerCachingProvider(node.hazelcastInstance);
        CacheManager cacheManager = cachingProvider.getCacheManager();
        cacheManager.createCache("hazelcast", new CacheConfig<>("hazelcast"));
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get(PhoneHomeMetrics.COUNT_OF_CACHES.getRequestParameterName()), "1");
        assertEquals(parameters.get(PhoneHomeMetrics.COUNT_OF_CACHES_ALL_TIME.getRequestParameterName()), "1");

        Cache<Object, Object> cache = cacheManager.createCache("phonehome", new CacheConfig<>("phonehome"));
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get(PhoneHomeMetrics.COUNT_OF_CACHES.getRequestParameterName()), "2");
        assertEquals(parameters.get(PhoneHomeMetrics.COUNT_OF_CACHES_ALL_TIME.getRequestParameterName()), "2");

        cacheManager.destroyCache("phonehome");
        cacheManager.destroyCache("hazelcast");
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get(PhoneHomeMetrics.COUNT_OF_CACHES.getRequestParameterName()), "0");
        assertEquals(parameters.get(PhoneHomeMetrics.COUNT_OF_CACHES_ALL_TIME.getRequestParameterName()), "2");
    }

    @Test
    public void testCacheWithWANReplication() {
        Map<String, String> parameters;
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get(PhoneHomeMetrics.CACHE_COUNT_WITH_WAN_REPLICATION.getRequestParameterName()), "0");

        CachingProvider cachingProvider = createServerCachingProvider(node.hazelcastInstance);
        CacheManager cacheManager = cachingProvider.getCacheManager();
        CacheSimpleConfig cacheSimpleConfig = new CacheSimpleConfig();
        cacheSimpleConfig.setName("hazelcast");
        cacheSimpleConfig.setWanReplicationRef(new WanReplicationRef());
        cacheManager.createCache("hazelcast", new CacheConfig<>("hazelcast"));
        node.getConfig().addCacheConfig(cacheSimpleConfig);
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get(PhoneHomeMetrics.CACHE_COUNT_WITH_WAN_REPLICATION.getRequestParameterName()), "1");

    }

    @Test
    public void testTopicCount() {
        testCounts(node.hazelcastInstance::getTopic, 0,
                PhoneHomeMetrics.COUNT_OF_TOPICS, PhoneHomeMetrics.COUNT_OF_TOPICS_ALL_TIME);

    }

    @Test
    public void testReplicatedMapCount() {
        testCounts(node.hazelcastInstance::getReplicatedMap, 0,
                PhoneHomeMetrics.COUNT_OF_REPLICATED_MAPS, PhoneHomeMetrics.COUNT_OF_REPLICATED_MAPS_ALL_TIME);
    }

    @Test
    public void testCardinalityEstimatorMapCount() {
        testCounts(node.hazelcastInstance::getCardinalityEstimator, 0,
                PhoneHomeMetrics.COUNT_OF_CARDINALITY_ESTIMATORS,
                PhoneHomeMetrics.COUNT_OF_CARDINALITY_ESTIMATORS_ALL_TIME);

    }

    @Test
    public void testPNCounterCount() {
        testCounts(node.hazelcastInstance::getPNCounter, 0,
                PhoneHomeMetrics.COUNT_OF_PN_COUNTERS,
                PhoneHomeMetrics.COUNT_OF_PN_COUNTERS_ALL_TIME);

    }

    @Test
    public void testFlakeIDGeneratorCount() {
        testCounts(node.hazelcastInstance::getFlakeIdGenerator, 0,
                PhoneHomeMetrics.COUNT_OF_FLAKE_ID_GENERATORS,
                PhoneHomeMetrics.COUNT_OF_FLAKE_ID_GENERATORS_ALL_TIME);
    }

    @Test
    public void testMapPutLatencyWithoutMapStore() {
        Map<String, String> parameters;
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get(PhoneHomeMetrics.AVERAGE_PUT_LATENCY_OF_MAPS_WITHOUT_MAPSTORE.getRequestParameterName()), "-1");

        IMap<Object, Object> iMap = node.hazelcastInstance.getMap("hazelcast");
        LocalMapStatsImpl localMapStats = (LocalMapStatsImpl) iMap.getLocalMapStats();
        localMapStats.incrementPutLatencyNanos(2000000000L);
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get(PhoneHomeMetrics.AVERAGE_PUT_LATENCY_OF_MAPS_WITHOUT_MAPSTORE.getRequestParameterName()), String.valueOf(2000));

        localMapStats.incrementPutLatencyNanos(1000000000L);
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get(PhoneHomeMetrics.AVERAGE_PUT_LATENCY_OF_MAPS_WITHOUT_MAPSTORE.getRequestParameterName()), String.valueOf(1500));

        localMapStats.incrementPutLatencyNanos(2000000000L);
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get(PhoneHomeMetrics.AVERAGE_PUT_LATENCY_OF_MAPS_WITHOUT_MAPSTORE.getRequestParameterName()), String.valueOf(1666));
    }

    @Test
    public void testMapGetLatencyWithoutMapStore() {
        Map<String, String> parameters;
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get(PhoneHomeMetrics.AVERAGE_GET_LATENCY_OF_MAPS_WITHOUT_MAPSTORE.getRequestParameterName()), "-1");

        IMap<Object, Object> iMap = node.hazelcastInstance.getMap("hazelcast");
        LocalMapStatsImpl localMapStats = (LocalMapStatsImpl) iMap.getLocalMapStats();
        localMapStats.incrementGetLatencyNanos(2000000000L);
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get(PhoneHomeMetrics.AVERAGE_GET_LATENCY_OF_MAPS_WITHOUT_MAPSTORE.getRequestParameterName()), String.valueOf(2000));

        localMapStats.incrementGetLatencyNanos(2000000000L);
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get(PhoneHomeMetrics.AVERAGE_GET_LATENCY_OF_MAPS_WITHOUT_MAPSTORE.getRequestParameterName()), String.valueOf(2000));
    }

    private IMap<Object, Object> initialiseForMapStore(String mapName) {
        IMap<Object, Object> iMap = node.hazelcastInstance.getMap(mapName);
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setEnabled(true);
        mapStoreConfig.setImplementation(new DelayMapStore());
        node.getConfig().getMapConfig(mapName).setMapStoreConfig(mapStoreConfig);
        return iMap;
    }

    @Test
    public void testMapPutLatencyWithMapStore() {
        Map<String, String> parameters;
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get(PhoneHomeMetrics.AVERAGE_PUT_LATENCY_OF_MAPS_USING_MAPSTORE.getRequestParameterName()), "-1");

        IMap<Object, Object> iMap1 = initialiseForMapStore("hazelcast");
        iMap1.put("key1", "hazelcast");
        iMap1.put("key2", "phonehome");
        parameters = phoneHome.phoneHome(true);
        assertGreaterOrEquals(PhoneHomeMetrics.AVERAGE_PUT_LATENCY_OF_MAPS_USING_MAPSTORE.getRequestParameterName(),
                Long.parseLong(parameters.get(PhoneHomeMetrics.AVERAGE_PUT_LATENCY_OF_MAPS_USING_MAPSTORE.getRequestParameterName())), 200);

        IMap<Object, Object> iMap2 = initialiseForMapStore("phonehome");
        iMap2.put("key3", "hazelcast");
        parameters = phoneHome.phoneHome(true);
        assertGreaterOrEquals(PhoneHomeMetrics.AVERAGE_PUT_LATENCY_OF_MAPS_USING_MAPSTORE.getRequestParameterName(),
                Long.parseLong(parameters.get(PhoneHomeMetrics.AVERAGE_PUT_LATENCY_OF_MAPS_USING_MAPSTORE.getRequestParameterName())), 200);
    }

    @Test
    public void testMapGetLatencyWithMapStore() {
        Map<String, String> parameters;
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get(PhoneHomeMetrics.AVERAGE_GET_LATENCY_OF_MAPS_USING_MAPSTORE.getRequestParameterName()), "-1");

        IMap<Object, Object> iMap1 = initialiseForMapStore("hazelcast");
        iMap1.get("key1");
        iMap1.get("key2");
        parameters = phoneHome.phoneHome(true);
        assertGreaterOrEquals(PhoneHomeMetrics.AVERAGE_GET_LATENCY_OF_MAPS_USING_MAPSTORE.getRequestParameterName(),
                Long.parseLong(parameters.get(PhoneHomeMetrics.AVERAGE_GET_LATENCY_OF_MAPS_USING_MAPSTORE.getRequestParameterName())), 200);

        IMap<Object, Object> iMap2 = initialiseForMapStore("phonehome");
        iMap2.get("key3");
        parameters = phoneHome.phoneHome(true);
        assertGreaterOrEquals(PhoneHomeMetrics.AVERAGE_GET_LATENCY_OF_MAPS_USING_MAPSTORE.getRequestParameterName(),
                Long.parseLong(parameters.get(PhoneHomeMetrics.AVERAGE_GET_LATENCY_OF_MAPS_USING_MAPSTORE.getRequestParameterName())), 200);
    }

    private void testCounts(Function<String, ? extends DistributedObject> distributedObjectCreateFn,
                            int initial, PhoneHomeMetrics countMetric,
                            PhoneHomeMetrics totalCountMetric) {
        Map<String, String> parameters;
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get(countMetric.getRequestParameterName()), Integer.toString(initial));
        assertEquals(parameters.get(totalCountMetric.getRequestParameterName()), Integer.toString(initial));

        DistributedObject obj1 = distributedObjectCreateFn.apply("hazelcast");
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get(countMetric.getRequestParameterName()), Integer.toString(initial + 1));
        assertEquals(parameters.get(totalCountMetric.getRequestParameterName()), Integer.toString(initial + 1));

        DistributedObject obj2 = distributedObjectCreateFn.apply("phonehome");
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get(countMetric.getRequestParameterName()), Integer.toString(initial + 2));
        assertEquals(parameters.get(totalCountMetric.getRequestParameterName()), Integer.toString(initial + 2));

        obj2.destroy();
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get(countMetric.getRequestParameterName()), Integer.toString(initial + 1));
        assertEquals(parameters.get(totalCountMetric.getRequestParameterName()), Integer.toString(initial + 2));
    }
}
