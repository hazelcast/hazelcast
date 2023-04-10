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
package com.hazelcast.internal.util.phonehome;

import com.google.common.collect.ImmutableMap;
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
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.test.TestSources;
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
import static com.hazelcast.spi.impl.proxyservice.impl.ProxyRegistry.INTERNAL_OBJECTS_PREFIXES;
import static com.hazelcast.test.Accessors.getNode;
import static java.lang.Long.parseLong;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

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
        assertTrue(parameters.get(PhoneHomeMetrics.JAVA_CLASSPATH.getRequestParameterName()).endsWith(".jar"));
        assertEquals(UUID.fromString(parameters.get(PhoneHomeMetrics.UUID_OF_CLUSTER.getRequestParameterName())), node.getLocalMember().getUuid());
        assertNull(parameters.get("e"));
        assertNull(parameters.get("oem"));
        assertNull(parameters.get("l"));
        assertNull(parameters.get("hdgb"));
        assertEquals("source", parameters.get(PhoneHomeMetrics.HAZELCAST_DOWNLOAD_ID.getRequestParameterName()));
        assertEquals("A", parameters.get(PhoneHomeMetrics.CLUSTER_SIZE.getRequestParameterName()));
        assertEquals("1", parameters.get(PhoneHomeMetrics.EXACT_CLUSTER_SIZE.getRequestParameterName()));
        assertEquals("271", parameters.get(PhoneHomeMetrics.PARTITION_COUNT.getRequestParameterName()));
        assertEquals("A", parameters.get(PhoneHomeMetrics.CLIENT_ENDPOINT_COUNT.getRequestParameterName()));

        assertEquals("0", parameters.get(PhoneHomeMetrics.ACTIVE_CPP_CLIENTS_COUNT.getRequestParameterName()));
        assertEquals("0", parameters.get(PhoneHomeMetrics.ACTIVE_CSHARP_CLIENTS_COUNT.getRequestParameterName()));
        assertEquals("0", parameters.get(PhoneHomeMetrics.ACTIVE_JAVA_CLIENTS_COUNT.getRequestParameterName()));
        assertEquals("0", parameters.get(PhoneHomeMetrics.ACTIVE_NODEJS_CLIENTS_COUNT.getRequestParameterName()));
        assertEquals("0", parameters.get(PhoneHomeMetrics.ACTIVE_PYTHON_CLIENTS_COUNT.getRequestParameterName()));
        assertEquals("0", parameters.get(PhoneHomeMetrics.ACTIVE_GO_CLIENTS_COUNT.getRequestParameterName()));

        assertEquals("0", parameters.get(PhoneHomeMetrics.OPENED_CPP_CLIENT_CONNECTIONS_COUNT.getRequestParameterName()));
        assertEquals("0", parameters.get(PhoneHomeMetrics.OPENED_CSHARP_CLIENT_CONNECTIONS_COUNT.getRequestParameterName()));
        assertEquals("0", parameters.get(PhoneHomeMetrics.OPENED_JAVA_CLIENT_CONNECTIONS_COUNT.getRequestParameterName()));
        assertEquals("0", parameters.get(PhoneHomeMetrics.OPENED_NODEJS_CLIENT_CONNECTIONS_COUNT.getRequestParameterName()));
        assertEquals("0", parameters.get(PhoneHomeMetrics.OPENED_PYTHON_CLIENT_CONNECTIONS_COUNT.getRequestParameterName()));
        assertEquals("0", parameters.get(PhoneHomeMetrics.OPENED_GO_CLIENT_CONNECTIONS_COUNT.getRequestParameterName()));

        assertEquals("0", parameters.get(PhoneHomeMetrics.CLOSED_CPP_CLIENT_CONNECTIONS_COUNT.getRequestParameterName()));
        assertEquals("0", parameters.get(PhoneHomeMetrics.CLOSED_CSHARP_CLIENT_CONNECTIONS_COUNT.getRequestParameterName()));
        assertEquals("0", parameters.get(PhoneHomeMetrics.CLOSED_JAVA_CLIENT_CONNECTIONS_COUNT.getRequestParameterName()));
        assertEquals("0", parameters.get(PhoneHomeMetrics.CLOSED_NODEJS_CLIENT_CONNECTIONS_COUNT.getRequestParameterName()));
        assertEquals("0", parameters.get(PhoneHomeMetrics.CLOSED_PYTHON_CLIENT_CONNECTIONS_COUNT.getRequestParameterName()));
        assertEquals("0", parameters.get(PhoneHomeMetrics.CLOSED_GO_CLIENT_CONNECTIONS_COUNT.getRequestParameterName()));

        assertEquals("0", parameters.get(PhoneHomeMetrics.TOTAL_CPP_CLIENT_CONNECTION_DURATION.getRequestParameterName()));
        assertEquals("0", parameters.get(PhoneHomeMetrics.TOTAL_CSHARP_CLIENT_CONNECTION_DURATION.getRequestParameterName()));
        assertEquals("0", parameters.get(PhoneHomeMetrics.TOTAL_JAVA_CLIENT_CONNECTION_DURATION.getRequestParameterName()));
        assertEquals("0", parameters.get(PhoneHomeMetrics.TOTAL_NODEJS_CLIENT_CONNECTION_DURATION.getRequestParameterName()));
        assertEquals("0", parameters.get(PhoneHomeMetrics.TOTAL_PYTHON_CLIENT_CONNECTION_DURATION.getRequestParameterName()));
        assertEquals("0", parameters.get(PhoneHomeMetrics.TOTAL_GO_CLIENT_CONNECTION_DURATION.getRequestParameterName()));

        assertEquals("", parameters.get(PhoneHomeMetrics.CPP_CLIENT_VERSIONS.getRequestParameterName()));
        assertEquals("", parameters.get(PhoneHomeMetrics.CSHARP_CLIENT_VERSIONS.getRequestParameterName()));
        assertEquals("", parameters.get(PhoneHomeMetrics.JAVA_CLIENT_VERSIONS.getRequestParameterName()));
        assertEquals("", parameters.get(PhoneHomeMetrics.NODEJS_CLIENT_VERSIONS.getRequestParameterName()));
        assertEquals("", parameters.get(PhoneHomeMetrics.PYTHON_CLIENT_VERSIONS.getRequestParameterName()));
        assertEquals("", parameters.get(PhoneHomeMetrics.GO_CLIENT_VERSIONS.getRequestParameterName()));

        assertFalse(Integer.parseInt(parameters.get(PhoneHomeMetrics.TIME_TAKEN_TO_CLUSTER_UP.getRequestParameterName())) < 0);
        assertNotEquals("0", parameters.get(PhoneHomeMetrics.UPTIME_OF_RUNTIME_MXBEAN.getRequestParameterName()));
        assertNotEquals(parameters.get(PhoneHomeMetrics.UPTIME_OF_RUNTIME_MXBEAN.getRequestParameterName()), parameters.get(PhoneHomeMetrics.TIME_TAKEN_TO_CLUSTER_UP.getRequestParameterName()));
        assertEquals(parameters.get(PhoneHomeMetrics.OPERATING_SYSTEM_NAME.getRequestParameterName()), osMxBean.getName());
        assertEquals(parameters.get(PhoneHomeMetrics.OPERATING_SYSTEM_ARCH.getRequestParameterName()), osMxBean.getArch());
        assertEquals(parameters.get(PhoneHomeMetrics.OPERATING_SYSTEM_VERSION.getRequestParameterName()), osMxBean.getVersion());
        assertEquals(parameters.get(PhoneHomeMetrics.RUNTIME_MXBEAN_VM_NAME.getRequestParameterName()), runtimeMxBean.getVmName());
        assertEquals(System.getProperty("java.version"), parameters.get(PhoneHomeMetrics.JAVA_VERSION_OF_SYSTEM.getRequestParameterName()));
        assertEquals("true", parameters.get(PhoneHomeMetrics.JET_ENABLED.getRequestParameterName()));
        assertEquals("false", parameters.get(PhoneHomeMetrics.JET_RESOURCE_UPLOAD_ENABLED.getRequestParameterName()));
        assertEquals("false", parameters.get(PhoneHomeMetrics.CP_SUBSYSTEM_ENABLED.getRequestParameterName()));
        assertEquals("false", parameters.get(PhoneHomeMetrics.DYNAMIC_CONFIG_PERSISTENCE_ENABLED.getRequestParameterName()));
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
        assertEquals("0", parameters.get(PhoneHomeMetrics.MAP_COUNT_WITH_READ_ENABLED.getRequestParameterName()));

        Map<String, String> map1 = node.hazelcastInstance.getMap("hazelcast");
        parameters = phoneHome.phoneHome(true);
        assertEquals("0", parameters.get(PhoneHomeMetrics.MAP_COUNT_WITH_READ_ENABLED.getRequestParameterName()));

        node.getConfig().getMapConfig("hazelcast").setReadBackupData(true);
        parameters = phoneHome.phoneHome(true);
        assertEquals("1", parameters.get(PhoneHomeMetrics.MAP_COUNT_WITH_READ_ENABLED.getRequestParameterName()));
    }

    @Test
    public void pardotIdOverride_withEnvVar() {
        PhoneHome phoneHome = new PhoneHome(node, "http://example.org", ImmutableMap.of("HZ_PARDOT_ID", "1234"));
        Map<String, String> params = phoneHome.phoneHome(true);
        assertEquals("1234", params.get("p"));
    }

    @Test
    public void testMapCountWithMapStoreEnabled() {
        Map<String, String> parameters;
        parameters = phoneHome.phoneHome(true);
        assertEquals("0", parameters.get(PhoneHomeMetrics.MAP_COUNT_WITH_MAP_STORE_ENABLED.getRequestParameterName()));

        Map<String, String> map1 = node.hazelcastInstance.getMap("hazelcast");
        parameters = phoneHome.phoneHome(true);
        assertEquals("0", parameters.get(PhoneHomeMetrics.MAP_COUNT_WITH_MAP_STORE_ENABLED.getRequestParameterName()));

        node.getConfig().getMapConfig("hazelcast").getMapStoreConfig().setEnabled(true);
        parameters = phoneHome.phoneHome(true);
        assertEquals("1", parameters.get(PhoneHomeMetrics.MAP_COUNT_WITH_MAP_STORE_ENABLED.getRequestParameterName()));
    }

    @Test
    public void testMapCountWithAtleastOneQueryCache() {
        Map<String, String> parameters;
        parameters = phoneHome.phoneHome(true);
        assertEquals("0", parameters.get(PhoneHomeMetrics.MAP_COUNT_WITH_ATLEAST_ONE_QUERY_CACHE.getRequestParameterName()));

        Map<String, String> map1 = node.hazelcastInstance.getMap("hazelcast");
        parameters = phoneHome.phoneHome(true);
        assertEquals("0", parameters.get(PhoneHomeMetrics.MAP_COUNT_WITH_ATLEAST_ONE_QUERY_CACHE.getRequestParameterName()));

        QueryCacheConfig cacheConfig = new QueryCacheConfig();
        cacheConfig.setName("hazelcastconfig");
        node.getConfig().getMapConfig("hazelcast").addQueryCacheConfig(cacheConfig);
        parameters = phoneHome.phoneHome(true);
        assertEquals("1", parameters.get(PhoneHomeMetrics.MAP_COUNT_WITH_ATLEAST_ONE_QUERY_CACHE.getRequestParameterName()));

        cacheConfig = new QueryCacheConfig();
        cacheConfig.setName("hazelcastconfig2");
        node.getConfig().getMapConfig("hazelcast").addQueryCacheConfig(cacheConfig);
        parameters = phoneHome.phoneHome(true);
        assertEquals("1", parameters.get(PhoneHomeMetrics.MAP_COUNT_WITH_ATLEAST_ONE_QUERY_CACHE.getRequestParameterName()));
    }

    @Test
    public void testMapCountWithAtleastOneIndex() {
        Map<String, String> parameters;
        parameters = phoneHome.phoneHome(true);
        assertEquals("0", parameters.get(PhoneHomeMetrics.MAP_COUNT_WITH_ATLEAST_ONE_INDEX.getRequestParameterName()));

        Map<String, String> map1 = node.hazelcastInstance.getMap("hazelcast");
        parameters = phoneHome.phoneHome(true);
        assertEquals("0", parameters.get(PhoneHomeMetrics.MAP_COUNT_WITH_ATLEAST_ONE_INDEX.getRequestParameterName()));

        IndexConfig config = new IndexConfig(IndexType.SORTED, "hazelcast");
        node.getConfig().getMapConfig("hazelcast").getIndexConfigs().add(config);
        parameters = phoneHome.phoneHome(true);
        assertEquals("1", parameters.get(PhoneHomeMetrics.MAP_COUNT_WITH_ATLEAST_ONE_INDEX.getRequestParameterName()));

        config = new IndexConfig(IndexType.HASH, "phonehome");
        node.getConfig().getMapConfig("hazelcast").getIndexConfigs().add(config);
        parameters = phoneHome.phoneHome(true);
        assertEquals("1", parameters.get(PhoneHomeMetrics.MAP_COUNT_WITH_ATLEAST_ONE_INDEX.getRequestParameterName()));
    }

    @Test
    public void testMapCountWithHotRestartEnabled() {
        Map<String, String> parameters;
        parameters = phoneHome.phoneHome(true);
        assertEquals("0", parameters.get(PhoneHomeMetrics.MAP_COUNT_WITH_HOT_RESTART_OR_PERSISTENCE_ENABLED.getRequestParameterName()));

        node.hazelcastInstance.getMap("hazelcast");
        parameters = phoneHome.phoneHome(true);
        assertEquals("0", parameters.get(PhoneHomeMetrics.MAP_COUNT_WITH_HOT_RESTART_OR_PERSISTENCE_ENABLED.getRequestParameterName()));

        node.hazelcastInstance.getMap("with-hot-restart");
        node.getConfig().getMapConfig("with-hot-restart").getHotRestartConfig().setEnabled(true);
        parameters = phoneHome.phoneHome(true);
        assertEquals("1", parameters.get(PhoneHomeMetrics.MAP_COUNT_WITH_HOT_RESTART_OR_PERSISTENCE_ENABLED.getRequestParameterName()));

        node.hazelcastInstance.getMap("with-persistence");
        node.getConfig().getMapConfig("with-persistence").getDataPersistenceConfig().setEnabled(true);
        parameters = phoneHome.phoneHome(true);
        assertEquals("2", parameters.get(PhoneHomeMetrics.MAP_COUNT_WITH_HOT_RESTART_OR_PERSISTENCE_ENABLED.getRequestParameterName()));

        node.hazelcastInstance.getMap("with-both");
        node.getConfig().getMapConfig("with-both").getHotRestartConfig().setEnabled(true);
        node.getConfig().getMapConfig("with-both").getDataPersistenceConfig().setEnabled(true);
        parameters = phoneHome.phoneHome(true);
        assertEquals("3", parameters.get(PhoneHomeMetrics.MAP_COUNT_WITH_HOT_RESTART_OR_PERSISTENCE_ENABLED.getRequestParameterName()));
    }

    @Test
    public void testMapCountWithWANReplication() {
        Map<String, String> parameters;
        parameters = phoneHome.phoneHome(true);
        assertEquals("0", parameters.get(PhoneHomeMetrics.MAP_COUNT_WITH_WAN_REPLICATION.getRequestParameterName()));

        Map<String, String> map1 = node.hazelcastInstance.getMap("hazelcast");
        parameters = phoneHome.phoneHome(true);
        assertEquals("0", parameters.get(PhoneHomeMetrics.MAP_COUNT_WITH_WAN_REPLICATION.getRequestParameterName()));

        node.getConfig().getMapConfig("hazelcast").setWanReplicationRef(new WanReplicationRef());
        parameters = phoneHome.phoneHome(true);
        assertEquals("1", parameters.get(PhoneHomeMetrics.MAP_COUNT_WITH_WAN_REPLICATION.getRequestParameterName()));
    }

    @Test
    public void testMapCountWithAtleastOneAttribute() {
        Map<String, String> parameters;
        parameters = phoneHome.phoneHome(true);
        assertEquals("0", parameters.get(PhoneHomeMetrics.MAP_COUNT_WITH_ATLEAST_ONE_ATTRIBUTE.getRequestParameterName()));

        Map<String, String> map1 = node.hazelcastInstance.getMap("hazelcast");
        parameters = phoneHome.phoneHome(true);
        assertEquals("0", parameters.get(PhoneHomeMetrics.MAP_COUNT_WITH_ATLEAST_ONE_ATTRIBUTE.getRequestParameterName()));

        node.getConfig().getMapConfig("hazelcast").getAttributeConfigs().add(new AttributeConfig());
        parameters = phoneHome.phoneHome(true);
        assertEquals("1", parameters.get(PhoneHomeMetrics.MAP_COUNT_WITH_ATLEAST_ONE_ATTRIBUTE.getRequestParameterName()));

        node.getConfig().getMapConfig("hazelcast").getAttributeConfigs().add(new AttributeConfig());
        parameters = phoneHome.phoneHome(true);
        assertEquals("1", parameters.get(PhoneHomeMetrics.MAP_COUNT_WITH_ATLEAST_ONE_ATTRIBUTE.getRequestParameterName()));
    }

    @Test
    public void testMapCountUsingEviction() {
        Map<String, String> parameters;
        parameters = phoneHome.phoneHome(true);
        assertEquals("0", parameters.get(PhoneHomeMetrics.MAP_COUNT_USING_EVICTION.getRequestParameterName()));

        Map<String, String> map1 = node.hazelcastInstance.getMap("hazelcast");
        parameters = phoneHome.phoneHome(true);
        assertEquals("0", parameters.get(PhoneHomeMetrics.MAP_COUNT_USING_EVICTION.getRequestParameterName()));

        EvictionConfig config = new EvictionConfig();
        config.setEvictionPolicy(EvictionPolicy.LRU);
        node.getConfig().getMapConfig("hazelcast").setEvictionConfig(config);
        parameters = phoneHome.phoneHome(true);
        assertEquals("1", parameters.get(PhoneHomeMetrics.MAP_COUNT_USING_EVICTION.getRequestParameterName()));

        config.setEvictionPolicy(EvictionPolicy.NONE);
        node.getConfig().getMapConfig("hazelcast").setEvictionConfig(config);
        parameters = phoneHome.phoneHome(true);
        assertEquals("0", parameters.get(PhoneHomeMetrics.MAP_COUNT_USING_EVICTION.getRequestParameterName()));
    }

    @Test
    public void testMapCountWithNativeInMemory() {
        Map<String, String> parameters;
        parameters = phoneHome.phoneHome(true);
        assertEquals("0", parameters.get(PhoneHomeMetrics.MAP_COUNT_USING_NATIVE_INMEMORY_FORMAT.getRequestParameterName()));

        Map<String, String> map1 = node.hazelcastInstance.getMap("hazelcast");
        parameters = phoneHome.phoneHome(true);
        assertEquals("0", parameters.get(PhoneHomeMetrics.MAP_COUNT_USING_NATIVE_INMEMORY_FORMAT.getRequestParameterName()));

        node.getConfig().getMapConfig("hazelcast").setInMemoryFormat(InMemoryFormat.NATIVE);
        parameters = phoneHome.phoneHome(true);
        assertEquals("1", parameters.get(PhoneHomeMetrics.MAP_COUNT_USING_NATIVE_INMEMORY_FORMAT.getRequestParameterName()));
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
        assertEquals("0", parameters.get(PhoneHomeMetrics.COUNT_OF_CACHES.getRequestParameterName()));
        assertEquals("0", parameters.get(PhoneHomeMetrics.COUNT_OF_CACHES_ALL_TIME.getRequestParameterName()));

        CachingProvider cachingProvider = createServerCachingProvider(node.hazelcastInstance);
        CacheManager cacheManager = cachingProvider.getCacheManager();
        cacheManager.createCache("hazelcast", new CacheConfig<>("hazelcast"));
        parameters = phoneHome.phoneHome(true);
        assertEquals("1", parameters.get(PhoneHomeMetrics.COUNT_OF_CACHES.getRequestParameterName()));
        assertEquals("1", parameters.get(PhoneHomeMetrics.COUNT_OF_CACHES_ALL_TIME.getRequestParameterName()));

        Cache<Object, Object> cache = cacheManager.createCache("phonehome", new CacheConfig<>("phonehome"));
        parameters = phoneHome.phoneHome(true);
        assertEquals("2", parameters.get(PhoneHomeMetrics.COUNT_OF_CACHES.getRequestParameterName()));
        assertEquals("2", parameters.get(PhoneHomeMetrics.COUNT_OF_CACHES_ALL_TIME.getRequestParameterName()));

        cacheManager.destroyCache("phonehome");
        cacheManager.destroyCache("hazelcast");
        parameters = phoneHome.phoneHome(true);
        assertEquals("0", parameters.get(PhoneHomeMetrics.COUNT_OF_CACHES.getRequestParameterName()));
        assertEquals("2", parameters.get(PhoneHomeMetrics.COUNT_OF_CACHES_ALL_TIME.getRequestParameterName()));
    }

    @Test
    public void testCacheWithWANReplication() {
        Map<String, String> parameters;
        parameters = phoneHome.phoneHome(true);
        assertEquals("0", parameters.get(PhoneHomeMetrics.CACHE_COUNT_WITH_WAN_REPLICATION.getRequestParameterName()));

        CachingProvider cachingProvider = createServerCachingProvider(node.hazelcastInstance);
        CacheManager cacheManager = cachingProvider.getCacheManager();
        CacheSimpleConfig cacheSimpleConfig = new CacheSimpleConfig();
        cacheSimpleConfig.setName("hazelcast");
        cacheSimpleConfig.setWanReplicationRef(new WanReplicationRef());
        cacheManager.createCache("hazelcast", new CacheConfig<>("hazelcast"));
        node.getConfig().addCacheConfig(cacheSimpleConfig);
        parameters = phoneHome.phoneHome(true);
        assertEquals("1", parameters.get(PhoneHomeMetrics.CACHE_COUNT_WITH_WAN_REPLICATION.getRequestParameterName()));

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
        assertEquals("-1", parameters.get(PhoneHomeMetrics.AVERAGE_PUT_LATENCY_OF_MAPS_WITHOUT_MAPSTORE.getRequestParameterName()));

        IMap<Object, Object> iMap = node.hazelcastInstance.getMap("hazelcast");
        LocalMapStatsImpl localMapStats = (LocalMapStatsImpl) iMap.getLocalMapStats();
        localMapStats.incrementPutLatencyNanos(2000000000L);
        parameters = phoneHome.phoneHome(true);
        assertEquals(String.valueOf(2000), parameters.get(PhoneHomeMetrics.AVERAGE_PUT_LATENCY_OF_MAPS_WITHOUT_MAPSTORE.getRequestParameterName()));

        localMapStats.incrementPutLatencyNanos(1000000000L);
        parameters = phoneHome.phoneHome(true);
        assertEquals(String.valueOf(1500), parameters.get(PhoneHomeMetrics.AVERAGE_PUT_LATENCY_OF_MAPS_WITHOUT_MAPSTORE.getRequestParameterName()));

        localMapStats.incrementPutLatencyNanos(2000000000L);
        parameters = phoneHome.phoneHome(true);
        assertEquals(String.valueOf(1666), parameters.get(PhoneHomeMetrics.AVERAGE_PUT_LATENCY_OF_MAPS_WITHOUT_MAPSTORE.getRequestParameterName()));
    }

    @Test
    public void testMapGetLatencyWithoutMapStore() {
        Map<String, String> parameters;
        parameters = phoneHome.phoneHome(true);
        assertEquals("-1", parameters.get(PhoneHomeMetrics.AVERAGE_GET_LATENCY_OF_MAPS_WITHOUT_MAPSTORE.getRequestParameterName()));

        IMap<Object, Object> iMap = node.hazelcastInstance.getMap("hazelcast");
        LocalMapStatsImpl localMapStats = (LocalMapStatsImpl) iMap.getLocalMapStats();
        localMapStats.incrementGetLatencyNanos(2000000000L);
        parameters = phoneHome.phoneHome(true);
        assertEquals(String.valueOf(2000), parameters.get(PhoneHomeMetrics.AVERAGE_GET_LATENCY_OF_MAPS_WITHOUT_MAPSTORE.getRequestParameterName()));

        localMapStats.incrementGetLatencyNanos(2000000000L);
        parameters = phoneHome.phoneHome(true);
        assertEquals(String.valueOf(2000), parameters.get(PhoneHomeMetrics.AVERAGE_GET_LATENCY_OF_MAPS_WITHOUT_MAPSTORE.getRequestParameterName()));
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
        assertEquals("-1", parameters.get(PhoneHomeMetrics.AVERAGE_PUT_LATENCY_OF_MAPS_USING_MAPSTORE.getRequestParameterName()));

        IMap<Object, Object> iMap1 = initialiseForMapStore("hazelcast");
        iMap1.put("key1", "hazelcast");
        iMap1.put("key2", "phonehome");
        parameters = phoneHome.phoneHome(true);
        assertGreaterOrEquals(PhoneHomeMetrics.AVERAGE_PUT_LATENCY_OF_MAPS_USING_MAPSTORE.getRequestParameterName(),
                parseLong(parameters.get(PhoneHomeMetrics.AVERAGE_PUT_LATENCY_OF_MAPS_USING_MAPSTORE.getRequestParameterName())), 200);

        IMap<Object, Object> iMap2 = initialiseForMapStore("phonehome");
        iMap2.put("key3", "hazelcast");
        parameters = phoneHome.phoneHome(true);
        assertGreaterOrEquals(PhoneHomeMetrics.AVERAGE_PUT_LATENCY_OF_MAPS_USING_MAPSTORE.getRequestParameterName(),
                parseLong(parameters.get(PhoneHomeMetrics.AVERAGE_PUT_LATENCY_OF_MAPS_USING_MAPSTORE.getRequestParameterName())), 200);
    }

    @Test
    public void testMapGetLatencyWithMapStore() {
        Map<String, String> parameters;
        parameters = phoneHome.phoneHome(true);
        assertEquals("-1", parameters.get(PhoneHomeMetrics.AVERAGE_GET_LATENCY_OF_MAPS_USING_MAPSTORE.getRequestParameterName()));

        IMap<Object, Object> iMap1 = initialiseForMapStore("hazelcast");
        iMap1.get("key1");
        iMap1.get("key2");
        parameters = phoneHome.phoneHome(true);
        assertGreaterOrEquals(PhoneHomeMetrics.AVERAGE_GET_LATENCY_OF_MAPS_USING_MAPSTORE.getRequestParameterName(),
                parseLong(parameters.get(PhoneHomeMetrics.AVERAGE_GET_LATENCY_OF_MAPS_USING_MAPSTORE.getRequestParameterName())), 200);

        IMap<Object, Object> iMap2 = initialiseForMapStore("phonehome");
        iMap2.get("key3");
        parameters = phoneHome.phoneHome(true);
        assertGreaterOrEquals(PhoneHomeMetrics.AVERAGE_GET_LATENCY_OF_MAPS_USING_MAPSTORE.getRequestParameterName(),
                parseLong(parameters.get(PhoneHomeMetrics.AVERAGE_GET_LATENCY_OF_MAPS_USING_MAPSTORE.getRequestParameterName())), 200);
    }

    @Test
    public void testJetJobsSubmitted() {
        Map<String, String> parameters = phoneHome.phoneHome(true);
        assertEquals("0", parameters.get(PhoneHomeMetrics.JET_JOBS_SUBMITTED.getRequestParameterName()));

        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(TestSources.items(1, 2, 3))
                .writeTo(Sinks.logger());
        node.hazelcastInstance.getJet().newJob(pipeline);

        parameters = phoneHome.phoneHome(true);
        assertEquals("1", parameters.get(PhoneHomeMetrics.JET_JOBS_SUBMITTED.getRequestParameterName()));
    }

    @Test
    public void testMapMemoryCost() {
        Map<String, String> parameters;
        parameters = phoneHome.phoneHome(true);
        assertThat(parameters.get(PhoneHomeMetrics.DATA_MEMORY_COST.getRequestParameterName())).isEqualTo("0");

        node.hazelcastInstance.getMap("hazelcast").put("hazelcast", "hazelcast");
        parameters = phoneHome.phoneHome(true);
        long oneMapMemoryCost = parseLong(parameters.get(PhoneHomeMetrics.DATA_MEMORY_COST.getRequestParameterName()));
        assertThat(oneMapMemoryCost).isGreaterThan(0);

        node.hazelcastInstance.getMap("hazelcast2").put("hazelcast", "hazelcast");
        parameters = phoneHome.phoneHome(true);
        long twoMapsMemoryCost = parseLong(parameters.get(PhoneHomeMetrics.DATA_MEMORY_COST.getRequestParameterName()));
        assertThat(twoMapsMemoryCost).isGreaterThan(oneMapMemoryCost);
    }

    @Test
    public void testReplicatedMapMemoryCost() {
        node.getConfig().getReplicatedMapConfig("hazelcast").setInMemoryFormat(InMemoryFormat.BINARY);
        node.getConfig().getReplicatedMapConfig("hazelcast2").setInMemoryFormat(InMemoryFormat.BINARY);

        Map<String, String> parameters;
        parameters = phoneHome.phoneHome(true);
        assertThat(parameters.get(PhoneHomeMetrics.DATA_MEMORY_COST.getRequestParameterName())).isEqualTo("0");

        node.hazelcastInstance.getReplicatedMap("hazelcast").put("hazelcast", "hazelcast");
        parameters = phoneHome.phoneHome(true);
        long oneMapMemoryCost = parseLong(parameters.get(PhoneHomeMetrics.DATA_MEMORY_COST.getRequestParameterName()));
        assertThat(oneMapMemoryCost).isGreaterThan(0);

        node.hazelcastInstance.getReplicatedMap("hazelcast2").put("hazelcast", "hazelcast");
        parameters = phoneHome.phoneHome(true);
        long twoMapsMemoryCost = parseLong(parameters.get(PhoneHomeMetrics.DATA_MEMORY_COST.getRequestParameterName()));
        assertThat(twoMapsMemoryCost).isGreaterThan(oneMapMemoryCost);
    }

    @Test
    public void testViridianPhoneHomeNullByDefault() {
        Map<String, String> parameters;
        parameters = phoneHome.phoneHome(true);
        assertThat(parameters.get(PhoneHomeMetrics.VIRIDIAN.getRequestParameterName())).isNull();
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

        INTERNAL_OBJECTS_PREFIXES.stream()
                .map(prefix -> prefix + "phonehome")
                .forEach(distributedObjectCreateFn::apply);
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get(countMetric.getRequestParameterName()), Integer.toString(initial + 2));
        assertEquals(parameters.get(totalCountMetric.getRequestParameterName()), Integer.toString(initial + 2));

        obj2.destroy();
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get(countMetric.getRequestParameterName()), Integer.toString(initial + 1));
        assertEquals(parameters.get(totalCountMetric.getRequestParameterName()), Integer.toString(initial + 2));
    }
}
