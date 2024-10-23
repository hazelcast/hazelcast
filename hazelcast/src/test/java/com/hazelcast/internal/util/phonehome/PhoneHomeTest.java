/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

import javax.cache.CacheManager;
import javax.cache.spi.CachingProvider;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.RuntimeMXBean;
import java.util.Map;
import java.util.function.Function;

import static com.hazelcast.cache.CacheTestSupport.createServerCachingProvider;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.ACTIVE_CPP_CLIENTS_COUNT;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.ACTIVE_CSHARP_CLIENTS_COUNT;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.ACTIVE_GO_CLIENTS_COUNT;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.ACTIVE_JAVA_CLIENTS_COUNT;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.ACTIVE_NODEJS_CLIENTS_COUNT;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.ACTIVE_PYTHON_CLIENTS_COUNT;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.ALL_MEMBERS_CLIENTS_COUNT;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.AVERAGE_GET_LATENCY_OF_MAPS_USING_MAPSTORE;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.AVERAGE_GET_LATENCY_OF_MAPS_WITHOUT_MAPSTORE;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.AVERAGE_PUT_LATENCY_OF_MAPS_USING_MAPSTORE;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.AVERAGE_PUT_LATENCY_OF_MAPS_WITHOUT_MAPSTORE;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.BUILD_VERSION;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.CACHE_COUNT_WITH_WAN_REPLICATION;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.CLIENT_ENDPOINT_COUNT;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.CLOSED_CPP_CLIENT_CONNECTIONS_COUNT;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.CLOSED_CSHARP_CLIENT_CONNECTIONS_COUNT;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.CLOSED_GO_CLIENT_CONNECTIONS_COUNT;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.CLOSED_JAVA_CLIENT_CONNECTIONS_COUNT;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.CLOSED_NODEJS_CLIENT_CONNECTIONS_COUNT;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.CLOSED_PYTHON_CLIENT_CONNECTIONS_COUNT;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.CLUSTER_SIZE;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.COUNT_OF_CACHES;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.COUNT_OF_CACHES_ALL_TIME;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.CPP_CLIENT_VERSIONS;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.CSHARP_CLIENT_VERSIONS;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.DATA_MEMORY_COST;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.DYNAMIC_CONFIG_PERSISTENCE_ENABLED;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.EXACT_CLUSTER_SIZE;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.GO_CLIENT_VERSIONS;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.HAZELCAST_DOWNLOAD_ID;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.JAVA_CLASSPATH;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.JAVA_CLIENT_VERSIONS;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.JAVA_VERSION_OF_SYSTEM;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.JET_ENABLED;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.JET_JOBS_SUBMITTED;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.JET_RESOURCE_UPLOAD_ENABLED;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.MAP_COUNT_USING_EVICTION;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.MAP_COUNT_USING_NATIVE_INMEMORY_FORMAT;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.MAP_COUNT_WITH_ATLEAST_ONE_ATTRIBUTE;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.MAP_COUNT_WITH_ATLEAST_ONE_INDEX;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.MAP_COUNT_WITH_ATLEAST_ONE_QUERY_CACHE;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.MAP_COUNT_WITH_HOT_RESTART_OR_PERSISTENCE_ENABLED;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.MAP_COUNT_WITH_MAP_STORE_ENABLED;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.MAP_COUNT_WITH_READ_ENABLED;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.MAP_COUNT_WITH_WAN_REPLICATION;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.MULTI_MEMBER_CLIENTS_COUNT;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.NODEJS_CLIENT_VERSIONS;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.OPENED_CPP_CLIENT_CONNECTIONS_COUNT;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.OPENED_CSHARP_CLIENT_CONNECTIONS_COUNT;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.OPENED_GO_CLIENT_CONNECTIONS_COUNT;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.OPENED_JAVA_CLIENT_CONNECTIONS_COUNT;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.OPENED_NODEJS_CLIENT_CONNECTIONS_COUNT;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.OPENED_PYTHON_CLIENT_CONNECTIONS_COUNT;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.OPERATING_SYSTEM_ARCH;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.OPERATING_SYSTEM_NAME;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.OPERATING_SYSTEM_VERSION;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.PARTITION_COUNT;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.PYTHON_CLIENT_VERSIONS;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.RUNTIME_MXBEAN_VM_NAME;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.SINGLE_MEMBER_CLIENTS_COUNT;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.TIME_TAKEN_TO_CLUSTER_UP;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.TOTAL_CPP_CLIENT_CONNECTION_DURATION;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.TOTAL_CSHARP_CLIENT_CONNECTION_DURATION;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.TOTAL_GO_CLIENT_CONNECTION_DURATION;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.TOTAL_JAVA_CLIENT_CONNECTION_DURATION;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.TOTAL_MAP_ENTRYSET_CALLS;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.TOTAL_MAP_VALUES_CALLS;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.TOTAL_NODEJS_CLIENT_CONNECTION_DURATION;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.TOTAL_PYTHON_CLIENT_CONNECTION_DURATION;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.UCN_ENABLED;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.UPTIME_OF_RUNTIME_MXBEAN;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.UUID_OF_CLUSTER;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.VIRIDIAN;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.V_CPU_COUNT;
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
    private HazelcastInstance instance;
    private Node node;
    private PhoneHome phoneHome;
    private Map<String, String> parameters;

    @Before
    public void initialise() {
        instance = createHazelcastInstance();
        node = getNode(instance);
        phoneHome = new PhoneHome(node);
    }

    private void refreshMetrics() {
        parameters = phoneHome.phoneHome(true);
    }

    private String get(Metric metric) {
        return parameters.get(metric.getQueryParameter());
    }

    @Test
    public void testPhoneHomeParameters() {
        sleepAtLeastMillis(1);
        refreshMetrics();
        RuntimeMXBean runtimeMxBean = ManagementFactory.getRuntimeMXBean();
        OperatingSystemMXBean osMxBean = ManagementFactory.getOperatingSystemMXBean();
        assertEquals(BuildInfoProvider.getBuildInfo().getVersion(), get(BUILD_VERSION));
        assertTrue(get(JAVA_CLASSPATH).endsWith(".jar"));
        assertEquals(node.getLocalMember().getUuid().toString(), get(UUID_OF_CLUSTER));
        assertNull(get(() -> "e"));
        assertNull(get(() -> "oem"));
        assertNull(get(() -> "l"));
        assertNull(get(() -> "hdgb"));
        assertEquals("source", get(HAZELCAST_DOWNLOAD_ID));
        assertEquals("A", get(CLUSTER_SIZE));
        assertEquals("1", get(EXACT_CLUSTER_SIZE));
        assertEquals("271", get(PARTITION_COUNT));
        assertEquals("A", get(CLIENT_ENDPOINT_COUNT));

        assertEquals("0", get(ACTIVE_CPP_CLIENTS_COUNT));
        assertEquals("0", get(ACTIVE_CSHARP_CLIENTS_COUNT));
        assertEquals("0", get(ACTIVE_JAVA_CLIENTS_COUNT));
        assertEquals("0", get(ACTIVE_NODEJS_CLIENTS_COUNT));
        assertEquals("0", get(ACTIVE_PYTHON_CLIENTS_COUNT));
        assertEquals("0", get(ACTIVE_GO_CLIENTS_COUNT));

        assertEquals("0", get(OPENED_CPP_CLIENT_CONNECTIONS_COUNT));
        assertEquals("0", get(OPENED_CSHARP_CLIENT_CONNECTIONS_COUNT));
        assertEquals("0", get(OPENED_JAVA_CLIENT_CONNECTIONS_COUNT));
        assertEquals("0", get(OPENED_NODEJS_CLIENT_CONNECTIONS_COUNT));
        assertEquals("0", get(OPENED_PYTHON_CLIENT_CONNECTIONS_COUNT));
        assertEquals("0", get(OPENED_GO_CLIENT_CONNECTIONS_COUNT));

        assertEquals("0", get(CLOSED_CPP_CLIENT_CONNECTIONS_COUNT));
        assertEquals("0", get(CLOSED_CSHARP_CLIENT_CONNECTIONS_COUNT));
        assertEquals("0", get(CLOSED_JAVA_CLIENT_CONNECTIONS_COUNT));
        assertEquals("0", get(CLOSED_NODEJS_CLIENT_CONNECTIONS_COUNT));
        assertEquals("0", get(CLOSED_PYTHON_CLIENT_CONNECTIONS_COUNT));
        assertEquals("0", get(CLOSED_GO_CLIENT_CONNECTIONS_COUNT));

        assertEquals("0", get(TOTAL_CPP_CLIENT_CONNECTION_DURATION));
        assertEquals("0", get(TOTAL_CSHARP_CLIENT_CONNECTION_DURATION));
        assertEquals("0", get(TOTAL_JAVA_CLIENT_CONNECTION_DURATION));
        assertEquals("0", get(TOTAL_NODEJS_CLIENT_CONNECTION_DURATION));
        assertEquals("0", get(TOTAL_PYTHON_CLIENT_CONNECTION_DURATION));
        assertEquals("0", get(TOTAL_GO_CLIENT_CONNECTION_DURATION));

        assertEquals("", get(CPP_CLIENT_VERSIONS));
        assertEquals("", get(CSHARP_CLIENT_VERSIONS));
        assertEquals("", get(JAVA_CLIENT_VERSIONS));
        assertEquals("", get(NODEJS_CLIENT_VERSIONS));
        assertEquals("", get(PYTHON_CLIENT_VERSIONS));
        assertEquals("", get(GO_CLIENT_VERSIONS));

        assertFalse(Integer.parseInt(get(TIME_TAKEN_TO_CLUSTER_UP)) < 0);
        assertNotEquals("0", get(UPTIME_OF_RUNTIME_MXBEAN));
        assertNotEquals(get(UPTIME_OF_RUNTIME_MXBEAN), get(TIME_TAKEN_TO_CLUSTER_UP));
        assertEquals(osMxBean.getName(), get(OPERATING_SYSTEM_NAME));
        assertEquals(osMxBean.getArch(), get(OPERATING_SYSTEM_ARCH));
        assertEquals(osMxBean.getVersion(), get(OPERATING_SYSTEM_VERSION));
        assertEquals(runtimeMxBean.getVmName(), get(RUNTIME_MXBEAN_VM_NAME));
        assertEquals(System.getProperty("java.version"), get(JAVA_VERSION_OF_SYSTEM));
        assertEquals("true", get(JET_ENABLED));
        assertEquals("false", get(JET_RESOURCE_UPLOAD_ENABLED));
        assertEquals("false", get(DYNAMIC_CONFIG_PERSISTENCE_ENABLED));
        assertEquals("false", get(UCN_ENABLED));

        assertEquals(0, Integer.parseInt(get(ALL_MEMBERS_CLIENTS_COUNT)));
        assertEquals(0, Integer.parseInt(get(MULTI_MEMBER_CLIENTS_COUNT)));
        assertEquals(0, Integer.parseInt(get(SINGLE_MEMBER_CLIENTS_COUNT)));

        assertThat(Integer.valueOf(get(V_CPU_COUNT))).isPositive();
    }

    @Test
    public void testConvertToLetter() {
        assertEquals("A", MetricsProvider.convertToLetter(4));
        assertEquals("B", MetricsProvider.convertToLetter(9));
        assertEquals("C", MetricsProvider.convertToLetter(19));
        assertEquals("D", MetricsProvider.convertToLetter(39));
        assertEquals("E", MetricsProvider.convertToLetter(59));
        assertEquals("F", MetricsProvider.convertToLetter(99));
        assertEquals("G", MetricsProvider.convertToLetter(149));
        assertEquals("H", MetricsProvider.convertToLetter(299));
        assertEquals("J", MetricsProvider.convertToLetter(599));
        assertEquals("I", MetricsProvider.convertToLetter(1000));
    }

    @Test
    public void testMapCount() {
        testCounts(instance::getMap, 0,
                PhoneHomeMetrics.COUNT_OF_MAPS,
                PhoneHomeMetrics.COUNT_OF_MAPS_ALL_TIME);
    }

    @Test
    public void testMapCountWithBackupReadEnabled() {
        refreshMetrics();
        assertEquals("0", get(MAP_COUNT_WITH_READ_ENABLED));

        instance.getMap("hazelcast");
        refreshMetrics();
        assertEquals("0", get(MAP_COUNT_WITH_READ_ENABLED));

        node.getConfig().getMapConfig("hazelcast").setReadBackupData(true);
        refreshMetrics();
        assertEquals("1", get(MAP_COUNT_WITH_READ_ENABLED));
    }

    @Test
    public void testMapCountWithMapStoreEnabled() {
        refreshMetrics();
        assertEquals("0", get(MAP_COUNT_WITH_MAP_STORE_ENABLED));

        instance.getMap("hazelcast");
        refreshMetrics();
        assertEquals("0", get(MAP_COUNT_WITH_MAP_STORE_ENABLED));

        node.getConfig().getMapConfig("hazelcast").getMapStoreConfig().setEnabled(true);
        refreshMetrics();
        assertEquals("1", get(MAP_COUNT_WITH_MAP_STORE_ENABLED));
    }

    @Test
    public void testMapCountWithAtLeastOneQueryCache() {
        refreshMetrics();
        assertEquals("0", get(MAP_COUNT_WITH_ATLEAST_ONE_QUERY_CACHE));

        instance.getMap("hazelcast");
        refreshMetrics();
        assertEquals("0", get(MAP_COUNT_WITH_ATLEAST_ONE_QUERY_CACHE));

        QueryCacheConfig cacheConfig = new QueryCacheConfig();
        cacheConfig.setName("hazelcastconfig");
        node.getConfig().getMapConfig("hazelcast").addQueryCacheConfig(cacheConfig);
        refreshMetrics();
        assertEquals("1", get(MAP_COUNT_WITH_ATLEAST_ONE_QUERY_CACHE));

        cacheConfig = new QueryCacheConfig();
        cacheConfig.setName("hazelcastconfig2");
        node.getConfig().getMapConfig("hazelcast").addQueryCacheConfig(cacheConfig);
        refreshMetrics();
        assertEquals("1", get(MAP_COUNT_WITH_ATLEAST_ONE_QUERY_CACHE));
    }

    @Test
    public void testMapCountWithAtLeastOneIndex() {
        refreshMetrics();
        assertEquals("0", get(MAP_COUNT_WITH_ATLEAST_ONE_INDEX));

        instance.getMap("hazelcast");
        refreshMetrics();
        assertEquals("0", get(MAP_COUNT_WITH_ATLEAST_ONE_INDEX));

        IndexConfig config = new IndexConfig(IndexType.SORTED, "hazelcast");
        node.getConfig().getMapConfig("hazelcast").getIndexConfigs().add(config);
        refreshMetrics();
        assertEquals("1", get(MAP_COUNT_WITH_ATLEAST_ONE_INDEX));

        config = new IndexConfig(IndexType.HASH, "phonehome");
        node.getConfig().getMapConfig("hazelcast").getIndexConfigs().add(config);
        refreshMetrics();
        assertEquals("1", get(MAP_COUNT_WITH_ATLEAST_ONE_INDEX));
    }

    @Test
    public void testMapCountWithHotRestartEnabled() {
        refreshMetrics();
        assertEquals("0", get(MAP_COUNT_WITH_HOT_RESTART_OR_PERSISTENCE_ENABLED));

        instance.getMap("hazelcast");
        refreshMetrics();
        assertEquals("0", get(MAP_COUNT_WITH_HOT_RESTART_OR_PERSISTENCE_ENABLED));

        instance.getMap("with-hot-restart");
        node.getConfig().getMapConfig("with-hot-restart").getHotRestartConfig().setEnabled(true);
        refreshMetrics();
        assertEquals("1", get(MAP_COUNT_WITH_HOT_RESTART_OR_PERSISTENCE_ENABLED));

        instance.getMap("with-persistence");
        node.getConfig().getMapConfig("with-persistence").getDataPersistenceConfig().setEnabled(true);
        refreshMetrics();
        assertEquals("2", get(MAP_COUNT_WITH_HOT_RESTART_OR_PERSISTENCE_ENABLED));

        instance.getMap("with-both");
        node.getConfig().getMapConfig("with-both").getHotRestartConfig().setEnabled(true);
        node.getConfig().getMapConfig("with-both").getDataPersistenceConfig().setEnabled(true);
        refreshMetrics();
        assertEquals("3", get(MAP_COUNT_WITH_HOT_RESTART_OR_PERSISTENCE_ENABLED));
    }

    @Test
    public void testMapCountWithWANReplication() {
        refreshMetrics();
        assertEquals("0", get(MAP_COUNT_WITH_WAN_REPLICATION));

        instance.getMap("hazelcast");
        refreshMetrics();
        assertEquals("0", get(MAP_COUNT_WITH_WAN_REPLICATION));

        node.getConfig().getMapConfig("hazelcast").setWanReplicationRef(new WanReplicationRef());
        refreshMetrics();
        assertEquals("1", get(MAP_COUNT_WITH_WAN_REPLICATION));
    }

    @Test
    public void testMapCountWithAtleastOneAttribute() {
        refreshMetrics();
        assertEquals("0", get(MAP_COUNT_WITH_ATLEAST_ONE_ATTRIBUTE));

        instance.getMap("hazelcast");
        refreshMetrics();
        assertEquals("0", get(MAP_COUNT_WITH_ATLEAST_ONE_ATTRIBUTE));

        node.getConfig().getMapConfig("hazelcast").getAttributeConfigs().add(new AttributeConfig());
        refreshMetrics();
        assertEquals("1", get(MAP_COUNT_WITH_ATLEAST_ONE_ATTRIBUTE));

        node.getConfig().getMapConfig("hazelcast").getAttributeConfigs().add(new AttributeConfig());
        refreshMetrics();
        assertEquals("1", get(MAP_COUNT_WITH_ATLEAST_ONE_ATTRIBUTE));
    }

    @Test
    public void testMapCountUsingEviction() {
        refreshMetrics();
        assertEquals("0", get(MAP_COUNT_USING_EVICTION));

        instance.getMap("hazelcast");
        refreshMetrics();
        assertEquals("0", get(MAP_COUNT_USING_EVICTION));

        EvictionConfig config = new EvictionConfig();
        config.setEvictionPolicy(EvictionPolicy.LRU);
        node.getConfig().getMapConfig("hazelcast").setEvictionConfig(config);
        refreshMetrics();
        assertEquals("1", get(MAP_COUNT_USING_EVICTION));

        config.setEvictionPolicy(EvictionPolicy.NONE);
        node.getConfig().getMapConfig("hazelcast").setEvictionConfig(config);
        refreshMetrics();
        assertEquals("0", get(MAP_COUNT_USING_EVICTION));
    }

    @Test
    public void testMapCountWithNativeInMemory() {
        refreshMetrics();
        assertEquals("0", get(MAP_COUNT_USING_NATIVE_INMEMORY_FORMAT));

        instance.getMap("hazelcast");
        refreshMetrics();
        assertEquals("0", get(MAP_COUNT_USING_NATIVE_INMEMORY_FORMAT));

        node.getConfig().getMapConfig("hazelcast").setInMemoryFormat(InMemoryFormat.NATIVE);
        refreshMetrics();
        assertEquals("1", get(MAP_COUNT_USING_NATIVE_INMEMORY_FORMAT));
    }

    @Test
    public void testSetCount() {
        testCounts(instance::getSet, 0,
                PhoneHomeMetrics.COUNT_OF_SETS,
                PhoneHomeMetrics.COUNT_OF_SETS_ALL_TIME);
    }

    @Test
    public void testQueueCount() {
        testCounts(instance::getQueue, 0,
                PhoneHomeMetrics.COUNT_OF_QUEUES,
                PhoneHomeMetrics.COUNT_OF_QUEUES_ALL_TIME);
    }

    @Test
    public void testMultimapCount() {
        testCounts(instance::getMultiMap, 0,
                PhoneHomeMetrics.COUNT_OF_MULTIMAPS,
                PhoneHomeMetrics.COUNT_OF_MULTIMAPS_ALL_TIME);
    }

    @Test
    public void testListCount() {
        testCounts(instance::getList, 0,
                PhoneHomeMetrics.COUNT_OF_LISTS,
                PhoneHomeMetrics.COUNT_OF_LISTS_ALL_TIME);
    }

    @Test
    public void testRingBufferCount() {
        testCounts(instance::getRingbuffer, 0,
                PhoneHomeMetrics.COUNT_OF_RING_BUFFERS,
                PhoneHomeMetrics.COUNT_OF_RING_BUFFERS_ALL_TIME);
    }

    @Test
    public void testCacheCount() {
        refreshMetrics();
        assertEquals("0", get(COUNT_OF_CACHES));
        assertEquals("0", get(COUNT_OF_CACHES_ALL_TIME));

        CachingProvider cachingProvider = createServerCachingProvider(instance);
        CacheManager cacheManager = cachingProvider.getCacheManager();
        cacheManager.createCache("hazelcast", new CacheConfig<>("hazelcast"));
        refreshMetrics();
        assertEquals("1", get(COUNT_OF_CACHES));
        assertEquals("1", get(COUNT_OF_CACHES_ALL_TIME));

        cacheManager.createCache("phonehome", new CacheConfig<>("phonehome"));
        refreshMetrics();
        assertEquals("2", get(COUNT_OF_CACHES));
        assertEquals("2", get(COUNT_OF_CACHES_ALL_TIME));

        cacheManager.destroyCache("phonehome");
        cacheManager.destroyCache("hazelcast");
        refreshMetrics();
        assertEquals("0", get(COUNT_OF_CACHES));
        assertEquals("2", get(COUNT_OF_CACHES_ALL_TIME));
    }

    @Test
    public void testCacheWithWANReplication() {
        refreshMetrics();
        assertEquals("0", get(CACHE_COUNT_WITH_WAN_REPLICATION));

        CachingProvider cachingProvider = createServerCachingProvider(instance);
        CacheManager cacheManager = cachingProvider.getCacheManager();
        CacheSimpleConfig cacheSimpleConfig = new CacheSimpleConfig();
        cacheSimpleConfig.setName("hazelcast");
        cacheSimpleConfig.setWanReplicationRef(new WanReplicationRef());
        cacheManager.createCache("hazelcast", new CacheConfig<>("hazelcast"));
        node.getConfig().addCacheConfig(cacheSimpleConfig);
        refreshMetrics();
        assertEquals("1", get(CACHE_COUNT_WITH_WAN_REPLICATION));
    }

    @Test
    public void testTopicCount() {
        testCounts(instance::getTopic, 0,
                PhoneHomeMetrics.COUNT_OF_TOPICS,
                PhoneHomeMetrics.COUNT_OF_TOPICS_ALL_TIME);
    }

    @Test
    public void testReplicatedMapCount() {
        testCounts(instance::getReplicatedMap, 0,
                PhoneHomeMetrics.COUNT_OF_REPLICATED_MAPS,
                PhoneHomeMetrics.COUNT_OF_REPLICATED_MAPS_ALL_TIME);
    }

    @Test
    public void testCardinalityEstimatorMapCount() {
        testCounts(instance::getCardinalityEstimator, 0,
                PhoneHomeMetrics.COUNT_OF_CARDINALITY_ESTIMATORS,
                PhoneHomeMetrics.COUNT_OF_CARDINALITY_ESTIMATORS_ALL_TIME);
    }

    @Test
    public void testPNCounterCount() {
        testCounts(instance::getPNCounter, 0,
                PhoneHomeMetrics.COUNT_OF_PN_COUNTERS,
                PhoneHomeMetrics.COUNT_OF_PN_COUNTERS_ALL_TIME);
    }

    @Test
    public void testFlakeIDGeneratorCount() {
        testCounts(instance::getFlakeIdGenerator, 0,
                PhoneHomeMetrics.COUNT_OF_FLAKE_ID_GENERATORS,
                PhoneHomeMetrics.COUNT_OF_FLAKE_ID_GENERATORS_ALL_TIME);
    }

    @Test
    public void testMapPutLatencyWithoutMapStore() {
        refreshMetrics();
        assertEquals("-1", get(AVERAGE_PUT_LATENCY_OF_MAPS_WITHOUT_MAPSTORE));

        IMap<Object, Object> iMap = instance.getMap("hazelcast");
        LocalMapStatsImpl localMapStats = (LocalMapStatsImpl) iMap.getLocalMapStats();
        localMapStats.incrementPutLatencyNanos(2000000000L);
        refreshMetrics();
        assertEquals(String.valueOf(2000), get(AVERAGE_PUT_LATENCY_OF_MAPS_WITHOUT_MAPSTORE));

        localMapStats.incrementPutLatencyNanos(1000000000L);
        refreshMetrics();
        assertEquals(String.valueOf(1500), get(AVERAGE_PUT_LATENCY_OF_MAPS_WITHOUT_MAPSTORE));

        localMapStats.incrementPutLatencyNanos(2000000000L);
        refreshMetrics();
        assertEquals(String.valueOf(1666), get(AVERAGE_PUT_LATENCY_OF_MAPS_WITHOUT_MAPSTORE));
    }

    @Test
    public void testMapGetLatencyWithoutMapStore() {
        refreshMetrics();
        assertEquals("-1", get(AVERAGE_GET_LATENCY_OF_MAPS_WITHOUT_MAPSTORE));

        IMap<Object, Object> iMap = instance.getMap("hazelcast");
        LocalMapStatsImpl localMapStats = (LocalMapStatsImpl) iMap.getLocalMapStats();
        localMapStats.incrementGetLatencyNanos(2000000000L);
        refreshMetrics();
        assertEquals(String.valueOf(2000), get(AVERAGE_GET_LATENCY_OF_MAPS_WITHOUT_MAPSTORE));

        localMapStats.incrementGetLatencyNanos(2000000000L);
        refreshMetrics();
        assertEquals(String.valueOf(2000), get(AVERAGE_GET_LATENCY_OF_MAPS_WITHOUT_MAPSTORE));
    }

    private IMap<Object, Object> initialiseForMapStore(String mapName) {
        IMap<Object, Object> iMap = instance.getMap(mapName);
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setEnabled(true);
        mapStoreConfig.setImplementation(new DelayMapStore());
        node.getConfig().getMapConfig(mapName).setMapStoreConfig(mapStoreConfig);
        return iMap;
    }

    @Test
    public void testMapValuesCalls() {
        refreshMetrics();

        instance.getMap(randomMapName()).values();
        instance.getMap(randomMapName()).values();

        refreshMetrics();
        assertEquals(2L, parseLong(get(TOTAL_MAP_VALUES_CALLS)));
    }

    @Test
    public void testMapEntrySetCalls() {
        refreshMetrics();

        instance.getMap(randomMapName()).entrySet();
        instance.getMap(randomMapName()).entrySet();
        instance.getMap(randomMapName()).entrySet();

        refreshMetrics();
        assertEquals(3L, parseLong(get(TOTAL_MAP_ENTRYSET_CALLS)));
    }


    @Test
    public void testMapPutLatencyWithMapStore() {
        refreshMetrics();
        assertEquals("-1", get(AVERAGE_PUT_LATENCY_OF_MAPS_USING_MAPSTORE));

        IMap<Object, Object> iMap1 = initialiseForMapStore("hazelcast");
        iMap1.put("key1", "hazelcast");
        iMap1.put("key2", "phonehome");
        refreshMetrics();
        assertGreaterOrEquals(AVERAGE_PUT_LATENCY_OF_MAPS_USING_MAPSTORE.getQueryParameter(),
                parseLong(get(AVERAGE_PUT_LATENCY_OF_MAPS_USING_MAPSTORE)), 200);

        IMap<Object, Object> iMap2 = initialiseForMapStore("phonehome");
        iMap2.put("key3", "hazelcast");
        refreshMetrics();
        assertGreaterOrEquals(AVERAGE_PUT_LATENCY_OF_MAPS_USING_MAPSTORE.getQueryParameter(),
                parseLong(get(AVERAGE_PUT_LATENCY_OF_MAPS_USING_MAPSTORE)), 200);
    }

    @Test
    public void testMapGetLatencyWithMapStore() {
        refreshMetrics();
        assertEquals("-1", get(AVERAGE_GET_LATENCY_OF_MAPS_USING_MAPSTORE));

        IMap<Object, Object> iMap1 = initialiseForMapStore("hazelcast");
        iMap1.get("key1");
        iMap1.get("key2");
        refreshMetrics();
        assertGreaterOrEquals(AVERAGE_GET_LATENCY_OF_MAPS_USING_MAPSTORE.getQueryParameter(),
                parseLong(get(AVERAGE_GET_LATENCY_OF_MAPS_USING_MAPSTORE)), 200);

        IMap<Object, Object> iMap2 = initialiseForMapStore("phonehome");
        iMap2.get("key3");
        refreshMetrics();
        assertGreaterOrEquals(AVERAGE_GET_LATENCY_OF_MAPS_USING_MAPSTORE.getQueryParameter(),
                parseLong(get(AVERAGE_GET_LATENCY_OF_MAPS_USING_MAPSTORE)), 200);
    }

    @Test
    public void testJetJobsSubmitted() {
        refreshMetrics();
        assertEquals("0", get(JET_JOBS_SUBMITTED));

        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(TestSources.items(1, 2, 3))
                .writeTo(Sinks.logger());
        instance.getJet().newJob(pipeline);

        refreshMetrics();
        assertEquals("1", get(JET_JOBS_SUBMITTED));
    }

    @Test
    public void testMapMemoryCost() {
        refreshMetrics();
        assertThat(get(DATA_MEMORY_COST)).isEqualTo("0");

        instance.getMap("hazelcast").put("hazelcast", "hazelcast");
        refreshMetrics();
        long oneMapMemoryCost = parseLong(get(DATA_MEMORY_COST));
        assertThat(oneMapMemoryCost).isPositive();

        instance.getMap("hazelcast2").put("hazelcast", "hazelcast");
        refreshMetrics();
        long twoMapsMemoryCost = parseLong(get(DATA_MEMORY_COST));
        assertThat(twoMapsMemoryCost).isGreaterThan(oneMapMemoryCost);
    }

    @Test
    public void testReplicatedMapMemoryCost() {
        node.getConfig().getReplicatedMapConfig("hazelcast").setInMemoryFormat(InMemoryFormat.BINARY);
        node.getConfig().getReplicatedMapConfig("hazelcast2").setInMemoryFormat(InMemoryFormat.BINARY);

        refreshMetrics();
        assertThat(get(DATA_MEMORY_COST)).isEqualTo("0");

        instance.getReplicatedMap("hazelcast").put("hazelcast", "hazelcast");
        refreshMetrics();
        long oneMapMemoryCost = parseLong(get(DATA_MEMORY_COST));
        assertThat(oneMapMemoryCost).isPositive();

        instance.getReplicatedMap("hazelcast2").put("hazelcast", "hazelcast");
        refreshMetrics();
        long twoMapsMemoryCost = parseLong(get(DATA_MEMORY_COST));
        assertThat(twoMapsMemoryCost).isGreaterThan(oneMapMemoryCost);
    }

    @Test
    public void testViridianPhoneHomeNullByDefault() {
        refreshMetrics();
        assertThat(get(VIRIDIAN)).isNull();
    }

    private void testCounts(Function<String, ? extends DistributedObject> distributedObjectCreateFn,
                            int initial, Metric countMetric, Metric totalCountMetric) {
        refreshMetrics();
        assertEquals(get(countMetric), Integer.toString(initial));
        assertEquals(get(totalCountMetric), Integer.toString(initial));

        distributedObjectCreateFn.apply("hazelcast");
        refreshMetrics();
        assertEquals(get(countMetric), Integer.toString(initial + 1));
        assertEquals(get(totalCountMetric), Integer.toString(initial + 1));

        DistributedObject obj2 = distributedObjectCreateFn.apply("phonehome");
        refreshMetrics();
        assertEquals(get(countMetric), Integer.toString(initial + 2));
        assertEquals(get(totalCountMetric), Integer.toString(initial + 2));

        INTERNAL_OBJECTS_PREFIXES.stream()
                .map(prefix -> prefix + "phonehome")
                .forEach(distributedObjectCreateFn::apply);
        refreshMetrics();
        assertEquals(get(countMetric), Integer.toString(initial + 2));
        assertEquals(get(totalCountMetric), Integer.toString(initial + 2));

        obj2.destroy();
        refreshMetrics();
        assertEquals(get(countMetric), Integer.toString(initial + 1));
        assertEquals(get(totalCountMetric), Integer.toString(initial + 2));
    }
}
