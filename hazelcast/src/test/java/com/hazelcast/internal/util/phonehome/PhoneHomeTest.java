/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cardinality.CardinalityEstimator;
import com.hazelcast.collection.IList;
import com.hazelcast.collection.IQueue;
import com.hazelcast.collection.ISet;
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
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.crdt.pncounter.PNCounter;
import com.hazelcast.flakeidgen.FlakeIdGenerator;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.monitor.impl.LocalMapStatsImpl;
import com.hazelcast.map.IMap;
import com.hazelcast.multimap.MultiMap;
import com.hazelcast.replicatedmap.ReplicatedMap;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.topic.ITopic;
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
import java.util.UUID;

import static com.hazelcast.cache.CacheTestSupport.createServerCachingProvider;
import static com.hazelcast.test.Accessors.getNode;
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
        assertEquals(parameters.get("version"), BuildInfoProvider.getBuildInfo().getVersion());
        assertEquals(UUID.fromString(parameters.get("m")), node.getLocalMember().getUuid());
        assertNull(parameters.get("e"));
        assertNull(parameters.get("oem"));
        assertNull(parameters.get("l"));
        assertNull(parameters.get("hdgb"));
        assertEquals(parameters.get("p"), "source");
        assertEquals(parameters.get("crsz"), "A");
        assertEquals(parameters.get("cssz"), "A");
        assertEquals(parameters.get("ccpp"), "0");
        assertEquals(parameters.get("cdn"), "0");
        assertEquals(parameters.get("cjv"), "0");
        assertEquals(parameters.get("cnjs"), "0");
        assertEquals(parameters.get("cpy"), "0");
        assertEquals(parameters.get("cgo"), "0");
        assertEquals(parameters.get("jetv"), "");
        assertFalse(Integer.parseInt(parameters.get("cuptm")) < 0);
        assertNotEquals(parameters.get("nuptm"), "0");
        assertNotEquals(parameters.get("nuptm"), parameters.get("cuptm"));
        assertEquals(parameters.get("osn"), osMxBean.getName());
        assertEquals(parameters.get("osa"), osMxBean.getArch());
        assertEquals(parameters.get("osv"), osMxBean.getVersion());
        assertEquals(parameters.get("jvmn"), runtimeMxBean.getVmName());
        assertEquals(parameters.get("jvmv"), System.getProperty("java.version"));
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
        Map<String, String> parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("mpct"), "0");
        Map<String, String> map1 = node.hazelcastInstance.getMap("hazelcast");
        Map<String, String> map2 = node.hazelcastInstance.getMap("phonehome");
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("mpct"), "2");
        Map<String, String> map3 = node.hazelcastInstance.getMap("maps");
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("mpct"), "3");
    }

    @Test
    public void testMapCountWithBackupReadEnabled() {
        Map<String, String> parameters;
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("mpbrct"), "0");

        Map<String, String> map1 = node.hazelcastInstance.getMap("hazelcast");
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("mpbrct"), "0");

        node.getConfig().getMapConfig("hazelcast").setReadBackupData(true);
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("mpbrct"), "1");
    }

    @Test
    public void testMapCountWithMapStoreEnabled() {
        Map<String, String> parameters;
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("mpmsct"), "0");

        Map<String, String> map1 = node.hazelcastInstance.getMap("hazelcast");
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("mpmsct"), "0");

        node.getConfig().getMapConfig("hazelcast").getMapStoreConfig().setEnabled(true);
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("mpmsct"), "1");
    }

    @Test
    public void testMapCountWithAtleastOneQueryCache() {
        Map<String, String> parameters;
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("mpaoqcct"), "0");

        Map<String, String> map1 = node.hazelcastInstance.getMap("hazelcast");
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("mpaoqcct"), "0");

        QueryCacheConfig cacheConfig = new QueryCacheConfig();
        cacheConfig.setName("hazelcastconfig");
        node.getConfig().getMapConfig("hazelcast").addQueryCacheConfig(cacheConfig);
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("mpaoqcct"), "1");

        cacheConfig = new QueryCacheConfig();
        cacheConfig.setName("hazelcastconfig2");
        node.getConfig().getMapConfig("hazelcast").addQueryCacheConfig(cacheConfig);
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("mpaoqcct"), "1");

    }

    @Test
    public void testMapCountWithAtleastOneIndex() {
        Map<String, String> parameters;
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("mpaoict"), "0");

        Map<String, String> map1 = node.hazelcastInstance.getMap("hazelcast");
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("mpaoict"), "0");

        IndexConfig config = new IndexConfig(IndexType.SORTED, "hazelcast");
        node.getConfig().getMapConfig("hazelcast").getIndexConfigs().add(config);
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("mpaoict"), "1");

        config = new IndexConfig(IndexType.HASH, "phonehome");
        node.getConfig().getMapConfig("hazelcast").getIndexConfigs().add(config);
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("mpaoict"), "1");

    }

    @Test
    public void testMapCountWithHotRestartEnabled() {
        Map<String, String> parameters;
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("mphect"), "0");

        Map<String, String> map1 = node.hazelcastInstance.getMap("hazelcast");
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("mphect"), "0");

        node.getConfig().getMapConfig("hazelcast").getHotRestartConfig().setEnabled(true);
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("mphect"), "1");

    }

    @Test
    public void testMapCountWithWANReplication() {
        Map<String, String> parameters;
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("mpwact"), "0");

        Map<String, String> map1 = node.hazelcastInstance.getMap("hazelcast");
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("mpwact"), "0");

        node.getConfig().getMapConfig("hazelcast").setWanReplicationRef(new WanReplicationRef());
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("mpwact"), "1");

    }

    @Test
    public void testMapCountWithAtleastOneAttribute() {
        Map<String, String> parameters;
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("mpaocct"), "0");

        Map<String, String> map1 = node.hazelcastInstance.getMap("hazelcast");
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("mpaocct"), "0");

        node.getConfig().getMapConfig("hazelcast").getAttributeConfigs().add(new AttributeConfig());
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("mpaocct"), "1");

        node.getConfig().getMapConfig("hazelcast").getAttributeConfigs().add(new AttributeConfig());
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("mpaocct"), "1");

    }

    @Test
    public void testMapCountUsingEviction() {
        Map<String, String> parameters;
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("mpevct"), "0");

        Map<String, String> map1 = node.hazelcastInstance.getMap("hazelcast");
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("mpevct"), "0");

        EvictionConfig config = new EvictionConfig();
        config.setEvictionPolicy(EvictionPolicy.LRU);
        node.getConfig().getMapConfig("hazelcast").setEvictionConfig(config);
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("mpevct"), "1");

        config.setEvictionPolicy(EvictionPolicy.NONE);
        node.getConfig().getMapConfig("hazelcast").setEvictionConfig(config);
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("mpevct"), "0");

    }

    @Test
    public void testMapCountWithNativeInMemory() {
        Map<String, String> parameters;
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("mpnmct"), "0");

        Map<String, String> map1 = node.hazelcastInstance.getMap("hazelcast");
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("mpnmct"), "0");

        node.getConfig().getMapConfig("hazelcast").setInMemoryFormat(InMemoryFormat.NATIVE);
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("mpnmct"), "1");

    }

    @Test
    public void testSetCount() {
        Map<String, String> parameters;
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("sect"), "0");

        ISet<String> set1 = node.hazelcastInstance.getSet("hazelcast");
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("sect"), "1");

        ISet<Object> set2 = node.hazelcastInstance.getSet("phonehome");
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("sect"), "2");

        set2.destroy();
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("sect"), "1");
    }

    @Test
    public void testQueueCount() {
        Map<String, String> parameters;
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("quct"), "0");

        IQueue<Object> queue1 = node.hazelcastInstance.getQueue("hazelcast");
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("quct"), "1");

        IQueue<String> queue2 = node.hazelcastInstance.getQueue("phonehome");
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("quct"), "2");

        queue2.destroy();
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("quct"), "1");
    }

    @Test
    public void testMultimapCount() {
        Map<String, String> parameters;
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("mmct"), "0");

        MultiMap<Object, Object> multimap1 = node.hazelcastInstance.getMultiMap("hazelcast");
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("mmct"), "1");

        MultiMap<Object, Object> multimap2 = node.hazelcastInstance.getMultiMap("phonehome");
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("mmct"), "2");
    }

    @Test
    public void testListCount() {
        Map<String, String> parameters;
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("lict"), "0");

        IList<Object> list1 = node.hazelcastInstance.getList("hazelcast");
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("lict"), "1");

        IList<Object> list2 = node.hazelcastInstance.getList("phonehome");
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("lict"), "2");
    }

    @Test
    public void testRingBufferCount() {
        Map<String, String> parameters;
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("rbct"), "0");

        Ringbuffer<Object> ringbuffer1 = node.hazelcastInstance.getRingbuffer("hazelcast");
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("rbct"), "1");

        Ringbuffer<Object> ringbuffer2 = node.hazelcastInstance.getRingbuffer("phonehome");
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("rbct"), "2");
    }

    @Test
    public void testCacheCount() {
        Map<String, String> parameters;
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("cact"), "0");

        CachingProvider cachingProvider = createServerCachingProvider(node.hazelcastInstance);
        CacheManager cacheManager = cachingProvider.getCacheManager();
        cacheManager.createCache("hazelcast", new CacheConfig<>("hazelcast"));

        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("cact"), "1");

        cacheManager.createCache("phonehome", new CacheConfig<>("phonehome"));

        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("cact"), "2");
    }

    @Test
    public void testCacheWithWANReplication() {
        Map<String, String> parameters;
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("cawact"), "0");

        CachingProvider cachingProvider = createServerCachingProvider(node.hazelcastInstance);
        CacheManager cacheManager = cachingProvider.getCacheManager();
        CacheSimpleConfig cacheSimpleConfig = new CacheSimpleConfig();
        cacheSimpleConfig.setName("hazelcast");
        cacheSimpleConfig.setWanReplicationRef(new WanReplicationRef());
        cacheManager.createCache("hazelcast", new CacheConfig<>("hazelcast"));
        node.getConfig().addCacheConfig(cacheSimpleConfig);
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("cawact"), "1");

    }

    @Test
    public void testTopicCount() {
        Map<String, String> parameters;
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("tpct"), "0");

        ITopic<String> topic1 = node.hazelcastInstance.getTopic("hazelcast");
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("tpct"), "1");

        ITopic<String> topic2 = node.hazelcastInstance.getTopic("phonehome");
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("tpct"), "2");

    }

    @Test
    public void testReplicatedMapCount() {
        Map<String, String> parameters;
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("rpct"), "0");

        ReplicatedMap<String, String> replicatedMap1 = node.hazelcastInstance.getReplicatedMap("hazelcast");
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("rpct"), "1");

        ReplicatedMap<String, String> replicatedMap2 = node.hazelcastInstance.getReplicatedMap("phonehome");
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("rpct"), "2");

    }

    @Test
    public void testCardinalityEstimatorMapCount() {
        Map<String, String> parameters;
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("cect"), "0");

        CardinalityEstimator cardinalityEstimator1 = node.hazelcastInstance.getCardinalityEstimator("hazelcast");
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("cect"), "1");

        CardinalityEstimator cardinalityEstimator2 = node.hazelcastInstance.getCardinalityEstimator("phonehome");
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("cect"), "2");

    }

    @Test
    public void testPNCounterCount() {
        Map<String, String> parameters;
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("pncct"), "0");

        PNCounter pnCounter1 = node.hazelcastInstance.getPNCounter("hazelcast");
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("pncct"), "1");

        PNCounter pnCounter2 = node.hazelcastInstance.getPNCounter("phonehome");
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("pncct"), "2");

    }

    @Test
    public void testFlakeIDGeneratorCount() {
        Map<String, String> parameters;
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("figct"), "0");

        FlakeIdGenerator flakeIdGenerator1 = node.hazelcastInstance.getFlakeIdGenerator("hazelcast");
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("figct"), "1");

        FlakeIdGenerator flakeIdGenerator2 = node.hazelcastInstance.getFlakeIdGenerator("phonehome");
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("figct"), "2");

    }

    @Test
    public void testMapPutLatencyWithoutMapStore() {
        Map<String, String> parameters;
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("mpptla"), "-1");

        IMap<Object, Object> iMap = node.hazelcastInstance.getMap("hazelcast");
        LocalMapStatsImpl localMapStats = (LocalMapStatsImpl) iMap.getLocalMapStats();

        localMapStats.incrementPutLatencyNanos(2000000000L);
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("mpptla"), String.valueOf(2000));

        localMapStats.incrementPutLatencyNanos(1000000000L);
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("mpptla"), String.valueOf(1500));

        localMapStats.incrementPutLatencyNanos(2000000000L);
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("mpptla"), String.valueOf(1666));

    }

    @Test
    public void testMapGetLatencyWithoutMapStore() {
        Map<String, String> parameters;
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("mpgtla"), "-1");

        IMap<Object, Object> iMap = node.hazelcastInstance.getMap("hazelcast");
        LocalMapStatsImpl localMapStats = (LocalMapStatsImpl) iMap.getLocalMapStats();

        localMapStats.incrementGetLatencyNanos(2000000000L);
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("mpgtla"), String.valueOf(2000));

        localMapStats.incrementGetLatencyNanos(2000000000L);
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("mpgtla"), String.valueOf(2000));
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
        assertEquals(parameters.get("mpptlams"), "-1");

        IMap<Object, Object> iMap1 = initialiseForMapStore("hazelcast");
        iMap1.put("key1", "hazelcast");
        iMap1.put("key2", "phonehome");
        parameters = phoneHome.phoneHome(true);
        assertGreaterOrEquals("mpptlams", Long.parseLong(parameters.get("mpptlams")), 200);

        IMap<Object, Object> iMap2 = initialiseForMapStore("phonehome");
        iMap2.put("key3", "hazelcast");
        parameters = phoneHome.phoneHome(true);
        assertGreaterOrEquals("mpptlams", Long.parseLong(parameters.get("mpptlams")), 200);
    }


    @Test
    public void testMapGetLatencyWithMapStore() {
        Map<String, String> parameters;
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("mpgtlams"), "-1");

        IMap<Object, Object> iMap1 = initialiseForMapStore("hazelcast");
        iMap1.get("key1");
        iMap1.get("key2");
        parameters = phoneHome.phoneHome(true);
        assertGreaterOrEquals("mpgtlams", Long.parseLong(parameters.get("mpgtlams")), 200);

        IMap<Object, Object> iMap2 = initialiseForMapStore("phonehome");
        iMap2.get("key3");
        parameters = phoneHome.phoneHome(true);
        assertGreaterOrEquals("mpgtlams", Long.parseLong(parameters.get("mpgtlams")), 200);
    }
}

