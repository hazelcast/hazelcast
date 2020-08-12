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

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.hazelcast.cardinality.CardinalityEstimator;
import com.hazelcast.collection.IQueue;
import com.hazelcast.collection.ISet;
import com.hazelcast.config.AttributeConfig;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.QueryCacheConfig;
import com.hazelcast.config.WanReplicationRef;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.crdt.pncounter.PNCounter;
import com.hazelcast.flakeidgen.FlakeIdGenerator;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.monitor.impl.LocalMapStatsImpl;
import com.hazelcast.map.IMap;
import com.hazelcast.multimap.MultiMap;
import com.hazelcast.replicatedmap.ReplicatedMap;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.hazelcast.test.HazelcastTestSupport;

import com.hazelcast.topic.ITopic;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import javax.cache.CacheManager;
import javax.cache.spi.CachingProvider;
import java.util.List;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static com.hazelcast.cache.CacheTestSupport.createServerCachingProvider;
import static com.hazelcast.test.Accessors.getNode;

public class PhoneHomeIntegrationTest extends HazelcastTestSupport {

    @Rule
    public WireMockRule wireMockRule = new WireMockRule();

    private Node node;
    private PhoneHome phoneHome;

    @Before
    public void initialise() {
        HazelcastInstance hz = createHazelcastInstance();
        node = getNode(hz);
        phoneHome = new PhoneHome(node, "http://localhost:8080/ping");
    }

    @Test()
    public void testMapMetrics() {
        IMap<String, String> map1 = node.hazelcastInstance.getMap("hazelcast");
        IMap<String, String> map2 = node.hazelcastInstance.getMap("phonehome");
        node.getConfig().getMapConfig("hazelcast").setReadBackupData(true);
        node.getConfig().getMapConfig("phonehome").getMapStoreConfig().setClassName(DelayMapStore.class.getName()).setEnabled(true);
        node.getConfig().getMapConfig("hazelcast").addQueryCacheConfig(new QueryCacheConfig("queryconfig"));
        node.getConfig().getMapConfig("hazelcast").getHotRestartConfig().setEnabled(true);
        node.getConfig().getMapConfig("hazelcast").getIndexConfigs().add(new IndexConfig().setName("index"));
        node.getConfig().getMapConfig("hazelcast").setWanReplicationRef(new WanReplicationRef().setName("wan"));
        node.getConfig().getMapConfig("hazelcast").getAttributeConfigs().add(new AttributeConfig("hz", AttributeExtractor.class.getName()));
        node.getConfig().getMapConfig("hazelcast").getEvictionConfig().setEvictionPolicy(EvictionPolicy.LRU);
        node.getConfig().getMapConfig("hazelcast").setInMemoryFormat(InMemoryFormat.NATIVE);

        stubFor(get(urlPathEqualTo("/ping"))
                .willReturn(aResponse()
                        .withStatus(200)));

        phoneHome.phoneHome(false);

        verify(1, getRequestedFor(urlPathEqualTo("/ping"))
                .withQueryParam("mpct", equalTo("2"))
                .withQueryParam("mpbrct", equalTo("1"))
                .withQueryParam("mpmsct", equalTo("1"))
                .withQueryParam("mpaoqcct", equalTo("1"))
                .withQueryParam("mpaoict", equalTo("1"))
                .withQueryParam("mphect", equalTo("1"))
                .withQueryParam("mpwact", equalTo("1"))
                .withQueryParam("mpaocct", equalTo("1"))
                .withQueryParam("mpevct", equalTo("1"))
                .withQueryParam("mpnmct", equalTo("1")));
    }

    @Test
    public void testCountDistributedObjects() {
        IMap<Object, Object> map = node.hazelcastInstance.getMap("hazelcast");
        ISet<Object> set = node.hazelcastInstance.getSet("hazelcast");
        IQueue<Object> queue = node.hazelcastInstance.getQueue("hazelcast");
        MultiMap<Object, Object> multimap = node.hazelcastInstance.getMultiMap("hazelcast");
        List<Object> list = node.hazelcastInstance.getList("hazelcast");
        Ringbuffer<Object> ringbuffer = node.hazelcastInstance.getRingbuffer("hazelcast");
        ITopic<String> topic = node.hazelcastInstance.getTopic("hazelcast");
        ReplicatedMap<String, String> replicatedMap = node.hazelcastInstance.getReplicatedMap("hazelcast");
        CardinalityEstimator cardinalityEstimator = node.hazelcastInstance.getCardinalityEstimator("hazelcast");
        PNCounter pnCounter = node.hazelcastInstance.getPNCounter("hazelcast");
        FlakeIdGenerator flakeIdGenerator = node.hazelcastInstance.getFlakeIdGenerator("hazelcast");

        stubFor(get(urlPathEqualTo("/ping"))
                .willReturn(aResponse()
                        .withStatus(200)));

        phoneHome.phoneHome(false);

        verify(1, getRequestedFor(urlPathEqualTo("/ping"))
                .withQueryParam("mpct", equalTo("1"))
                .withQueryParam("sect", equalTo("1"))
                .withQueryParam("quct", equalTo("1"))
                .withQueryParam("mmct", equalTo("1"))
                .withQueryParam("lict", equalTo("1"))
                .withQueryParam("rbct", equalTo("1"))
                .withQueryParam("tpct", equalTo("1"))
                .withQueryParam("rpct", equalTo("1"))
                .withQueryParam("cect", equalTo("1"))
                .withQueryParam("pncct", equalTo("1"))
                .withQueryParam("figct", equalTo("1")));
    }

    @Test
    public void testCacheMetrics() {
        CachingProvider cachingProvider = createServerCachingProvider(node.hazelcastInstance);
        CacheManager cacheManager = cachingProvider.getCacheManager();
        CacheSimpleConfig cacheSimpleConfig = new CacheSimpleConfig();
        cacheSimpleConfig.setName("hazelcast");
        cacheSimpleConfig.setWanReplicationRef(new WanReplicationRef());
        cacheManager.createCache("hazelcast", new CacheConfig<>("hazelcast"));
        node.getConfig().addCacheConfig(cacheSimpleConfig);

        stubFor(get(urlPathEqualTo("/ping"))
                .willReturn(aResponse()
                        .withStatus(200)));

        phoneHome.phoneHome(false);
        verify(1, getRequestedFor(urlPathEqualTo("/ping"))
                .withQueryParam("cact", equalTo("1"))
                .withQueryParam("cawact", equalTo("1")));
    }

    @Test
    public void testMapLatenciesWithMapStore() {
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setEnabled(true);
        mapStoreConfig.setImplementation(new DelayMapStore());
        IMap<String, String> iMap = node.hazelcastInstance.getMap("hazelcast");
        node.getConfig().getMapConfig("hazelcast").setMapStoreConfig(mapStoreConfig);
        iMap.put("key1", "hazelcast");
        iMap.put("key2", "phonehome");
        iMap.get("key3");
        LocalMapStatsImpl mapStats = (LocalMapStatsImpl) iMap.getLocalMapStats();
        long totalGetLatency = mapStats.getTotalGetLatency();
        long totalPutLatency = mapStats.getTotalPutLatency();
        long totalGetOperationCount = mapStats.getGetOperationCount();
        long totalPutOperationCount = mapStats.getPutOperationCount();

        stubFor(get(urlPathEqualTo("/ping"))
                .willReturn(aResponse()
                        .withStatus(200)));

        phoneHome.phoneHome(false);

        verify(1, getRequestedFor(urlPathEqualTo("/ping"))
                .withQueryParam("mpptlams", equalTo(String.valueOf(totalPutLatency / totalPutOperationCount)))
                .withQueryParam("mpgtlams", equalTo(String.valueOf(totalGetLatency / totalGetOperationCount))));

        assertGreaterOrEquals("mpptlams", totalPutLatency / totalPutOperationCount, 200);
        assertGreaterOrEquals("mpgtlams", totalGetLatency / totalGetOperationCount, 200);
    }

    @Test
    public void testMapLatenciesWithoutMapStore() {
        IMap<Object, Object> iMap1 = node.hazelcastInstance.getMap("hazelcast");
        LocalMapStatsImpl localMapStats1 = (LocalMapStatsImpl) iMap1.getLocalMapStats();
        IMap<Object, Object> iMap2 = node.hazelcastInstance.getMap("phonehome");
        LocalMapStatsImpl localMapStats2 = (LocalMapStatsImpl) iMap2.getLocalMapStats();
        localMapStats1.incrementPutLatencyNanos(2000000000L);
        localMapStats1.incrementPutLatencyNanos(1000000000L);
        localMapStats2.incrementPutLatencyNanos(2000000000L);
        localMapStats1.incrementGetLatencyNanos(1000000000L);
        localMapStats2.incrementGetLatencyNanos(1000000000L);

        stubFor(get(urlPathEqualTo("/ping"))
                .willReturn(aResponse()
                        .withStatus(200)));

        phoneHome.phoneHome(false);
        verify(1, getRequestedFor(urlPathEqualTo("/ping"))
                .withQueryParam("mpptla", equalTo("1666"))
                .withQueryParam("mpgtla", equalTo("1000")));
    }
}

