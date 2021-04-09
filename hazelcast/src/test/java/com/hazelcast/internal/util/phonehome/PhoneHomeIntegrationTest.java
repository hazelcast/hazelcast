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

import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
import com.github.tomakehurst.wiremock.http.Fault;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.hazelcast.config.AttributeConfig;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.QueryCacheConfig;
import com.hazelcast.config.WanReplicationRef;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.monitor.impl.LocalMapStatsImpl;
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;


import javax.cache.CacheManager;
import javax.cache.spi.CachingProvider;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static com.hazelcast.cache.CacheTestSupport.createServerCachingProvider;
import static com.hazelcast.test.Accessors.getNode;
import static org.mockito.Mockito.when;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class PhoneHomeIntegrationTest extends HazelcastTestSupport {

    @Rule
    public WireMockRule wireMockRule = new WireMockRule(options().jettyHeaderBufferSize(16384));

    private Node node;
    private PhoneHome phoneHome;
    private CloudInfoCollector cloudInfoCollector;

    @Mock
    private Path kubernetesTokenPath;

    @Mock
    private Path dockerPath;

    @Before
    public void initialise()
            throws IOException {
        MockitoAnnotations.initMocks(this);
        HazelcastInstance hz = createHazelcastInstance();
        node = getNode(hz);

        when(dockerPath.toRealPath()).thenReturn(Paths.get(System.getProperty("user.dir")));
        when(kubernetesTokenPath.toRealPath()).thenReturn(Paths.get(System.getProperty("user.dir")));

        cloudInfoCollector = new CloudInfoCollector("http://localhost:8080/latest/meta-data",
                "http://localhost:8080/metadata/instance/compute?api-version=2018-02-01",
                "http://localhost:8080/metadata.google.internal", kubernetesTokenPath, dockerPath);

        phoneHome = new PhoneHome(node, "http://localhost:8080/ping", cloudInfoCollector);
        stubUrls("200", "4XX", "4XX", "4XX");
    }

    public void stubUrls(String phoneHomeStatus, String awsStatus, String azureStatus, String gcpStatus) {
        stubFor(get(urlPathEqualTo("/ping"))
                .willReturn(checkStatusConditional(phoneHomeStatus.equals("200"))));
        stubFor(get(urlPathEqualTo("/latest/meta-data"))
                .willReturn(checkStatusConditional(awsStatus.equals("200"))));
        stubFor(get(urlPathEqualTo("/metadata/instance/compute"))
                .withQueryParam("api-version", equalTo("2018-02-01"))
                .willReturn(checkStatusConditional(azureStatus.equals("200"))));
        stubFor(get(urlPathEqualTo("/metadata.google.internal"))
                .willReturn(checkStatusConditional(gcpStatus.equals("200"))));
    }

    private ResponseDefinitionBuilder checkStatusConditional(boolean condition) {
        return condition ? aResponse().withStatus(200) : aResponse().withFault(Fault.CONNECTION_RESET_BY_PEER);
    }

    @Test()
    public void testMapMetrics() {
        node.hazelcastInstance.getMap("hazelcast");
        node.hazelcastInstance.getMap("phonehome");
        MapConfig config = node.getConfig().getMapConfig("hazelcast");
        config.setReadBackupData(true);
        config.getMapStoreConfig().setClassName(DelayMapStore.class.getName()).setEnabled(true);
        config.addQueryCacheConfig(new QueryCacheConfig("queryconfig"));
        config.getHotRestartConfig().setEnabled(true);
        config.getIndexConfigs().add(new IndexConfig().setName("index"));
        config.setWanReplicationRef(new WanReplicationRef().setName("wan"));
        config.getAttributeConfigs().add(new AttributeConfig("hz", AttributeExtractor.class.getName()));
        config.getEvictionConfig().setEvictionPolicy(EvictionPolicy.LRU);
        config.setInMemoryFormat(InMemoryFormat.NATIVE);

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
        node.hazelcastInstance.getMap("hazelcast");
        node.hazelcastInstance.getSet("hazelcast");
        node.hazelcastInstance.getQueue("hazelcast");
        node.hazelcastInstance.getMultiMap("hazelcast");
        node.hazelcastInstance.getList("hazelcast");
        node.hazelcastInstance.getRingbuffer("hazelcast");
        node.hazelcastInstance.getTopic("hazelcast");
        node.hazelcastInstance.getReplicatedMap("hazelcast");
        node.hazelcastInstance.getCardinalityEstimator("hazelcast");
        node.hazelcastInstance.getPNCounter("hazelcast");
        node.hazelcastInstance.getFlakeIdGenerator("hazelcast");

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

        phoneHome.phoneHome(false);
        verify(1, getRequestedFor(urlPathEqualTo("/ping"))
                .withQueryParam("mpptla", equalTo("1666"))
                .withQueryParam("mpgtla", equalTo("1000")));
    }

    @Test
    public void testForCloudIfAWS() {
        stubUrls("200", "200", "4XX", "4XX");
        phoneHome.phoneHome(false);

        verify(1, getRequestedFor(urlPathEqualTo("/ping"))
                .withQueryParam("cld", equalTo("A")));
    }

    @Test
    public void testForCloudIfAzure() {
        stubUrls("200", "4XX", "200", "4XX");
        phoneHome.phoneHome(false);

        verify(1, getRequestedFor(urlPathEqualTo("/ping"))
                .withQueryParam("cld", equalTo("Z")));
    }

    @Test
    public void testForCloudIfGCP() {
        stubUrls("200", "4XX", "4XX", "200");
        phoneHome.phoneHome(false);

        verify(1, getRequestedFor(urlPathEqualTo("/ping"))
                .withQueryParam("cld", equalTo("G")));
    }

    @Test
    public void testDockerStateIfKuberNetes() {
        phoneHome.phoneHome(false);
        verify(1, getRequestedFor(urlPathEqualTo("/ping"))
                .withQueryParam("dck", equalTo("K")));
    }

    @Test
    public void testDockerStateIfOnlyDocker()
            throws IOException {
        when(kubernetesTokenPath.toRealPath()).thenThrow(new IOException());
        phoneHome.phoneHome(false);
        verify(1, getRequestedFor(urlPathEqualTo("/ping"))
                .withQueryParam("dck", equalTo("D")));
    }

    @Test
    public void testDockerStateIfNone()
            throws IOException {
        when(dockerPath.toRealPath()).thenThrow(new IOException());
        phoneHome.phoneHome(false);
        verify(1, getRequestedFor(urlPathEqualTo("/ping"))
                .withQueryParam("dck", equalTo("N")));
    }

    @Test
    public void testPhoneHomeCalledTwice() throws IOException {
        node.hazelcastInstance.getMap("hazelcast");

        phoneHome.phoneHome(false);
        phoneHome.phoneHome(false);

        verify(2, getRequestedFor(urlPathEqualTo("/ping")).withQueryParam("mpct", equalTo("1")));
    }

    @Test
    public void testCloudInfoCollectorCalledTwice_doesNotThrowException() {
        PhoneHomeParameterCreator parameterCreator1 = new PhoneHomeParameterCreator();
        cloudInfoCollector.forEachMetric(node,
                (type, value) -> parameterCreator1.addParam(type.getRequestParameterName(), value));

        PhoneHomeParameterCreator parameterCreator2 = new PhoneHomeParameterCreator();
        cloudInfoCollector.forEachMetric(node,
                (type, value) -> parameterCreator2.addParam(type.getRequestParameterName(), value));

    }
}
