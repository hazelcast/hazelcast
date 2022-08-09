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

package com.hazelcast.internal.util.phonehome;

import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
import com.github.tomakehurst.wiremock.http.Fault;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.github.tomakehurst.wiremock.matching.ContainsPattern;
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

import javax.cache.CacheManager;
import javax.cache.spi.CachingProvider;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static com.hazelcast.cache.CacheTestSupport.createServerCachingProvider;
import static com.hazelcast.test.Accessors.getNode;
import static org.mockito.Mockito.when;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class PhoneHomeIntegrationTest extends HazelcastTestSupport {

    static ContainsPattern containingParam(String paramName, String expectedValue) {
        return new ContainsPattern(paramName + "=" + expectedValue);
    }

    @Rule
    public WireMockRule wireMockRule = new WireMockRule(wireMockConfig().dynamicPort());

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

        int port = wireMockRule.port();
        cloudInfoCollector = new CloudInfoCollector("http://localhost:" + port + "/latest/meta-data",
                "http://localhost:" + port + "/metadata/instance/compute?api-version=2018-02-01",
                "http://localhost:" + port + "/metadata.google.internal", kubernetesTokenPath, dockerPath);

        phoneHome = new PhoneHome(node, "http://localhost:" + port + "/ping", cloudInfoCollector);
        stubUrls("200", "4XX", "4XX", "4XX");
    }

    public void stubUrls(String phoneHomeStatus, String awsStatus, String azureStatus, String gcpStatus) {
        stubFor(post(urlPathEqualTo("/ping"))
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

    @Test
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

        verify(1, postRequestedFor(urlPathEqualTo("/ping"))
                .withRequestBody(containingParam("mpct", "2"))
                .withRequestBody(containingParam("mpbrct", "1"))
                .withRequestBody(containingParam("mpmsct", "1"))
                .withRequestBody(containingParam("mpaoqcct", "1"))
                .withRequestBody(containingParam("mpaoict", "1"))
                .withRequestBody(containingParam("mphect", "1"))
                .withRequestBody(containingParam("mpwact", "1"))
                .withRequestBody(containingParam("mpaocct", "1"))
                .withRequestBody(containingParam("mpevct", "1"))
                .withRequestBody(containingParam("mpnmct", "1")));
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

        verify(1, postRequestedFor(urlPathEqualTo("/ping"))
                .withRequestBody(containingParam("mpct", "1"))
                .withRequestBody(containingParam("sect", "1"))
                .withRequestBody(containingParam("quct", "1"))
                .withRequestBody(containingParam("mmct", "1"))
                .withRequestBody(containingParam("lict", "1"))
                .withRequestBody(containingParam("rbct", "1"))
                .withRequestBody(containingParam("tpct", "1"))
                .withRequestBody(containingParam("rpct", "1"))
                .withRequestBody(containingParam("cect", "1"))
                .withRequestBody(containingParam("pncct", "1"))
                .withRequestBody(containingParam("figct", "1")));
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

        verify(1, postRequestedFor(urlPathEqualTo("/ping"))
                .withRequestBody(containingParam("cact", "1"))
                .withRequestBody(containingParam("cawact", "1")));
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

        verify(1, postRequestedFor(urlPathEqualTo("/ping"))
                .withRequestBody(containingParam("mpptlams", String.valueOf(totalPutLatency / totalPutOperationCount)))
                .withRequestBody(containingParam("mpgtlams", String.valueOf(totalGetLatency / totalGetOperationCount))));

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
        verify(1, postRequestedFor(urlPathEqualTo("/ping"))
                .withRequestBody(containingParam("mpptla", "1666"))
                .withRequestBody(containingParam("mpgtla", "1000")));
    }

    @Test
    public void testForCloudIfAWS() {
        stubUrls("200", "200", "4XX", "4XX");
        phoneHome.phoneHome(false);

        verify(1, postRequestedFor(urlPathEqualTo("/ping"))
                .withRequestBody(containingParam("cld", "A")));
    }

    @Test
    public void testForCloudIfAzure() {
        stubUrls("200", "4XX", "200", "4XX");
        phoneHome.phoneHome(false);

        verify(1, postRequestedFor(urlPathEqualTo("/ping"))
                .withRequestBody(containingParam("cld", "Z")));
    }

    @Test
    public void testForCloudIfGCP() {
        stubUrls("200", "4XX", "4XX", "200");
        phoneHome.phoneHome(false);

        verify(1, postRequestedFor(urlPathEqualTo("/ping"))
                .withRequestBody(containingParam("cld", "G")));
    }

    @Test
    public void testDockerStateIfKuberNetes() {
        phoneHome.phoneHome(false);
        verify(1, postRequestedFor(urlPathEqualTo("/ping"))
                .withRequestBody(containingParam("dck", "K")));
    }

    @Test
    public void testDockerStateIfOnlyDocker()
            throws IOException {
        when(kubernetesTokenPath.toRealPath()).thenThrow(new IOException());
        phoneHome.phoneHome(false);
        verify(1, postRequestedFor(urlPathEqualTo("/ping"))
                .withRequestBody(containingParam("dck", "D")));
    }

    @Test
    public void testDockerStateIfNone()
            throws IOException {
        when(dockerPath.toRealPath()).thenThrow(new IOException());
        phoneHome.phoneHome(false);
        verify(1, postRequestedFor(urlPathEqualTo("/ping"))
                .withRequestBody(containingParam("dck", "N")));
    }

    @Test
    public void testPhoneHomeCalledTwice() throws IOException {
        node.hazelcastInstance.getMap("hazelcast");

        phoneHome.phoneHome(false);
        phoneHome.phoneHome(false);

        verify(2, postRequestedFor(urlPathEqualTo("/ping"))
                .withRequestBody(containingParam("mpct", "1")));
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
