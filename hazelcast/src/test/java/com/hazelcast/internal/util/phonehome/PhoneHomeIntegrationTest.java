/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.spi.properties.HazelcastProperty;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.CacheManager;
import javax.cache.spi.CachingProvider;
import java.net.HttpURLConnection;
import java.util.Map;

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
import static com.hazelcast.internal.util.phonehome.BuildInfoProvider.PARDOT_ID_ENV_VAR;
import static com.hazelcast.internal.util.phonehome.CloudInfoProvider.AWS_ENDPOINT;
import static com.hazelcast.internal.util.phonehome.CloudInfoProvider.AZURE_ENDPOINT;
import static com.hazelcast.internal.util.phonehome.CloudInfoProvider.CLOUD_ENVIRONMENT_ENV_VAR;
import static com.hazelcast.internal.util.phonehome.CloudInfoProvider.DOCKER_FILE_PATH;
import static com.hazelcast.internal.util.phonehome.CloudInfoProvider.GCP_ENDPOINT;
import static com.hazelcast.internal.util.phonehome.CloudInfoProvider.KUBERNETES_TOKEN_PATH;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.AVERAGE_GET_LATENCY_OF_MAPS_USING_MAPSTORE;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.AVERAGE_PUT_LATENCY_OF_MAPS_USING_MAPSTORE;
import static com.hazelcast.test.Accessors.getNode;
import static uk.org.webcompere.systemstubs.SystemStubs.withEnvironmentVariables;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class PhoneHomeIntegrationTest extends HazelcastTestSupport {
    @Rule
    public WireMockRule wireMockRule = new WireMockRule(wireMockConfig().dynamicPort());

    private HazelcastInstance instance;
    private Node node;
    private PhoneHome phoneHome;

    @Before
    public void initialise() {
        int port = wireMockRule.port();

        Map<HazelcastProperty, String> properties = Map.of(
                AWS_ENDPOINT, "http://localhost:" + port + "/latest/meta-data",
                AZURE_ENDPOINT, "http://localhost:" + port + "/metadata/instance/compute?api-version=2018-02-01",
                GCP_ENDPOINT, "http://localhost:" + port + "/metadata.google.internal",
                KUBERNETES_TOKEN_PATH, System.getProperty("user.dir"),
                DOCKER_FILE_PATH, System.getProperty("user.dir")
        );
        properties.forEach(HazelcastProperty::setSystemProperty);
        instance = createHazelcastInstance();
        node = getNode(instance);

        phoneHome = new PhoneHome(node, "http://localhost:" + port + "/ping");
        stubUrls("200", "4XX", "4XX", "4XX");
    }

    private void stubUrls(String phoneHomeStatus, String awsStatus, String azureStatus, String gcpStatus) {
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
        return condition ? aResponse().withStatus(HttpURLConnection.HTTP_OK) : aResponse().withFault(Fault.CONNECTION_RESET_BY_PEER);
    }

    @Test
    @SuppressWarnings({"ResultOfMethodCallIgnored"})
    public void testMapMetrics() {
        instance.getMap("hazelcast");
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

        IMap<String, String> phoneHomeMap = instance.getMap("phonehome");
        phoneHomeMap.entrySet();
        phoneHomeMap.values();

        phoneHome.phoneHome(false);

        verify(1, postRequestedFor(urlPathEqualTo("/ping"))
                .withRequestBody(containingParam("mpct" /*COUNT_OF_MAPS*/, 2))
                .withRequestBody(containingParam("mpbrct" /*MAP_COUNT_WITH_READ_ENABLED*/, 1))
                .withRequestBody(containingParam("mpmsct" /*MAP_COUNT_WITH_MAP_STORE_ENABLED*/, 1))
                .withRequestBody(containingParam("mpaoqcct" /*MAP_COUNT_WITH_ATLEAST_ONE_QUERY_CACHE*/, 1))
                .withRequestBody(containingParam("mpaoict" /*MAP_COUNT_WITH_ATLEAST_ONE_INDEX*/, 1))
                .withRequestBody(containingParam("mphect" /*MAP_COUNT_WITH_HOT_RESTART_OR_PERSISTENCE_ENABLED*/, 1))
                .withRequestBody(containingParam("mpwact" /*MAP_COUNT_WITH_WAN_REPLICATION*/, 1))
                .withRequestBody(containingParam("mpaocct" /*MAP_COUNT_WITH_ATLEAST_ONE_ATTRIBUTE*/, 1))
                .withRequestBody(containingParam("mpevct" /*MAP_COUNT_USING_EVICTION*/, 1))
                .withRequestBody(containingParam("mpnmct" /*MAP_COUNT_USING_NATIVE_INMEMORY_FORMAT*/, 1))
                .withRequestBody(containingParam("mpvaluesct" /*TOTAL_MAP_VALUES_CALLS*/, 1))
                .withRequestBody(containingParam("mpentriesct" /*TOTAL_MAP_ENTRYSET_CALLS*/, 1))
                .withRequestBody(containingParam("mpqslh" /*TOTAL_MAP_QUERY_SIZE_LIMITER_HITS*/, 0))
        );
    }

    @Test
    public void testCountDistributedObjects() {
        instance.getMap("hazelcast");
        instance.getSet("hazelcast");
        instance.getQueue("hazelcast");
        instance.getMultiMap("hazelcast");
        instance.getList("hazelcast");
        instance.getRingbuffer("hazelcast");
        instance.getTopic("hazelcast");
        instance.getReplicatedMap("hazelcast");
        instance.getCardinalityEstimator("hazelcast");
        instance.getPNCounter("hazelcast");
        instance.getFlakeIdGenerator("hazelcast");

        phoneHome.phoneHome(false);

        verify(1, postRequestedFor(urlPathEqualTo("/ping"))
                .withRequestBody(containingParam("mpct" /*COUNT_OF_MAPS*/, 1))
                .withRequestBody(containingParam("sect" /*COUNT_OF_SETS*/, 1))
                .withRequestBody(containingParam("quct" /*COUNT_OF_QUEUES*/, 1))
                .withRequestBody(containingParam("mmct" /*COUNT_OF_MULTIMAPS*/, 1))
                .withRequestBody(containingParam("lict" /*COUNT_OF_LISTS*/, 1))
                .withRequestBody(containingParam("rbct" /*COUNT_OF_RING_BUFFERS*/, 1))
                .withRequestBody(containingParam("tpct" /*COUNT_OF_TOPICS*/, 1))
                .withRequestBody(containingParam("rpct" /*COUNT_OF_REPLICATED_MAPS*/, 1))
                .withRequestBody(containingParam("cect" /*COUNT_OF_CARDINALITY_ESTIMATORS*/, 1))
                .withRequestBody(containingParam("pncct" /*COUNT_OF_PN_COUNTERS*/, 1))
                .withRequestBody(containingParam("figct" /*COUNT_OF_FLAKE_ID_GENERATORS*/, 1)));
    }

    @Test
    public void testCacheMetrics() {
        CachingProvider cachingProvider = createServerCachingProvider(instance);
        CacheManager cacheManager = cachingProvider.getCacheManager();
        CacheSimpleConfig cacheSimpleConfig = new CacheSimpleConfig();
        cacheSimpleConfig.setName("hazelcast");
        cacheSimpleConfig.setWanReplicationRef(new WanReplicationRef());
        cacheManager.createCache("hazelcast", new CacheConfig<>("hazelcast"));
        node.getConfig().addCacheConfig(cacheSimpleConfig);

        phoneHome.phoneHome(false);

        verify(1, postRequestedFor(urlPathEqualTo("/ping"))
                .withRequestBody(containingParam("cact" /*COUNT_OF_CACHES*/, 1))
                .withRequestBody(containingParam("cawact" /*CACHE_COUNT_WITH_WAN_REPLICATION*/, 1)));
    }

    @Test
    public void testMapLatenciesWithMapStore() {
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setEnabled(true);
        mapStoreConfig.setImplementation(new DelayMapStore());
        IMap<String, String> iMap = instance.getMap("hazelcast");
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
                .withRequestBody(containingParam("mpptlams" /*AVERAGE_PUT_LATENCY_OF_MAPS_USING_MAPSTORE*/,
                        totalPutLatency / totalPutOperationCount))
                .withRequestBody(containingParam("mpgtlams" /*AVERAGE_GET_LATENCY_OF_MAPS_USING_MAPSTORE*/,
                        totalGetLatency / totalGetOperationCount)));

        assertGreaterOrEquals(AVERAGE_PUT_LATENCY_OF_MAPS_USING_MAPSTORE.toString(),
                totalPutLatency / totalPutOperationCount, 200);
        assertGreaterOrEquals(AVERAGE_GET_LATENCY_OF_MAPS_USING_MAPSTORE.toString(),
                totalGetLatency / totalGetOperationCount, 200);
    }

    @Test
    public void testMapLatenciesWithoutMapStore() {
        IMap<Object, Object> iMap1 = instance.getMap("hazelcast");
        LocalMapStatsImpl localMapStats1 = (LocalMapStatsImpl) iMap1.getLocalMapStats();
        IMap<Object, Object> iMap2 = instance.getMap("phonehome");
        LocalMapStatsImpl localMapStats2 = (LocalMapStatsImpl) iMap2.getLocalMapStats();
        localMapStats1.incrementPutLatencyNanos(2000000000L);
        localMapStats1.incrementPutLatencyNanos(1000000000L);
        localMapStats2.incrementPutLatencyNanos(2000000000L);
        localMapStats1.incrementGetLatencyNanos(1000000000L);
        localMapStats2.incrementGetLatencyNanos(1000000000L);

        phoneHome.phoneHome(false);

        verify(1, postRequestedFor(urlPathEqualTo("/ping"))
                .withRequestBody(containingParam("mpptla" /*AVERAGE_PUT_LATENCY_OF_MAPS_WITHOUT_MAPSTORE*/, 1666))
                .withRequestBody(containingParam("mpgtla" /*AVERAGE_GET_LATENCY_OF_MAPS_WITHOUT_MAPSTORE*/, 1000)));
    }

    @Test
    public void testForCloudIfAWS() {
        stubUrls("200", "200", "4XX", "4XX");
        phoneHome.phoneHome(false);

        verify(1, postRequestedFor(urlPathEqualTo("/ping"))
                .withRequestBody(containingParam("cld" /*CLOUD*/, "A")));
    }

    @Test
    public void testForCloudIfAzure() {
        stubUrls("200", "4XX", "200", "4XX");
        phoneHome.phoneHome(false);

        verify(1, postRequestedFor(urlPathEqualTo("/ping"))
                .withRequestBody(containingParam("cld" /*CLOUD*/, "Z")));
    }

    @Test
    public void testForCloudIfGCP() {
        stubUrls("200", "4XX", "4XX", "200");
        phoneHome.phoneHome(false);

        verify(1, postRequestedFor(urlPathEqualTo("/ping"))
                .withRequestBody(containingParam("cld" /*CLOUD*/, "G")));
    }

    @Test
    public void testDockerStateIfKubernetes() {
        phoneHome.phoneHome(false);

        verify(1, postRequestedFor(urlPathEqualTo("/ping"))
                .withRequestBody(containingParam("dck" /*DOCKER*/, "K")));
    }

    @Test
    public void testDockerStateIfOnlyDocker() {
        KUBERNETES_TOKEN_PATH.setSystemProperty("/non-existing/path");
        phoneHome.phoneHome(false);

        verify(1, postRequestedFor(urlPathEqualTo("/ping"))
                .withRequestBody(containingParam("dck" /*DOCKER*/, "D")));
    }

    @Test
    public void testDockerStateIfNone() {
        DOCKER_FILE_PATH.setSystemProperty("/non-existing/path");
        phoneHome.phoneHome(false);

        verify(1, postRequestedFor(urlPathEqualTo("/ping"))
                .withRequestBody(containingParam("dck" /*DOCKER*/, "N")));
    }

    @Test
    public void testPhoneHomeCalledTwice() {
        instance.getMap("hazelcast");

        phoneHome.phoneHome(false);
        phoneHome.phoneHome(false);

        verify(2, postRequestedFor(urlPathEqualTo("/ping"))
                .withRequestBody(containingParam("mpct" /*COUNT_OF_MAPS*/, 1)));
    }

    @Test
    public void testCloudInfoCollectorCalledTwice_doesNotThrowException() {
        CloudInfoProvider cloudInfoProvider = new CloudInfoProvider();

        MetricsCollectionContext context1 = new MetricsCollectionContext();
        cloudInfoProvider.provideMetrics(node, context1);

        MetricsCollectionContext context2 = new MetricsCollectionContext();
        cloudInfoProvider.provideMetrics(node, context2);
    }

    @Test
    public void testForViridian() throws Exception {
        stubUrls("200", "200", "4XX", "4XX");
        withEnvironmentVariables(CLOUD_ENVIRONMENT_ENV_VAR, "SERVERLESS")
                .execute(() -> phoneHome.phoneHome(false));

        verify(1, postRequestedFor(urlPathEqualTo("/ping"))
                .withRequestBody(containingParam("vrd" /*VIRIDIAN*/, "SERVERLESS")));
    }

    @Test
    public void testDownloadIdOverriddenWithEnvVar() throws Exception {
        withEnvironmentVariables(PARDOT_ID_ENV_VAR, "1234")
                .execute(() -> phoneHome.phoneHome(false));

        verify(1, postRequestedFor(urlPathEqualTo("/ping"))
                .withRequestBody(containingParam("p" /*HAZELCAST_DOWNLOAD_ID*/, "1234")));
    }

    static ContainsPattern containingParam(String key, boolean value) {
        return new ContainsPattern(key + "=" + value);
    }

    static ContainsPattern containingParam(String key, long value) {
        return new ContainsPattern(key + "=" + value);
    }

    static ContainsPattern containingParam(String key, String value) {
        return new ContainsPattern(key + "=" + value);
    }
}
