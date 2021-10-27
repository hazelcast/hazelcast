package com.hazelcast.internal.util.phonehome;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.hazelcast.cluster.ClusterState;
import com.hazelcast.config.AttributeConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.ConfigXmlGenerator;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.QueryCacheConfig;
import com.hazelcast.config.RestApiConfig;
import com.hazelcast.config.WanReplicationRef;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.ascii.HTTPCommunicator;
import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.TestAwareInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.net.HttpURLConnection;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static com.hazelcast.internal.util.phonehome.PhoneHomeIntegrationTest.containingParam;
import static com.hazelcast.internal.util.phonehome.TestUtil.getNode;
import static com.hazelcast.test.HazelcastTestSupport.smallInstanceConfig;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class RESTClientPhoneHomeTest {

    protected final TestAwareInstanceFactory factory = new TestAwareInstanceFactory();

    @Rule
    public WireMockRule wireMockRule = new WireMockRule(options().jettyHeaderBufferSize(16384));

    @BeforeClass
    public static void beforeClass() {
        Hazelcast.shutdownAll();
    }

    @After
    public void tearDown() {
        factory.terminateAll();
    }

    protected Config createConfig() {
        return smallInstanceConfig();
    }

    protected Config createConfigWithRestEnabled() {
        Config config = createConfig();
        RestApiConfig restApiConfig = new RestApiConfig().setEnabled(true).enableAllGroups();
        config.getNetworkConfig().setRestApiConfig(restApiConfig);
        return config;
    }

    @Test
    public void mapPutsAreCounted()
            throws IOException {
        stubFor(post(urlPathEqualTo("/ping"))
                .willReturn(aResponse().withStatus(200)));

        HazelcastInstance instance = factory.newHazelcastInstance(createConfigWithRestEnabled());

        HTTPCommunicator http = new HTTPCommunicator(instance);
        System.out.println(http.mapPut("my-map", "key", "value"));

        PhoneHome phoneHome = new PhoneHome(getNode(instance), "http://localhost:8080/ping");
        phoneHome.phoneHome(false);

        verify(1, postRequestedFor(urlPathEqualTo("/ping"))
                .withRequestBody(containingParam("mapput200", "1"))
        );
    }

    private JsonObject assertJsonContains(String json, String... attributesAndValues) {
        JsonObject object = Json.parse(json).asObject();
        for (int i = 0; i < attributesAndValues.length; ) {
            String key = attributesAndValues[i++];
            String expectedValue = attributesAndValues[i++];
            assertEquals(expectedValue, object.getString(key, null));
        }
        return object;
    }

    private void assertSuccessJson(HTTPCommunicator.ConnectionResponse resp, String... attributesAndValues) {
        assertEquals(HttpURLConnection.HTTP_OK, resp.responseCode);
        assertJsonContains(resp.response, "status", "success");
        if (attributesAndValues.length > 0) {
            assertJsonContains(resp.response, attributesAndValues);
        }
    }
}
