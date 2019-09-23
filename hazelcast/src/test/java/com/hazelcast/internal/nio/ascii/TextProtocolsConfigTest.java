/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.nio.ascii;

import com.hazelcast.config.Config;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.MemcacheProtocolConfig;
import com.hazelcast.config.RestApiConfig;
import com.hazelcast.config.RestEndpointGroup;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.config.RestEndpointGroup.DATA;
import static com.hazelcast.config.RestEndpointGroup.HEALTH_CHECK;
import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static com.hazelcast.test.HazelcastTestSupport.getAddress;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests enabling text protocols by {@link RestApiConfig}, {@link MemcacheProtocolConfig} and legacy system properties.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
@SuppressWarnings("deprecation")
public class TextProtocolsConfigTest extends RestApiConfigTestBase {

    private static final TestUrl TEST_URL_HEALTH_CHECK =
            new TestUrl(HEALTH_CHECK, GET, "/hazelcast/health/node-state", "ACTIVE");
    private static final TestUrl TEST_URL_DATA = new TestUrl(DATA, GET, "/hazelcast/rest/maps/test/testKey", "testValue");

    /**
     * <pre>
     * Given: -
     * When: empty RestApiConfig object is created
     * Then: it's disabled and the only enabled REST endpoint group is the CLUSTER_READ
     * </pre>
     */
    @Test
    public void testRestApiDefaults() throws Exception {
        RestApiConfig restApiConfig = new RestApiConfig();
        assertFalse("REST should be disabled by default", restApiConfig.isEnabled());
        for (RestEndpointGroup endpointGroup : RestEndpointGroup.values()) {
            if (isExpectedDefaultEnabled(endpointGroup)) {
                assertTrue(
                        "REST endpoint group should be enabled by default: " + endpointGroup,
                        restApiConfig.isGroupEnabled(endpointGroup));
            } else {
                assertFalse(
                        "REST endpoint group should be disabled by default: " + endpointGroup,
                        restApiConfig.isGroupEnabled(endpointGroup));
            }
        }
    }

    /**
     * <pre>
     * Given: -
     * When: empty RestApiConfig object is created
     * Then: access to all REST endpoints is denied
     * </pre>
     */
    @Test
    public void testRestApiCallWithDefaults() throws Exception {
        Config config = new Config();
        config.getNetworkConfig().setRestApiConfig(new RestApiConfig());
        HazelcastInstance hz = factory.newHazelcastInstance(config);
        for (TestUrl testUrl : TEST_URLS) {
            assertNoTextProtocolResponse(hz, testUrl);
        }
    }

    /**
     * <pre>
     * Given: RestApiConfig is explicitly enabled
     * When: REST endpoint is accessed
     * Then: it is permitted/denied based on its default groups values
     * </pre>
     */
    @Test
    public void testEnabledRestApiCallWithGroupDefaults() throws Exception {
        Config config = new Config();
        config.getNetworkConfig().setRestApiConfig(new RestApiConfig().setEnabled(true));
        HazelcastInstance hz = factory.newHazelcastInstance(config);
        for (TestUrl testUrl : TEST_URLS) {
            if (isExpectedDefaultEnabled(testUrl.restEndpointGroup)) {
                assertTextProtocolResponse(hz, testUrl);
            } else {
                assertNoTextProtocolResponse(hz, testUrl);
            }
        }
    }

    /**
     * <pre>
     * Given: RestApiConfig is explicitly enabled and all groups are explicitly enabled
     * When: REST endpoint is accessed
     * Then: access is permitted
     * </pre>
     */
    @Test
    public void testRestApiCallEnabledGroupsEnabled() throws Exception {
        Config config = new Config();
        config.getNetworkConfig().setRestApiConfig(new RestApiConfig().setEnabled(true).enableAllGroups());
        HazelcastInstance hz = factory.newHazelcastInstance(config);
        for (TestUrl testUrl : TEST_URLS) {
            assertTextProtocolResponse(hz, testUrl);
        }
    }

    /**
     * <pre>
     * Given: RestApiConfig is explicitly disabled and all groups are explicitly enabled
     * When: REST endpoint is accessed
     * Then: access is denied
     * </pre>
     */
    @Test
    public void testRestApiCallDisabledGroupsEnabled() throws Exception {
        Config config = new Config();
        config.getNetworkConfig().setRestApiConfig(new RestApiConfig().setEnabled(false).enableAllGroups());
        HazelcastInstance hz = factory.newHazelcastInstance(config);
        for (TestUrl testUrl : TEST_URLS) {
            assertNoTextProtocolResponse(hz, testUrl);
        }
    }

    @Test
    public void testRestConfigWithRestProperty() throws Exception {
        Config config = new Config()
                .setProperty(GroupProperty.REST_ENABLED.getName(), "true");
        createMemberWithRestConfigAndAssertConfigException(config);
    }

    @Test
    public void testRestConfigWithHealthCheckProperty() throws Exception {
        Config config = new Config()
                .setProperty(GroupProperty.HTTP_HEALTHCHECK_ENABLED.getName(), "true");
        createMemberWithRestConfigAndAssertConfigException(config);
    }

    @Test
    public void testRestConfigWithMemcacheProperty() throws Exception {
        Config config = new Config()
                .setProperty(GroupProperty.MEMCACHE_ENABLED.getName(), "true");
        config.getNetworkConfig().setRestApiConfig(new RestApiConfig());
        factory.newHazelcastInstance(config);
    }

    @Test
    public void testMemcacheProtocolConfigWithMemcachePropertyEnabled() throws Exception {
        Config config = new Config()
                .setProperty(GroupProperty.MEMCACHE_ENABLED.getName(), "true");
        config.getNetworkConfig().setMemcacheProtocolConfig(new MemcacheProtocolConfig());
        expectedException.expect(InvalidConfigurationException.class);
        factory.newHazelcastInstance(config);
    }

    @Test
    public void testRestConfigWithRestPropertyDisabled() throws Exception {
        Config config = new Config()
                .setProperty(GroupProperty.REST_ENABLED.getName(), "false");
        createMemberWithRestConfigAndAssertConfigException(config);
    }

    @Test
    public void testRestConfigWithHealthCheckPropertyDisabled() throws Exception {
        Config config = new Config()
                .setProperty(GroupProperty.HTTP_HEALTHCHECK_ENABLED.getName(), "false");
        createMemberWithRestConfigAndAssertConfigException(config);
    }

    @Test
    public void testRestConfigWithMemcachePropertyDisabled() throws Exception {
        Config config = new Config()
                .setProperty(GroupProperty.MEMCACHE_ENABLED.getName(), "false");
        config.getNetworkConfig().setRestApiConfig(new RestApiConfig());
        factory.newHazelcastInstance(config);
    }

    @Test
    public void testMemcacheProtocolConfigWithMemcachePropertyDisabled() throws Exception {
        Config config = new Config()
                .setProperty(GroupProperty.MEMCACHE_ENABLED.getName(), "false");
        config.getNetworkConfig().setMemcacheProtocolConfig(new MemcacheProtocolConfig());
        expectedException.expect(InvalidConfigurationException.class);
        factory.newHazelcastInstance(config);
    }

    @Test
    public void testMemcachePropertyEnabled() throws Exception {
        Config config = new Config()
                .setProperty(GroupProperty.MEMCACHE_ENABLED.getName(), "true");
        HazelcastInstance hz = factory.newHazelcastInstance(config);
        assertNoTextProtocolResponse(hz, TEST_URL_HEALTH_CHECK);
        TextProtocolClient client = new TextProtocolClient(getAddress(hz).getInetSocketAddress());
        try {
            client.connect();
            client.sendData("version\n");
            assertTrueEventually(createResponseAssertTask("Version expected", client, "VERSION Hazelcast"), 10);
        } finally {
            client.close();
        }
    }

    @Test
    public void testMemcachePropertyDisabled() throws Exception {
        Config config = new Config()
                .setProperty(GroupProperty.MEMCACHE_ENABLED.getName(), "false");
        HazelcastInstance hz = factory.newHazelcastInstance(config);
        assertNoTextProtocolResponse(hz, TEST_URL_DATA);
        assertNoTextProtocolResponse(hz, TEST_URL_HEALTH_CHECK);
    }

    @Test
    public void testHealthCheckPropertyEnabled() throws Exception {
        Config config = new Config()
                .setProperty(GroupProperty.HTTP_HEALTHCHECK_ENABLED.getName(), "true");
        HazelcastInstance hz = factory.newHazelcastInstance(config);
        assertTextProtocolResponse(hz, TEST_URL_HEALTH_CHECK);
        assertNoTextProtocolResponse(hz, TEST_URL_DATA);
    }

    @Test
    public void testHealthCheckPropertyDisabled() throws Exception {
        Config config = new Config()
                .setProperty(GroupProperty.HTTP_HEALTHCHECK_ENABLED.getName(), "false");
        HazelcastInstance hz = factory.newHazelcastInstance(config);
        assertNoTextProtocolResponse(hz, TEST_URL_DATA);
        assertNoTextProtocolResponse(hz, TEST_URL_HEALTH_CHECK);
    }

    @Test
    public void testRestPropertyEnabled() throws Exception {
        Config config = new Config()
                .setProperty(GroupProperty.REST_ENABLED.getName(), "true");
        HazelcastInstance hz = factory.newHazelcastInstance(config);
        hz.getMap("test").put("testKey", "testValue");
        assertTextProtocolResponse(hz, TEST_URL_DATA);
        assertTextProtocolResponse(hz, TEST_URL_HEALTH_CHECK);
    }

    @Test
    public void testRestPropertyDisabled() throws Exception {
        Config config = new Config()
                .setProperty(GroupProperty.REST_ENABLED.getName(), "false");
        HazelcastInstance hz = factory.newHazelcastInstance(config);
        hz.getMap("test").put("testKey", "testValue");
        assertNoTextProtocolResponse(hz, TEST_URL_DATA);
        assertNoTextProtocolResponse(hz, TEST_URL_HEALTH_CHECK);
    }

    @Test
    public void testAllRestPropertiesEnabled() throws Exception {
        Config config = new Config()
                .setProperty(GroupProperty.REST_ENABLED.getName(), "true")
                .setProperty(GroupProperty.HTTP_HEALTHCHECK_ENABLED.getName(), "true")
                .setProperty(GroupProperty.MEMCACHE_ENABLED.getName(), "true")
                ;
        HazelcastInstance hz = factory.newHazelcastInstance(config);
        hz.getMap("test").put("testKey", "testValue");
        assertTextProtocolResponse(hz, TEST_URL_DATA);
        assertTextProtocolResponse(hz, TEST_URL_HEALTH_CHECK);
    }

    private void createMemberWithRestConfigAndAssertConfigException(Config config) {
        config.getNetworkConfig().setRestApiConfig(new RestApiConfig());
        expectedException.expect(InvalidConfigurationException.class);
        factory.newHazelcastInstance(config);
    }

    private boolean isExpectedDefaultEnabled(RestEndpointGroup endpointGroup) {
        return endpointGroup == RestEndpointGroup.CLUSTER_READ || endpointGroup == RestEndpointGroup.HEALTH_CHECK;
    }
}
