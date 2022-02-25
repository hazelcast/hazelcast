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

package com.hazelcast.internal.nio.ascii;

import com.hazelcast.config.Config;
import com.hazelcast.config.MemcacheProtocolConfig;
import com.hazelcast.config.RestApiConfig;
import com.hazelcast.config.RestEndpointGroup;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.config.RestEndpointGroup.DATA;
import static com.hazelcast.config.RestEndpointGroup.HEALTH_CHECK;
import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static com.hazelcast.test.Accessors.getAddress;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests enabling text protocols by {@link RestApiConfig}, {@link MemcacheProtocolConfig} and legacy system properties.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
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
    public void testMemcachePropertyEnabled() throws Exception {
        Config config = new Config();
        config.getNetworkConfig().getMemcacheProtocolConfig().setEnabled(true);
        HazelcastInstance hz = factory.newHazelcastInstance(config);
        assertNoTextProtocolResponse(hz, TEST_URL_HEALTH_CHECK);
        try (TextProtocolClient client = new TextProtocolClient(getAddress(hz).getInetSocketAddress())) {
            client.connect();
            client.sendData("version\n");
            assertTrueEventually(createResponseAssertTask("Version expected", client, "VERSION Hazelcast"), 10);
        }
    }

    @Test
    public void testMemcacheProtocolDisabled() throws Exception {
        Config config = new Config();
        HazelcastInstance hz = factory.newHazelcastInstance(config);
        assertNoTextProtocolResponse(hz, TEST_URL_DATA);
        assertNoTextProtocolResponse(hz, TEST_URL_HEALTH_CHECK);
    }

    @Test
    public void testRestApiDisabled() throws Exception {
        Config config = new Config();
        HazelcastInstance hz = factory.newHazelcastInstance(config);
        hz.getMap("test").put("testKey", "testValue");
        assertNoTextProtocolResponse(hz, TEST_URL_DATA);
        assertNoTextProtocolResponse(hz, TEST_URL_HEALTH_CHECK);
    }

    @Test
    public void testAllRestPropertiesEnabled() throws Exception {
        Config config = new Config();
        config.getNetworkConfig().getMemcacheProtocolConfig().setEnabled(true);
        RestApiConfig restApiConfig = config.getNetworkConfig().getRestApiConfig();
        restApiConfig.setEnabled(true);
        restApiConfig.enableGroups(DATA);
        HazelcastInstance hz = factory.newHazelcastInstance(config);
        hz.getMap("test").put("testKey", "testValue");
        assertTextProtocolResponse(hz, TEST_URL_DATA);
    }

    private boolean isExpectedDefaultEnabled(RestEndpointGroup endpointGroup) {
        return endpointGroup == RestEndpointGroup.CLUSTER_READ || endpointGroup == RestEndpointGroup.HEALTH_CHECK;
    }
}
