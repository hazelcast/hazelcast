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

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.map.IMap;
import com.hazelcast.map.QueryResultSizeExceededException;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.spi.properties.HazelcastProperty;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;
import java.util.stream.IntStream;

import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static com.hazelcast.test.Accessors.getNode;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class PhoneHomeMapQueryResultLimitIntegrationTest extends HazelcastTestSupport {

    @Rule
    public WireMockRule wireMockRule = new WireMockRule(wireMockConfig().dynamicPort());

    private HazelcastInstance instance;
    private PhoneHome phoneHome;

    @Before
    public void initialise() {
        int port = wireMockRule.port();

        Map<HazelcastProperty, String> properties = Map.of(ClusterProperty.PHONE_HOME_ENABLED, "true",
                ClusterProperty.QUERY_RESULT_SIZE_LIMIT, "65000", ClusterProperty.QUERY_MAX_LOCAL_PARTITION_LIMIT_FOR_PRE_CHECK,
                "1");
        properties.forEach(HazelcastProperty::setSystemProperty);
        instance = createHazelcastInstance();
        Node node = getNode(instance);

        phoneHome = new PhoneHome(node, "http://localhost:" + port + "/ping");
    }

    @Test
    @SuppressWarnings({"ResultOfMethodCallIgnored"})
    public void testMapMetrics() {
        IMap<String, String> phoneHomeMap = instance.getMap("phoneHome");

        int expectedMaxWrites = 74_750; // see QueryResultSizeLimiter on those calculations
        phoneHomeMap.entrySet();
        phoneHomeMap.values();
        IntStream.rangeClosed(0, expectedMaxWrites).forEach(i -> phoneHomeMap.put(
                String.format("key_%d", i), String.format("value %d", i)));

        assertThrows(QueryResultSizeExceededException.class, phoneHomeMap::entrySet);
        assertThrows(QueryResultSizeExceededException.class, phoneHomeMap::values);

        Map<String, String> stats = phoneHome.phoneHome(true);

        assertThat(stats).containsKey(PhoneHomeMetrics.TOTAL_MAP_VALUES_CALLS.getQueryParameter());
        assertThat(stats).containsKey(PhoneHomeMetrics.TOTAL_MAP_ENTRYSET_CALLS.getQueryParameter());
        assertThat(stats).containsKey(PhoneHomeMetrics.TOTAL_MAP_QUERY_SIZE_LIMITER_HITS.getQueryParameter());
        assertThat(stats.get(PhoneHomeMetrics.TOTAL_MAP_VALUES_CALLS.getQueryParameter())).isEqualTo("2");
        assertThat(stats.get(PhoneHomeMetrics.TOTAL_MAP_ENTRYSET_CALLS.getQueryParameter())).isEqualTo("2");
        assertThat(stats.get(PhoneHomeMetrics.TOTAL_MAP_QUERY_SIZE_LIMITER_HITS.getQueryParameter())).isEqualTo("2");
    }
}
