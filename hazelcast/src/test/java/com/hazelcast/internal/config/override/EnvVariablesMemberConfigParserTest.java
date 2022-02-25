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

package com.hazelcast.internal.config.override;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class EnvVariablesMemberConfigParserTest extends HazelcastTestSupport {

    private final EnvVariablesConfigParser envVariablesConfigParser = EnvVariablesConfigParser.member();

    @Test
    public void shouldParseEntries() {
        Map<String, String> entries = new HashMap<>();
        entries.put("HZ_CLUSTERNAME", "foo");
        entries.put("HZ_NETWORK_JOIN_TCPIP_ENABLED", "false");
        entries.put("HZ_NETWORK_JOIN_MULTICAST_ENABLED", "false");

        Map<String, String> result = envVariablesConfigParser.parse(entries);

        assertContains(result.entrySet(), entry("hazelcast.clustername", "foo"));
        assertContains(result.entrySet(), entry("hazelcast.network.join.tcpip.enabled", "false"));
        assertContains(result.entrySet(), entry("hazelcast.network.join.multicast.enabled", "false"));
    }

    @Test
    public void shouldIgnoreExistingEnvConfigurationEntries() {
        Map<String, String> entries = new HashMap<>();
        entries.put("HZ_HOME", "/home");
        entries.put("HZ_LICENSE_KEY", "idkfa");
        entries.put("HZ_PHONE_HOME_ENABLED", "false");

        Map<String, String> result = envVariablesConfigParser.parse(entries);

        assertEquals(0, result.size());
    }

    @Test
    public void shouldIgnoreEntriesWithWrongPrefixes() {
        Map<String, String> entries = new HashMap<>();
        entries.put("HZCLIENT_CLUSTER__NAME", "foo");
        entries.put("HZNETWORK_JOIN_TCP__IP_ENABLED", "false");
        entries.put("NETWORK_JOIN_MULTICAST_ENABLED", "false");

        Map<String, String> result = envVariablesConfigParser.parse(entries);

        assertEquals(0, result.size());
    }

    private static <K, V> Map.Entry<K, V> entry(K key, V value) {
        return new AbstractMap.SimpleEntry<>(key, value);
    }
}
