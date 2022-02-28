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

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.impl.YamlClientDomConfigProcessor;
import com.hazelcast.config.Config;
import com.hazelcast.internal.config.YamlMemberDomConfigProcessor;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class ConfigNodeStateTrackerTest {

    @Test
    public void shouldDetectUnappliedMemberConfigEntries() throws Exception {
        Map<String, String> entries = new HashMap<>();
        entries.put("HZ_CLUSTERNAME", "foo");
        entries.put("HZ_CLUSTRNAME", "foo");
        entries.put("HZ_NETWORK_PORT", "5702");
        entries.put("HZ_NETWORK_JOIN_TCPIP_BLE", "false");
        entries.put("HZ_NETWORK_JOIN_MULTCAST_ENABLED", "false");

        ConfigNode configNode = PropertiesToNodeConverter.propsToNode(EnvVariablesConfigParser.member().parse(entries));

        new YamlMemberDomConfigProcessor(true, new Config(), false)
          .buildConfig(new ConfigOverrideElementAdapter(configNode));

        Map<String, String> unprocessed = new ConfigNodeStateTracker().unprocessedNodes(configNode);
        assertTrue(unprocessed.containsKey("hazelcast.network.port"));
        assertTrue(unprocessed.containsKey("hazelcast.clustrname"));
        assertTrue(unprocessed.containsKey("hazelcast.network.join.tcpip.ble"));
        assertTrue(unprocessed.containsKey("hazelcast.network.join.multcast.enabled"));
    }

    @Test
    public void shouldDetectUnappliedClientConfigEntries() {
        Map<String, String> entries = new HashMap<>();
        entries.put("HZCLIENT_FOO", "foo");
        entries.put("HZCLIENT_NETWORK_SOCKETINTERCEPTOR_ENABLE", "true");
        entries.put("HZCLIENT_NETWORK_SMARTROUTING", "true");

        ConfigNode configNode = PropertiesToNodeConverter.propsToNode(EnvVariablesConfigParser.client().parse(entries));

        new YamlClientDomConfigProcessor(true, new ClientConfig(), false)
          .buildConfig(new ConfigOverrideElementAdapter(configNode));

        Map<String, String> unprocessed = new ConfigNodeStateTracker().unprocessedNodes(configNode);
        assertTrue(unprocessed.containsKey("hazelcast-client.foo"));
        assertTrue(unprocessed.containsKey("hazelcast-client.network.socketinterceptor.enable"));
        assertFalse(unprocessed.containsKey("hazelcast-client.network.smartrouting"));
    }
}
