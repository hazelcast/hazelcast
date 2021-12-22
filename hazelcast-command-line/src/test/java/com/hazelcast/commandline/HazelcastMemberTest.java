/*
 * Copyright 2020 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.hazelcast.commandline;

import com.hazelcast.config.Config;
import com.hazelcast.config.InvalidConfigurationException;
import org.junit.After;
import org.junit.Test;

import static org.hamcrest.Matchers.hasItems;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class HazelcastMemberTest {
    @After
    public void clearProperties() {
        System.clearProperty("hazelcast.config");
        System.clearProperty("hazelcast.default.config");
        System.clearProperty("network.port");
        System.clearProperty("network.interface");
    }

    @Test
    public void test_config()
            throws Exception {
        // given
        String port = "1234";
        String networkInterface = "127.0.0.1";
        System.setProperty("network.port", port);
        System.setProperty("network.interface", networkInterface);
        // when
        Config config = HazelcastMember.config();
        // then
        assertEquals(port, String.valueOf(config.getNetworkConfig().getPort()));
        assertTrue(config.getNetworkConfig().getInterfaces().isEnabled());
        assertEquals("false", config.getProperty("hazelcast.socket.bind.any"));
        assertThat(config.getNetworkConfig().getInterfaces().getInterfaces(), hasItems(networkInterface));
    }

    @Test
    public void test_config_userDefinedWithYaml()
            throws Exception {
        // given
        System.setProperty("hazelcast.config", "src/test/resources/test-hazelcast-user-defined.yaml");
        System.setProperty("network.port", "null");
        System.setProperty("network.interface", "null");
        // when
        Config config = HazelcastMember.config();
        // then
        assertEquals("hz-yaml-configured-cluster", config.getClusterName());
    }

    @Test
    public void test_config_userDefinedWithXml()
            throws Exception {
        // given
        System.setProperty("hazelcast.config", "src/test/resources/test-hazelcast-user-defined.xml");
        System.setProperty("network.port", "null");
        System.setProperty("network.interface", "null");
        // when
        Config config = HazelcastMember.config();
        // then
        assertEquals("hz-xml-configured-cluster", config.getClusterName());
    }

    @Test(expected = InvalidConfigurationException.class)
    public void test_config_invalidConfig()
            throws Exception {
        // given
        System.setProperty("hazelcast.config", "src/test/resources/invalid-hazelcast.yaml");
        // when
        HazelcastMember.config();
        // then
        // Exception expected
    }
}
