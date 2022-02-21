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

package com.hazelcast.config;

import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.instance.ProtocolType;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.starter.ReflectionUtils;
import nl.jqno.equalsverifier.EqualsVerifier;
import nl.jqno.equalsverifier.Warning;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.IdentityHashMap;

import static com.hazelcast.instance.EndpointQualifier.CLIENT;
import static com.hazelcast.instance.EndpointQualifier.MEMBER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class AdvancedNetworkConfigTest extends HazelcastTestSupport {

    @Rule
    public ExpectedException expected = ExpectedException.none();

    @Test
    public void testDefault() {
        AdvancedNetworkConfig config = new AdvancedNetworkConfig();
        ServerSocketEndpointConfig defaultEndpointConfig = (ServerSocketEndpointConfig) config.getEndpointConfigs().get(MEMBER);

        assertNotNull(defaultEndpointConfig);
        assertEquals(ProtocolType.MEMBER, defaultEndpointConfig.getProtocolType());
        assertEquals(ServerSocketEndpointConfig.DEFAULT_PORT, defaultEndpointConfig.getPort());
        assertEquals(ServerSocketEndpointConfig.PORT_AUTO_INCREMENT, defaultEndpointConfig.getPortCount());
        assertTrue(defaultEndpointConfig.isPortAutoIncrement());
        assertEquals(Integer.parseInt(ClusterProperty.SOCKET_RECEIVE_BUFFER_SIZE.getDefaultValue()),
                defaultEndpointConfig.getSocketRcvBufferSizeKb());
        assertEquals(Integer.parseInt(ClusterProperty.SOCKET_SEND_BUFFER_SIZE.getDefaultValue()),
                defaultEndpointConfig.getSocketSendBufferSizeKb());
    }

    @Test
    public void testFailFast_whenWanPublisherRequiresUndefinedEndpointConfig() {
        Config config = new Config();
        config.getAdvancedNetworkConfig().setEnabled(true);
        config.addWanReplicationConfig(
                new WanReplicationConfig()
                        .setName("seattle-tokyo")
                        .addBatchReplicationPublisherConfig(
                                new WanBatchPublisherConfig()
                                        .setClusterName("target-cluster")
                                        .setEndpoint("does-not-exist")));

        expected.expect(InvalidConfigurationException.class);
        createHazelcastInstance(config);
    }

    @Test
    public void testFailFast_whenNoMemberServerSocketDefined() {
        Config config = new Config();
        config.getAdvancedNetworkConfig()
                .setEnabled(true)
                .getEndpointConfigs().remove(MEMBER);

        expected.expect(InvalidConfigurationException.class);
        createHazelcastInstance(config);
    }

    @Test
    public void test_setMemberServerSocketConfig_replacesPreviousConfig() {
        AdvancedNetworkConfig config = new AdvancedNetworkConfig();
        config.setMemberEndpointConfig(
                new ServerSocketEndpointConfig().setProtocolType(ProtocolType.MEMBER)
                        .setPort(19999)
        );
        config.setMemberEndpointConfig(
                new ServerSocketEndpointConfig().setProtocolType(ProtocolType.MEMBER)
                        .setPort(11000)
        );

        assertEquals(11000, ((ServerSocketEndpointConfig) config.getEndpointConfigs().get(MEMBER)).getPort());
    }

    @Test
    public void test_setClientServerSocketConfig_replacesPreviousConfig() {
        AdvancedNetworkConfig config = new AdvancedNetworkConfig();
        config.setClientEndpointConfig(
                new ServerSocketEndpointConfig().setProtocolType(ProtocolType.CLIENT)
                        .setPort(19999)
        );
        config.setClientEndpointConfig(
                new ServerSocketEndpointConfig().setProtocolType(ProtocolType.CLIENT)
                        .setPort(11000)
        );

        assertEquals(11000, ((ServerSocketEndpointConfig) config.getEndpointConfigs().get(CLIENT)).getPort());
    }

    @Test
    public void test_setTextServerSocketConfig_replacesPreviousConfig() {
        AdvancedNetworkConfig config = new AdvancedNetworkConfig();
        config.setRestEndpointConfig(new RestServerEndpointConfig().setPort(19999));
        config.setRestEndpointConfig(new RestServerEndpointConfig().setPort(11000));

        assertEquals(11000, ((ServerSocketEndpointConfig) config.getEndpointConfigs()
                .get(EndpointQualifier.REST)).getPort());
    }

    @Test
    public void test_setMemcacheEndpointConfig_replacesPreviousConfig() {
        AdvancedNetworkConfig config = new AdvancedNetworkConfig();
        config.setMemcacheEndpointConfig(new ServerSocketEndpointConfig().setPort(19999));
        config.setMemcacheEndpointConfig(new ServerSocketEndpointConfig().setPort(11000));

        assertEquals(11000, ((ServerSocketEndpointConfig) config.getEndpointConfigs()
                .get(EndpointQualifier.MEMCACHE)).getPort());
    }

    @Test
    public void test_setEndpointConfigs_throwsException_whenServerSocketCardinalityBroken() throws Exception {
        IdentityHashMap<EndpointQualifier, EndpointConfig> offendingEndpointConfigs =
                new IdentityHashMap<EndpointQualifier, EndpointConfig>();
        EndpointQualifier one = createReflectively(ProtocolType.MEMBER, "1");
        EndpointQualifier two = createReflectively(ProtocolType.MEMBER, "2");
        offendingEndpointConfigs.put(one, new ServerSocketEndpointConfig());
        offendingEndpointConfigs.put(two, new ServerSocketEndpointConfig());

        AdvancedNetworkConfig config = new AdvancedNetworkConfig();
        expected.expect(InvalidConfigurationException.class);
        config.setEndpointConfigs(offendingEndpointConfigs);
    }

    // bypass EndpointQualifier checks
    private EndpointQualifier createReflectively(ProtocolType protocolType, String name) throws Exception {
        EndpointQualifier endpointQualifier = new EndpointQualifier();
        ReflectionUtils.setFieldValueReflectively(endpointQualifier, "type", protocolType);
        ReflectionUtils.setFieldValueReflectively(endpointQualifier, "identifier", name);
        return endpointQualifier;
    }

    @Test
    public void testEqualsAndHashCode() {
        assumeDifferentHashCodes();
        EqualsVerifier.forClass(AdvancedNetworkConfig.class)
                .usingGetClass()
                .suppress(Warning.NONFINAL_FIELDS)
                .verify();
    }
}
