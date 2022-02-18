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

package com.hazelcast.internal.server.tcp;

import com.hazelcast.config.Config;
import com.hazelcast.config.ServerSocketEndpointConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.HazelcastInstanceProxy;
import com.hazelcast.internal.nio.ascii.TextProtocolClient;
import com.hazelcast.internal.server.ServerConnectionManager;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.test.annotation.Repeat;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.net.InetAddress;

import static com.hazelcast.instance.EndpointQualifier.CLIENT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(NightlyTest.class)
public class SingleProtocolHandlerRegressionTest extends HazelcastTestSupport {

    private static final long ASSERT_TRUE_TIMEOUT_SECS = 20;
    private TextProtocolClient textProtocolClient;

    @After
    public void cleanup() throws IOException {
        if (textProtocolClient != null) {
            textProtocolClient.close();
        }
        Hazelcast.shutdownAll();
    }

    /**
     * Regression test for #19756
     */
    @Test
    @Repeat(50)
    public void testWrongProtocolRegressionTest() {
        HazelcastInstance instance = Hazelcast.newHazelcastInstance(createMemberEndpointConfig());
        ServerConnectionManager connectionManager = ((HazelcastInstanceProxy) instance).getOriginal().node
                .getServer().getConnectionManager(CLIENT);
        // Get address of the client endpoint
        InetAddress address = instance.getCluster().getLocalMember().getSocketAddress(CLIENT).getAddress();
        int port = instance.getCluster().getLocalMember().getSocketAddress(CLIENT).getPort();
        textProtocolClient = new TextProtocolClient(address, port);
        textProtocolClient.connect();

        assertTrueEventually(() -> assertTrue(textProtocolClient.isConnected()), ASSERT_TRUE_TIMEOUT_SECS);
        assertTrueEventually(() -> assertEquals(connectionManager.getConnections().size(), 1), ASSERT_TRUE_TIMEOUT_SECS);
        // Send wrong protocol data to client endpoint
        textProtocolClient.sendData("AAACP2CP2");
        // Assert that connection must be closed on the member side
        assertTrueEventually(
                () -> assertTrue("Connection must be closed on the member side", connectionManager.getConnections().isEmpty()),
                ASSERT_TRUE_TIMEOUT_SECS
        );
    }


    protected Config createMemberEndpointConfig() {
        ServerSocketEndpointConfig endpointConfig = new ServerSocketEndpointConfig();
        endpointConfig.setName("CLIENT")
                .setPort(10000)
                .setPortAutoIncrement(true);

        Config config = new Config();
        config.getAdvancedNetworkConfig()
                .setEnabled(true)
                .setClientEndpointConfig(endpointConfig);
        return config;
    }
}
