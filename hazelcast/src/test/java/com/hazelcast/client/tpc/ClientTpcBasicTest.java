/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.tpc;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.connection.ClientConnection;
import com.hazelcast.client.impl.connection.ClientConnectionManager;
import com.hazelcast.client.impl.connection.tcp.TcpClientConnection;
import com.hazelcast.client.test.ClientTestSupport;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.networking.Channel;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class})
public class ClientTpcBasicTest extends ClientTestSupport {

    @After
    public void cleanup() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    private Config getMemberConfig() {
        Config config = new Config();
        // Jet prints too many logs
        config.getJetConfig().setEnabled(false);

        int loopCount = Math.min(Runtime.getRuntime().availableProcessors(), 3);
        config.getTpcConfig()
                .setEnabled(true)
                .setEventloopCount(loopCount);
        return config;
    }

    private ClientConfig getClientConfig() {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getTpcConfig().setEnabled(true);
        return clientConfig;
    }

    private ClientConnectionManager getConnectionManager(HazelcastInstance client) {
        return getHazelcastClientInstanceImpl(client).getConnectionManager();
    }

    @Test
    public void testClientConnectsToAllTpcPorts_byDefault() {
        Config config = getMemberConfig();
        Hazelcast.newHazelcastInstance(config);
        Hazelcast.newHazelcastInstance(config);

        HazelcastInstance client = HazelcastClient.newHazelcastClient(getClientConfig());

        Collection<ClientConnection> connections = getConnectionManager(client).getActiveConnections();
        assertTrueEventually(() -> {
            assertEquals(2, connections.size());
        });

        assertTrueEventually(() -> {
            for (ClientConnection connection : connections) {
                TcpClientConnection clientConnection = (TcpClientConnection) connection;

                Channel[] tpcChannels = clientConnection.getTpcChannels();
                assertNotNull(tpcChannels);
                assertEquals(config.getTpcConfig().getEventloopCount(), tpcChannels.length);

                for (Channel channel : tpcChannels) {
                    assertNotNull(channel);
                    assertFalse(channel.isClosed());
                }
            }
        });
    }
}
