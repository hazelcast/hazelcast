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
    public void after() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    private Config newMemberConfig() {
        Config config = new Config();
        // Jet prints too many logs
        config.getJetConfig().setEnabled(false);
        config.getTpcConfig()
                .setEnabled(true)
                .setEventloopCount(3);
        return config;
    }

    private ClientConfig newClientConfig() {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getTpcConfig().setEnabled(true);
        return clientConfig;
    }

    private ClientConnectionManager getConnectionManager(HazelcastInstance client) {
        return getHazelcastClientInstanceImpl(client).getConnectionManager();
    }

    @Test
    public void testClientConnectsToOneTpcPort_byDefault() {
        Config config = newMemberConfig();
        int memberCount = 2;
        for (int k = 0; k < memberCount; k++) {
            Hazelcast.newHazelcastInstance(config);
        }
        HazelcastInstance client = HazelcastClient.newHazelcastClient(newClientConfig());

        Collection<ClientConnection> connections = getConnectionManager(client).getActiveConnections();
        assertTrueEventually(() -> {
            assertEquals(memberCount, connections.size());
        });

        assertTrueEventually(() -> {
            for (ClientConnection connection : connections) {
                TcpClientConnection clientConnection = (TcpClientConnection) connection;

                Channel[] tpcChannels = clientConnection.getTpcChannels();
                assertNotNull(tpcChannels);
                assertEquals(1, tpcChannels.length);

                for (Channel channel : tpcChannels) {
                    assertNotNull(channel);
                    assertFalse(channel.isClosed());
                }
            }
        });
    }
}
