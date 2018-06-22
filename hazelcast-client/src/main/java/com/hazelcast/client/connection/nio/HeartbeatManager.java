/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.connection.nio;

import com.hazelcast.client.config.ClientIcmpPingConfig;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ClientPingCodec;
import com.hazelcast.client.spi.impl.ClientExecutionServiceImpl;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.exception.TargetDisconnectedException;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.util.Clock;

import java.util.concurrent.TimeUnit;

import static com.hazelcast.client.spi.properties.ClientProperty.HEARTBEAT_INTERVAL;
import static com.hazelcast.client.spi.properties.ClientProperty.HEARTBEAT_TIMEOUT;

/**
 * HeartbeatManager manager used by connection manager.
 */
public class HeartbeatManager implements Runnable {

    private final ClientConnectionManagerImpl clientConnectionManager;
    private final HazelcastClientInstanceImpl client;
    private final ILogger logger;
    private final long heartbeatInterval;
    private final long heartbeatTimeout;
    private final ClientICMPManager clientICMPManager;

    HeartbeatManager(ClientConnectionManagerImpl clientConnectionManager, HazelcastClientInstanceImpl client) {
        this.clientConnectionManager = clientConnectionManager;
        this.client = client;
        HazelcastProperties hazelcastProperties = client.getProperties();
        this.heartbeatTimeout = hazelcastProperties.getPositiveMillisOrDefault(HEARTBEAT_TIMEOUT);
        this.heartbeatInterval = hazelcastProperties.getPositiveMillisOrDefault(HEARTBEAT_INTERVAL);
        this.logger = client.getLoggingService().getLogger(HeartbeatManager.class);
        ClientIcmpPingConfig icmpPingConfig = client.getClientConfig().getNetworkConfig().getClientIcmpPingConfig();
        this.clientICMPManager = new ClientICMPManager(icmpPingConfig,
                (ClientExecutionServiceImpl) client.getClientExecutionService(),
                client.getLoggingService(), clientConnectionManager, this);
    }

    public void start() {
        final ClientExecutionServiceImpl es = (ClientExecutionServiceImpl) client.getClientExecutionService();
        es.scheduleWithRepetition(this, heartbeatInterval, heartbeatInterval, TimeUnit.MILLISECONDS);
        clientICMPManager.start();
    }

    long getHeartbeatTimeout() {
        return heartbeatTimeout;
    }

    @Override
    public void run() {
        if (!clientConnectionManager.alive) {
            return;
        }

        long now = Clock.currentTimeMillis();
        for (final ClientConnection connection : clientConnectionManager.getActiveConnections()) {
            checkConnection(now, connection);
        }
    }

    private void checkConnection(long now, final ClientConnection connection) {
        if (!connection.isAlive()) {
            return;
        }

        if (now - connection.lastReadTimeMillis() > heartbeatTimeout) {
            if (connection.isAlive()) {
                logger.warning("Heartbeat failed over the connection: " + connection);
                onHeartbeatStopped(connection, "Heartbeat timed out");
            }
        }

        if (now - connection.lastReadTimeMillis() > heartbeatInterval) {
            ClientMessage request = ClientPingCodec.encodeRequest();
            ClientInvocation clientInvocation = new ClientInvocation(client, request, null, connection);
            clientInvocation.invokeUrgent();
        }
    }

    void onHeartbeatStopped(ClientConnection connection, String reason) {
        connection.close(reason, new TargetDisconnectedException("Heartbeat timed out to connection " + connection));
    }

    public void shutdown() {
        clientICMPManager.shutdown();
    }
}
