/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ClientPingCodec;
import com.hazelcast.client.spi.impl.ClientExecutionServiceImpl;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.client.spi.impl.ConnectionHeartbeatListener;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.util.Clock;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.client.spi.properties.ClientProperty.HEARTBEAT_INTERVAL;
import static com.hazelcast.client.spi.properties.ClientProperty.HEARTBEAT_TIMEOUT;

/**
 * HeartbeatManager manager used by connection manager.
 */
public class HeartbeatManager implements Runnable {

    private ClientConnectionManagerImpl clientConnectionManager;
    private HazelcastClientInstanceImpl client;
    private final ILogger logger;
    private final long heartbeatInterval;
    private final long heartbeatTimeout;

    private final Set<ConnectionHeartbeatListener> heartbeatListeners = new CopyOnWriteArraySet<ConnectionHeartbeatListener>();

    HeartbeatManager(ClientConnectionManagerImpl clientConnectionManager, HazelcastClientInstanceImpl client) {
        this.clientConnectionManager = clientConnectionManager;
        this.client = client;
        HazelcastProperties hazelcastProperties = client.getProperties();
        long timeout = hazelcastProperties.getMillis(HEARTBEAT_TIMEOUT);
        this.heartbeatTimeout = timeout > 0 ? timeout : Integer.parseInt(HEARTBEAT_TIMEOUT.getDefaultValue());

        long interval = hazelcastProperties.getMillis(HEARTBEAT_INTERVAL);
        this.heartbeatInterval = interval > 0 ? interval : Integer.parseInt(HEARTBEAT_INTERVAL.getDefaultValue());
        this.logger = client.getLoggingService().getLogger(HeartbeatManager.class);

    }

    public void start() {
        ClientExecutionServiceImpl es = (ClientExecutionServiceImpl) client.getClientExecutionService();
        es.scheduleWithRepetition(this, heartbeatInterval, heartbeatInterval, TimeUnit.MILLISECONDS);
    }

    @Override
    public void run() {
        if (!clientConnectionManager.alive) {
            return;
        }
        final long now = Clock.currentTimeMillis();
        for (final ClientConnection connection : clientConnectionManager.getActiveConnections()) {
            if (!connection.isAlive()) {
                continue;
            }

            if (now - connection.lastReadTimeMillis() > heartbeatTimeout) {
                if (connection.isHeartBeating()) {
                    logger.warning("Heartbeat failed over the connection: " + connection);
                    connection.onHeartbeatFailed();
                    fireHeartbeatStopped(connection);
                }
            }
            if (now - connection.lastReadTimeMillis() > heartbeatInterval) {
                ClientMessage request = ClientPingCodec.encodeRequest();
                final ClientInvocation clientInvocation = new ClientInvocation(client, request, null, connection);
                clientInvocation.setBypassHeartbeatCheck(true);
                connection.onHeartbeatRequested();
                clientInvocation.invokeUrgent().andThen(new ExecutionCallback<ClientMessage>() {
                    @Override
                    public void onResponse(ClientMessage response) {
                        if (connection.isAlive()) {
                            connection.onHeartbeatReceived();
                        }
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        if (connection.isAlive()) {
                            logger.warning("Error receiving ping answer from the connection: " + connection, t);
                        }
                    }
                });
            } else {
                if (!connection.isHeartBeating()) {
                    logger.warning("Heartbeat is back to healthy for the connection: " + connection);
                    connection.onHeartbeatResumed();
                    fireHeartbeatResumed(connection);
                }
            }
        }
    }

    private void fireHeartbeatResumed(ClientConnection connection) {
        for (ConnectionHeartbeatListener heartbeatListener : heartbeatListeners) {
            heartbeatListener.heartbeatResumed(connection);
        }
    }

    private void fireHeartbeatStopped(ClientConnection connection) {
        for (ConnectionHeartbeatListener heartbeatListener : heartbeatListeners) {
            heartbeatListener.heartbeatStopped(connection);
        }
    }

    public void addConnectionHeartbeatListener(ConnectionHeartbeatListener connectionHeartbeatListener) {
        heartbeatListeners.add(connectionHeartbeatListener);
    }

    public void shutdown() {
        heartbeatListeners.clear();
    }

}
