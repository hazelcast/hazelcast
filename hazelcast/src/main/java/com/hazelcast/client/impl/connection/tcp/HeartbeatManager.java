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

package com.hazelcast.client.impl.connection.tcp;

import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.connection.ClientConnection;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ClientPingCodec;
import com.hazelcast.client.impl.spi.impl.ClientInvocation;
import com.hazelcast.internal.networking.Channel;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.exception.TargetDisconnectedException;
import com.hazelcast.spi.impl.executionservice.TaskScheduler;

import java.util.Collection;
import java.util.concurrent.ConcurrentMap;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Periodically on each `heartbeatInterval` pings active connections.
 * Connections not receiving any packages for `heartbeatTimeout` duration
 * is closed with {@link TargetDisconnectedException}.
 */
public final class HeartbeatManager {

    private HeartbeatManager() {

    }

    public static void start(HazelcastClientInstanceImpl client,
                             TaskScheduler taskScheduler,
                             ILogger logger,
                             long heartbeatInterval,
                             long heartbeatTimeout,
                             Collection<ClientConnection> connectionsView) {

        HeartbeatChecker heartbeatChecker = new HeartbeatChecker(client,
                logger,
                heartbeatInterval,
                heartbeatTimeout,
                connectionsView);

        taskScheduler.scheduleWithRepetition(heartbeatChecker, heartbeatInterval, heartbeatInterval, MILLISECONDS);
    }

    private static final class HeartbeatChecker implements Runnable {

        private final HazelcastClientInstanceImpl client;
        private final ILogger logger;
        private final long heartbeatInterval;
        private final long heartbeatTimeout;
        private final Collection<ClientConnection> connectionsView;

        private HeartbeatChecker(HazelcastClientInstanceImpl client,
                                 ILogger logger,
                                 long heartbeatInterval,
                                 long heartbeatTimeout,
                                 Collection<ClientConnection> connectionsView) {
            this.client = client;
            this.heartbeatTimeout = heartbeatTimeout;
            this.heartbeatInterval = heartbeatInterval;
            this.connectionsView = connectionsView;
            this.logger = logger;
        }

        @Override
        public void run() {
            long now = Clock.currentTimeMillis();
            for (ClientConnection connection : connectionsView) {
                check(connection, now);

                // Check Alto channels as well, if they exist
                Channel[] altoChannels = connection.getAltoChannels();
                if (altoChannels != null) {
                    for (Channel altoChannel : altoChannels) {
                        check(altoChannel, connection, now);
                    }
                }
            }
        }

        private void check(ClientConnection connection, long now) {
            if (!connection.isAlive()) {
                return;
            }

            if (now - connection.lastReadTimeMillis() > heartbeatTimeout) {
                logger.warning("Heartbeat failed over the connection: " + connection);
                connection.close("Heartbeat timed out",
                        new TargetDisconnectedException("Heartbeat timed out to connection " + connection));
                return;
            }

            if (now - connection.lastWriteTimeMillis() > heartbeatInterval) {
                sendPing(connection);
            }
        }

        private void check(Channel altoChannel, ClientConnection connection, long now) {
            if (altoChannel.isClosed()) {
                return;
            }

            long lastReadTime = altoChannel.lastReadTimeMillis();
            // TODO: remove the lastReadTime > 0 check while doing the
            //  auth changes. Right now, it is needed because we are
            //  not reading anything during the initial connection, hence
            //  it is returning -1, which fails the check immediately
            if (lastReadTime > 0 && now - lastReadTime > heartbeatTimeout) {
                logger.warning("Heartbeat failed over the Alto channel " + altoChannel + " for connection: " + connection);
                connection.close("Heartbeat timed out",
                        new TargetDisconnectedException("Heartbeat timed out to the Alto channel "
                                + altoChannel + " for connection: " + connection));
                return;
            }

            if (now - altoChannel.lastWriteTimeMillis() > heartbeatInterval) {
                ConcurrentMap attributeMap = altoChannel.attributeMap();
                ClientConnection adapter = (ClientConnection) attributeMap.get(AltoChannelClientConnectionAdapter.class);
                sendPing(adapter);
            }
        }

        private void sendPing(ClientConnection connection) {
            ClientMessage request = ClientPingCodec.encodeRequest();
            ClientInvocation invocation = new ClientInvocation(client, request, null, connection);
            invocation.invokeUrgent();
        }
    }
}
