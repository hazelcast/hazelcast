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

package com.hazelcast.client.impl.connection.tcp;

import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ClientPingCodec;
import com.hazelcast.client.impl.spi.impl.ClientInvocation;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.exception.TargetDisconnectedException;
import com.hazelcast.spi.impl.executionservice.TaskScheduler;

import java.util.Collection;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Periodically on each `heartbeatInterval` pings active connections.
 * Connections not receiving any packages for `heartbeatTimeout` duration
 * is closed with  {@link TargetDisconnectedException}.
 */
public final class HeartbeatManager {

    private HeartbeatManager() {

    }

    public static void start(HazelcastClientInstanceImpl client,
                             TaskScheduler taskScheduler,
                             ILogger logger,
                             long heartbeatIntervalMillis,
                             long heartbeatTimeoutMillis,
                             Collection<Connection> unmodifiableActiveConnections) {
        taskScheduler.scheduleWithRepetition(() -> {
            long now = Clock.currentTimeMillis();
            for (Connection connection : unmodifiableActiveConnections) {
                if (!connection.isAlive()) {
                    return;
                }

                if (now - connection.lastReadTimeMillis() > heartbeatTimeoutMillis) {
                    logger.warning("Heartbeat failed over the connection: " + connection);
                    connection.close("Heartbeat timed out",
                            new TargetDisconnectedException("Heartbeat timed out to connection " + connection));
                    return;
                }

                if (now - connection.lastWriteTimeMillis() > heartbeatIntervalMillis) {
                    ClientMessage request = ClientPingCodec.encodeRequest();
                    ClientInvocation invocation = new ClientInvocation(client, request, null, connection);
                    invocation.invokeUrgent();
                }
            }
        }, heartbeatIntervalMillis, heartbeatIntervalMillis, MILLISECONDS);
    }

}
