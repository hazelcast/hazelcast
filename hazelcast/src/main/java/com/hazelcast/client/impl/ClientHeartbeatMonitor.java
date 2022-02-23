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

package com.hazelcast.client.impl;

import com.hazelcast.logging.ILogger;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.spi.impl.executionservice.ExecutionService;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.internal.util.Clock;

import static com.hazelcast.internal.util.StringUtil.timeToString;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Monitors client heartbeats.. As soon as a client has not used its connection for a certain amount of time,
 * the client is disconnected.
 */
public class ClientHeartbeatMonitor implements Runnable {

    private static final int HEART_BEAT_CHECK_INTERVAL_SECONDS = 10;
    private static final int DEFAULT_CLIENT_HEARTBEAT_TIMEOUT_SECONDS = 60;

    private final ClientEndpointManager clientEndpointManager;
    private final long heartbeatTimeoutSeconds;
    private final ExecutionService executionService;
    private final ILogger logger;

    public ClientHeartbeatMonitor(ClientEndpointManager clientEndpointManager,
                                  ILogger logger,
                                  ExecutionService executionService,
                                  HazelcastProperties hazelcastProperties) {
        this.clientEndpointManager = clientEndpointManager;
        this.logger = logger;
        this.executionService = executionService;
        this.heartbeatTimeoutSeconds = getHeartbeatTimeout(hazelcastProperties);
    }

    private long getHeartbeatTimeout(HazelcastProperties hazelcastProperties) {
        long configuredTimeout = hazelcastProperties.getSeconds(ClusterProperty.CLIENT_HEARTBEAT_TIMEOUT_SECONDS);
        if (configuredTimeout > 0) {
            return configuredTimeout;
        }

        return DEFAULT_CLIENT_HEARTBEAT_TIMEOUT_SECONDS;
    }

    public void start() {
        executionService.scheduleWithRepetition(this, HEART_BEAT_CHECK_INTERVAL_SECONDS,
                HEART_BEAT_CHECK_INTERVAL_SECONDS, SECONDS);
    }

    @Override
    public void run() {
        cleanupEndpointsWithDeadConnections();

        for (ClientEndpoint clientEndpoint : clientEndpointManager.getEndpoints()) {
            monitor(clientEndpoint);
        }
    }

    private void cleanupEndpointsWithDeadConnections() {
        for (ClientEndpoint endpoint : clientEndpointManager.getEndpoints()) {
            if (!endpoint.getConnection().isAlive()) {
                //if connection is not alive, it means we come across an edge case.
                //normally connection close should remove endpoint from client endpoint manager
                //this means that connection.close happened before, authentication complete(endpoint registered to manager)
                //therefore connection.close could not remove the endpoint.
                //we will remove the endpoint here when detected.
                if (logger.isFineEnabled()) {
                    logger.fine("Cleaning up endpoints with dead connection " + endpoint);
                }
                clientEndpointManager.removeEndpoint(endpoint);
            }
        }

    }

    private void monitor(ClientEndpoint clientEndpoint) {
        Connection connection = clientEndpoint.getConnection();
        long lastTimePacketReceived = connection.lastReadTimeMillis();
        long timeoutInMillis = SECONDS.toMillis(heartbeatTimeoutSeconds);
        long currentTimeMillis = Clock.currentTimeMillis();
        if (lastTimePacketReceived + timeoutInMillis < currentTimeMillis) {
            String message = "Client heartbeat is timed out, closing connection to " + connection
                    + ". Now: " + timeToString(currentTimeMillis)
                    + ". LastTimePacketReceived: " + timeToString(lastTimePacketReceived);
            connection.close(message, null);
        }
    }
}
