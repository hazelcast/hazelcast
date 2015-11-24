/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.client.ClientEngine;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.instance.GroupProperty;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Connection;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.util.Clock;

import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

/**
 * Monitors client heartbeats.. As soon as a client has not used its connection for a certain amount of time,
 * the client is disconnected.
 */
public class ClientHeartbeatMonitor implements Runnable {

    private static final int HEART_BEAT_CHECK_INTERVAL_SECONDS = 10;
    private static final int DEFAULT_CLIENT_HEARTBEAT_TIMEOUT_SECONDS = 60;

    private final ClientEndpointManagerImpl clientEndpointManager;
    private final ClientEngine clientEngine;
    private final long heartbeatTimeoutSeconds;
    private final ILogger logger = Logger.getLogger(ClientHeartbeatMonitor.class);
    private final ExecutionService executionService;

    public ClientHeartbeatMonitor(ClientEndpointManagerImpl endpointManager,
                                  ClientEngine clientEngine,
                                  ExecutionService executionService,
                                  GroupProperties groupProperties) {
        this.clientEndpointManager = endpointManager;
        this.clientEngine = clientEngine;
        this.executionService = executionService;
        this.heartbeatTimeoutSeconds = getHeartBeatTimeout(groupProperties);
    }

    private long getHeartBeatTimeout(GroupProperties groupProperties) {
        long configuredTimeout = groupProperties.getSeconds(GroupProperty.CLIENT_HEARTBEAT_TIMEOUT_SECONDS);
        if (configuredTimeout > 0) {
            return configuredTimeout;
        }

        return DEFAULT_CLIENT_HEARTBEAT_TIMEOUT_SECONDS;
    }

    public void start() {
        executionService.scheduleWithFixedDelay(this, HEART_BEAT_CHECK_INTERVAL_SECONDS,
                HEART_BEAT_CHECK_INTERVAL_SECONDS, TimeUnit.SECONDS);
    }

    @Override
    public void run() {
        final String memberUuid = clientEngine.getLocalMember().getUuid();
        for (ClientEndpoint ce : clientEndpointManager.getEndpoints()) {
            ClientEndpointImpl clientEndpoint = (ClientEndpointImpl) ce;
            monitor(memberUuid, clientEndpoint);
        }
    }

    private void monitor(String memberUuid, ClientEndpointImpl clientEndpoint) {
        if (clientEndpoint.isFirstConnection()) {
            return;
        }

        final Connection connection = clientEndpoint.getConnection();
        final long lastTimePackageReceived = connection.lastReadTimeMillis();
        final long timeoutInMillis = TimeUnit.SECONDS.toMillis(heartbeatTimeoutSeconds);
        final long currentTimeInMillis = Clock.currentTimeMillis();
        if (lastTimePackageReceived + timeoutInMillis < currentTimeInMillis) {
            if (memberUuid.equals(clientEndpoint.getPrincipal().getOwnerUuid())) {
                logger.log(Level.WARNING, "Client heartbeat is timed out , closing connection to " + connection);
                connection.close();
            }
        }
    }
}
