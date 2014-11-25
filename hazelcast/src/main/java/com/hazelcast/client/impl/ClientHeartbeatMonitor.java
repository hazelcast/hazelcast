package com.hazelcast.client.impl;

import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.client.ClientEngine;
import com.hazelcast.instance.GroupProperties;
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
        long configuredTimeout = groupProperties.CLIENT_HEARTBEAT_TIMEOUT_SECONDS.getInteger();
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
        final long lastTimePackageReceived = connection.lastReadTime();
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
