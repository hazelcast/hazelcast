package com.hazelcast.client.impl;

import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.client.ClientEngine;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Connection;
import com.hazelcast.util.Clock;

import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

/**
 * Monitors client heartbeats
 */
public class ClientHeartbeatMonitor implements Runnable {

    private final ClientEndpointManagerImpl clientEndpointManager;
    private final ClientEngine clientEngine;
    private final long heartbeatTimeoutSeconds;
    private final ILogger logger = Logger.getLogger(ClientHeartbeatMonitor.class);
    private final int defaultHeartbeatTimeout = 60;

    public ClientHeartbeatMonitor(long heartbeatTimeoutSeconds,
                                  ClientEndpointManagerImpl endpointManager,
                                  ClientEngine clientEngine) {

        this.clientEndpointManager = endpointManager;
        this.clientEngine = clientEngine;
        this.heartbeatTimeoutSeconds = heartbeatTimeoutSeconds <= 0 ? defaultHeartbeatTimeout : heartbeatTimeoutSeconds;
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
