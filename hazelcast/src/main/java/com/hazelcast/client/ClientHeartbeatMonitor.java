package com.hazelcast.client;

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

    private final ClientEndpointManager clientEndpointManager;
    private final ClientEngine clientEngine;
    private final long heartbeatTimeoutSeconds;
    private final ILogger logger = Logger.getLogger(ClientHeartbeatMonitor.class);
    private final int defaultHeartbeatTimeout = 60;

    public ClientHeartbeatMonitor(long heartbeatTimeoutSeconds,
                                  ClientEndpointManager endpointManager,
                                  ClientEngine clientEngine) {

        clientEndpointManager = endpointManager;
        this.clientEngine = clientEngine;
        this.heartbeatTimeoutSeconds = heartbeatTimeoutSeconds <= 0 ? defaultHeartbeatTimeout : heartbeatTimeoutSeconds;
    }


    public void run() {
        final String memberUuid = clientEngine.getLocalMember().getUuid();
        for (ClientEndpoint clientEndpoint : clientEndpointManager.getEndpoints()) {
            if (clientEndpoint.isFirstConnection()) {
                continue;
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
}
