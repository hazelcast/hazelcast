package com.hazelcast.client.spi.impl;

import com.hazelcast.nio.Connection;

/**
 * A listener for the {@link com.hazelcast.client.connection.ClientConnectionManager} to listen to connection heartbeats
 */

public interface ConnectionHeartbeatListener {

    void heartBeatStarted(Connection connection);

    void heartBeatStopped(Connection connection);
}

