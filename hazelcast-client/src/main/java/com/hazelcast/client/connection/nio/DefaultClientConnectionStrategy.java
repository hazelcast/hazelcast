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

import com.hazelcast.client.HazelcastClientOfflineException;
import com.hazelcast.client.connection.ClientConnectionStrategy;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.exception.TargetDisconnectedException;

import static com.hazelcast.client.config.ClientConnectionStrategyConfig.ReconnectMode.ASYNC;
import static com.hazelcast.client.config.ClientConnectionStrategyConfig.ReconnectMode.OFF;

/**
 *
 */
public class DefaultClientConnectionStrategy extends ClientConnectionStrategy {

    private volatile boolean disconnectedFromCluster;

    @Override
    public void init() {
        if (clientStartAsync) {
            connectToClusterAsync();
        } else {
            connectToCluster();
        }
    }

    @Override
    public void beforeGetConnection(Address target) {
        if (isClusterAvailable()) {
            return;
        }
        if (clientStartAsync && !disconnectedFromCluster) {
            throw new HazelcastClientOfflineException("Client is connecting to cluster.");
        }
        if (reconnectMode == ASYNC && disconnectedFromCluster) {
            throw new HazelcastClientOfflineException("Client is offline.");
        }
    }

    @Override
    public void beforeOpenConnection(Address target) {
        if (isClusterAvailable()) {
            return;
        }
        if (reconnectMode == ASYNC && disconnectedFromCluster) {
            throw new HazelcastClientOfflineException("Client is offline");
        }
    }

    @Override
    public void onConnectToCluster() {

    }

    @Override
    public void onDisconnectFromCluster() {
        disconnectedFromCluster = true;
        if (reconnectMode == OFF) {
            client.getLifecycleService().shutdown();
            return;
        }
        if (client.getLifecycleService().isRunning()) {
            connectToClusterAsync();
        }
    }

    @Override
    public void onConnect(ClientConnection connection) {

    }

    @Override
    public void onDisconnect(ClientConnection connection) {

    }

    @Override
    public void onHeartbeatStopped(ClientConnection connection) {
        if (connection.isAuthenticatedAsOwner()) {
            connection
                    .close(null, new TargetDisconnectedException("HeartbeatManager timed out to owner connection " + connection));
        }
    }

    @Override
    public void onHeartbeatResumed(ClientConnection connection) {

    }

    private boolean isClusterAvailable() {
        return connectionManager.getOwnerConnectionAddress() != null;
    }
}
