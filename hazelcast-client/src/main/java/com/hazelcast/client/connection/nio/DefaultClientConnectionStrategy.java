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
import com.hazelcast.client.config.ClientConnectionStrategyConfig;
import com.hazelcast.client.connection.ClientConnectionStrategy;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.exception.TargetDisconnectedException;

import static com.hazelcast.client.config.ClientConnectionStrategyConfig.ReconnectMode.ASYNC;
import static com.hazelcast.client.config.ClientConnectionStrategyConfig.ReconnectMode.OFF;

/**
 * Default client connection strategy supporting async client start, reconnection modes and disabling reconnection.
 */
public class DefaultClientConnectionStrategy extends ClientConnectionStrategy {

    private volatile boolean disconnectedFromCluster;
    private boolean clientStartAsync;
    private ClientConnectionStrategyConfig.ReconnectMode reconnectMode;

    @Override
    public void start() {
        clientStartAsync = clientConnectionStrategyConfig.isAsyncStart();
        reconnectMode = clientConnectionStrategyConfig.getReconnectMode();
        if (clientStartAsync) {
            clientContext.getConnectionManager().connectToClusterAsync();
        } else {
            clientContext.getConnectionManager().connectToCluster();
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
            clientContext.getLifecycleService().shutdown();
            return;
        }
        if (clientContext.getLifecycleService().isRunning()) {
            clientContext.getConnectionManager().connectToClusterAsync();
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
            connection.close(null, new TargetDisconnectedException("Heartbeat timed out to owner connection " + connection));
        }
    }

    @Override
    public void onHeartbeatResumed(ClientConnection connection) {

    }

    @Override
    public void shutdown() {

    }

    private boolean isClusterAvailable() {
        return clientContext.getConnectionManager().getOwnerConnectionAddress() != null;
    }
}
