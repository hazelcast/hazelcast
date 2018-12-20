/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.spi.ClientContext;
import com.hazelcast.nio.Address;

import java.util.concurrent.RejectedExecutionException;

import static com.hazelcast.client.config.ClientConnectionStrategyConfig.ReconnectMode.ASYNC;
import static com.hazelcast.client.config.ClientConnectionStrategyConfig.ReconnectMode.OFF;

/**
 * Default client connection strategy supporting async client start, reconnection modes and disabling reconnection.
 */
public class DefaultClientConnectionStrategy extends ClientConnectionStrategy {

    private volatile boolean disconnectedFromCluster;
    private boolean clientStartAsync;
    private ClientConnectionStrategyConfig.ReconnectMode reconnectMode;
    private ClusterConnector connector;

    @Override
    public void init(ClientContext clientContext) {
        super.init(clientContext);
        ClientConnectionManagerImpl connectionManager = (ClientConnectionManagerImpl) clientContext.getConnectionManager();
        this.connector = connectionManager.getClusterConnector();
        this.clientStartAsync = clientConnectionStrategyConfig.isAsyncStart();
        this.reconnectMode = clientConnectionStrategyConfig.getReconnectMode();
    }

    @Override
    public void start() {
        if (clientStartAsync) {
            connector.connectToClusterAsync();
        } else {
            connector.connectToCluster();
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
    public void beforeConnectToCluster(Address target) {
    }

    @Override
    public void onClusterConnect() {
    }

    @Override
    public void onDisconnectFromCluster() {
        disconnectedFromCluster = true;
        if (reconnectMode == OFF) {
            shutdownWithExternalThread();
            return;
        }
        if (clientContext.getLifecycleService().isRunning()) {
            try {
                connector.connectToClusterAsync();
            } catch (RejectedExecutionException r) {
                shutdownWithExternalThread();
            }
        }
    }

    private void shutdownWithExternalThread() {
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    clientContext.getLifecycleService().shutdown();
                } catch (Exception exception) {
                    logger.severe("Exception during client shutdown ", exception);
                }
            }
        }, clientContext.getName() + ".clientShutdown-").start();
    }

    @Override
    public void onConnect(ClientConnection connection) {
    }

    @Override
    public void onDisconnect(ClientConnection connection) {
    }

    @Override
    public void shutdown() {
    }

    private boolean isClusterAvailable() {
        return clientContext.getConnectionManager().getOwnerConnectionAddress() != null;
    }
}
