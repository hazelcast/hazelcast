/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.connection.nio;

import com.hazelcast.client.config.ClientConnectionStrategyConfig;
import com.hazelcast.client.impl.connection.ClientConnectionStrategy;
import com.hazelcast.client.impl.spi.ClientContext;
import com.hazelcast.cluster.Address;

import java.util.concurrent.RejectedExecutionException;

import static com.hazelcast.client.config.ClientConnectionStrategyConfig.ReconnectMode.OFF;

/**
 * Default client connection strategy supporting async client start, reconnection modes and disabling reconnection.
 */
public class DefaultClientConnectionStrategy extends ClientConnectionStrategy {

    private boolean asyncStart;
    private ClientConnectionStrategyConfig.ReconnectMode reconnectMode;
    private ClusterConnectorService clusterConnectorService;

    @Override
    public void init(ClientContext clientContext) {
        super.init(clientContext);
        this.clusterConnectorService = clientContext.getClusterConnectorService();
        this.asyncStart = clientConnectionStrategyConfig.isAsyncStart();
        this.reconnectMode = clientConnectionStrategyConfig.getReconnectMode();
    }

    @Override
    public void start() {
        if (asyncStart) {
            clusterConnectorService.connectToClusterAsync();
        } else {
            clusterConnectorService.connectToCluster();
        }
    }

    @Override
    public void beforeGetConnection(Address target) {
    }

    @Override
    public void beforeOpenConnection(Address target) {
    }

    @Override
    public void onClusterConnect() {
    }

    @Override
    public void onDisconnectFromCluster() {
        if (reconnectMode == OFF) {
            shutdownWithExternalThread();
            return;
        }
        if (clientContext.getLifecycleService().isRunning()) {
            try {
                clusterConnectorService.connectToClusterAsync();
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

}
