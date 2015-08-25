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

package com.hazelcast.nio.tcp.spinning;

import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.ConnectionType;
import com.hazelcast.nio.IOService;
import com.hazelcast.nio.tcp.TcpIpConnection;
import com.hazelcast.nio.tcp.TcpIpConnectionManager;

import java.io.IOException;
import java.util.logging.Level;

public abstract class AbstractHandler {

    protected final TcpIpConnectionManager connectionManager;
    protected final TcpIpConnection connection;
    protected final ILogger logger;
    protected final IOService ioService;

    public AbstractHandler(TcpIpConnection connection, ILogger logger) {
        this.connection = connection;
        this.connectionManager = connection.getConnectionManager();
        this.logger = logger;
        this.ioService = connectionManager.getIoService();
    }

    public void onFailure(Throwable e) {
        if (e instanceof OutOfMemoryError) {
            connectionManager.getIoService().onOutOfMemory((OutOfMemoryError) e);
        }

        connection.close(e);
        ConnectionType connectionType = connection.getType();
        if (connectionType.isClient() && !connectionType.isBinary()) {
            return;
        }
        StringBuilder sb = new StringBuilder();
        sb.append(Thread.currentThread().getName());
        sb.append(" Closing socket to endpoint ");
        sb.append(connection.getEndPoint());
        sb.append(", Cause:").append(e);
        Level level = connectionManager.getIoService().isActive() ? Level.WARNING : Level.FINEST;
        if (e instanceof IOException) {
            logger.log(level, sb.toString());
        } else {
            logger.log(level, sb.toString(), e);
        }
    }
}
