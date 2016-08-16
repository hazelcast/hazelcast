/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.connection.tcp.spinning;

import com.hazelcast.internal.connection.tcp.TcpIpConnectionManager;
import com.hazelcast.logging.ILogger;
import com.hazelcast.internal.connection.IOService;
import com.hazelcast.internal.connection.tcp.TcpIpConnection;

import java.io.EOFException;

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

        if (e instanceof EOFException) {
            connection.close("Connection closed by the other side", e);
        } else {
            connection.close("Exception in " + getClass().getSimpleName(), e);
        }
    }
}
