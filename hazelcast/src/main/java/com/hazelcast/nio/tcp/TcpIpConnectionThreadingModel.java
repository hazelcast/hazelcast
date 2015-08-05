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

package com.hazelcast.nio.tcp;

/**
 * An abstract of the threading model used by the {@link TcpIpConnection}.
 *
 * The default implementation of this is the {@link com.hazelcast.nio.tcp.nonblocking.NonBlockingTcpIpConnectionThreadingModel}
 * that relies on selectors. But also different implementations can be added like spinning, thread per connection etc.
 *
 * Apart from providing a hook to add new functionality, it also simplifies the {@link TcpIpConnection} and
 * {@link TcpIpConnectionManager} since a lot of complexity is moved out into a self contained module; keeping them
 * more pure.
 */
public interface TcpIpConnectionThreadingModel {

    /**
     * Tells whether or not every I/O operation on SocketChannel should block until it completes.
     *
     * @return true if blocking, false otherwise.
     * @see {@link java.nio.channels.SelectableChannel#configureBlocking(boolean)}
     */
    boolean isBlocking();

    /**
     * Creates a new WriteHandler for the given connection.
     *
     * @param connection the TcpIpConnection to create the WriteHandler for.
     * @return the created WriteHandler
     */
    WriteHandler newWriteHandler(TcpIpConnection connection);

    /**
     * Creates a new ReadHandler for the given connection.
     *
     * @param connection the TcpIpConnection to create the ReadHandler for.
     * @return the created ReadHandler
     */
    ReadHandler newReadHandler(TcpIpConnection connection);

    /**
     * Is called when a connection is added.
     *
     * @param connection the connection added.
     */
    void onConnectionAdded(TcpIpConnection connection);

    /**
     * Is called when a connection is removed.
     *
     * @param connection the connection removed.
     */
    void onConnectionRemoved(TcpIpConnection connection);

    /**
     * Starts the ReadWriteHandlerFactory. Is called by the {@link TcpIpConnectionManager} when it starts.
     */
    void start();

    /**
     * Shuts down the ReadWriteHandlerFactory. Is called by the {@link TcpIpConnectionManager} when it shuts down.
     */
    void shutdown();
}
