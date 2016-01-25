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
 * The default implementation of this is the {@link com.hazelcast.nio.tcp.nonblocking.NonBlockingIOThreadingModel}
 * that relies on selectors. But also different implementations can be added like spinning, thread per connection etc.
 *
 * Apart from providing a hook to add new functionality, it also simplifies the {@link TcpIpConnection} and
 * {@link TcpIpConnectionManager} since concerns are separated:
 * separated:
 * <ol>
 * <li>the TcpIpConnectManager is responsible for managing connections</li>
 * <li>the IOThreadingModel is responsible for providing threads to the connections.</li>
 * </ol>
 *
 * The 2 crucial parts of the IOThreadingModel are the:
 * <ol>
 * <li>{@link SocketReader}: responsible for reading data from the socket(channel)</li>
 * <li>{@link SocketWriter}: responsible for writing data to the socket(channel)</li>
 * </ol>
 * The {@link TcpIpConnection} is pretty dumb; it doesn't know anything about threading models; it just owns
 * a {@link SocketReader} and {@link SocketWriter}. This keeps the TcpIpConnection very clean and flexible.
 *
 * The idea is that different SocketReader and SocketWriter implementations can be made. We already have specific
 * one for non blocking (selector based) IO and for spinning io.  These SocketReader/SocketWriter instances only
 * focus on getting data to and from the socket; they do not concern themselves about interpreting the data. This
 * is a concern of the {@link ReadHandler} and the {@link WriteHandler} instance each SocketReader/SocketWriter
 * has. So a SocketReader/SocketWriter-class is independent of the type of communication that runs on top of it.
 *
 * @see com.hazelcast.nio.tcp.nonblocking.NonBlockingIOThreadingModel
 * @see com.hazelcast.nio.tcp.spinning.SpinningIOThreadingModel
 * @see SocketReader
 * @see SocketWriter
 * @see ReadHandler
 * @see WriteHandler
 * @see TcpIpConnection
 */
public interface IOThreadingModel {

    /**
     * Tells whether or not every I/O operation on SocketChannel should block until it completes.
     *
     * @return true if blocking, false otherwise.
     * @see {@link java.nio.channels.SelectableChannel#configureBlocking(boolean)}
     */
    boolean isBlocking();

    /**
     * Creates a new SocketWriter for the given connection.
     *
     * @param connection the TcpIpConnection to create the SocketWriter for.
     * @return the created SocketWriter
     */
    SocketWriter newSocketWriter(TcpIpConnection connection);

    /**
     * Creates a new SocketReader for the given connection.
     *
     * @param connection the TcpIpConnection to create the SocketReader for.
     * @return the created SocketReader
     */
    SocketReader newSocketReader(TcpIpConnection connection);

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
     * Starts the IOThreadingModel. Is called by the {@link TcpIpConnectionManager} when it starts.
     *
     * @see TcpIpConnectionManager#start()
     */
    void start();

    /**
     * Shuts down the IOThreadingModel. Is called by the {@link TcpIpConnectionManager} when it shuts down.
     *
     * @see TcpIpConnectionManager#shutdown()
     */
    void shutdown();
}
