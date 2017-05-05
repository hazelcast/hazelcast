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

package com.hazelcast.internal.networking;

import com.hazelcast.internal.networking.nio.NioEventLoopGroup;
import com.hazelcast.internal.networking.spinning.SpinningEventLoopGroup;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.tcp.TcpIpConnection;

/**
 * An abstract of the threading model used by the {@link Connection}.
 *
 * The default implementation of this is the {@link NioEventLoopGroup}
 * that relies on selectors. But also different implementations can be added like spinning, thread per connection etc.
 *
 * Apart from providing a hook to add new functionality, it also simplifies the {@link Connection} and
 * {@link com.hazelcast.nio.ConnectionManager} since concerns are separated:
 * separated:
 * <ol>
 * <li>the ConnectionManager is responsible for managing connections</li>
 * <li>the EventLoopGroup is responsible for providing threads to the connections.</li>
 * </ol>
 *
 * The 2 crucial parts of the EventLoopGroup are the:
 * <ol>
 * <li>{@link ChannelReader}: responsible for reading data from the socket(channel)</li>
 * <li>{@link ChannelWriter}: responsible for writing data to the socket(channel)</li>
 * </ol>
 * The {@link TcpIpConnection} is pretty dumb; it doesn't know anything about threading models; it just owns
 * a {@link ChannelReader} and {@link ChannelWriter}. This keeps the TcpIpConnection very clean and flexible.
 *
 * The idea is that different ChannelReader and ChannelWriter implementations can be made. We already have specific
 * one for non blocking (selector based) IO and for spinning io.  These ChannelReader/ChannelWriter instances only
 * focus on getting data to and from the socket; they do not concern themselves about interpreting the data. This
 * is a concern of the {@link ChannelInboundHandler} and the {@link ChannelOutboundHandler} instance each ChannelReader/ChannelWriter
 * has. So a ChannelReader/ChannelWriter-class is independent of the type of communication that runs on top of it.
 *
 * @see NioEventLoopGroup
 * @see SpinningEventLoopGroup
 */
public interface EventLoopGroup {

    /**
     * Tells whether or not every I/O operation on SocketChannel should block until it completes.
     *
     * @return true if blocking, false otherwise.
     * @see {@link java.nio.channels.SelectableChannel#configureBlocking(boolean)}
     */
    boolean isBlocking();

    /**
     * Creates a new ChannelWriter for the given connection.
     *
     * @param connection the TcpIpConnection to create the ChannelWriter for.
     * @return the created ChannelWriter
     */
    ChannelWriter newSocketWriter(SocketConnection connection);

    /**
     * Creates a new ChannelReader for the given connection.
     *
     * @param connection the TcpIpConnection to create the ChannelReader for.
     * @return the created ChannelReader
     */
    ChannelReader newSocketReader(SocketConnection connection);

    /**
     * Is called when a connection is added.
     *
     * @param connection the connection added.
     */
    void onConnectionAdded(SocketConnection connection);

    /**
     * Is called when a connection is removed.
     *
     * @param connection the connection removed.
     */
    void onConnectionRemoved(SocketConnection connection);

    /**
     * Starts the EventLoopGroup.
      */
    void start();

    /**
     * Shuts down the EventLoopGroup.
      */
    void shutdown();
}
