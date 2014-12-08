/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.nio;

import java.net.InetAddress;
import java.net.InetSocketAddress;

/**
 * Represents a 'connection' between two machines. The most important implementation is the
 * {@link com.hazelcast.nio.tcp.TcpIpConnection}.
 */
public interface Connection {

    /**
     * Writes a SocketWritable packet to the other side.
     * <p/>
     * The packet could be stored in an internal queue before it actually is written, so this call
     * doesn't need to be a synchronous call.
     *
     * @param packet the packet to write.
     * @return false if the packet was not accepted to be written, e.g. because the Connection was
     * not alive.
     * @throws NullPointerException if packet is null.
     */
    boolean write(SocketWritable packet);

    /**
     * Checks if the Connection is still alive.
     *
     * @return true if alive, false otherwise.
     */
    boolean isAlive();

    /**
     * Returns the clock time of the most recent read using this connection.
     *
     * @return the clock time of the most recent read
     */
    long lastReadTime();

    /**
     * Returns the clock time of the most recent write using this connection.
     *
     * @return the clock time of the most recent write.
     */
    long lastWriteTime();

    /**
     * Closes this connection.
     * <p/>
     * todo: what happens with all pending SocketWritables? Are they flushed, discarded or undefined behavior?
     * <p/>
     * If the Connection already is closed, the call is ignored. So it can safely be called multiple times.
     */
    void close();

    /**
     * Returns the {@link ConnectionType} of this Connection.
     *
     * @return the ConnectionType. It could be that <code>null</code> is returned.
     */
    ConnectionType getType();

    /**
     * Checks if it is a client connection.
     *
     * @return true if client connection, false otherwise.
     */
    boolean isClient();

    /**
     * Returns remote address of this Connection.
     *
     * @return the remote address. The returned value could be <code>null</code> if the connection is not alive.
     */
    InetAddress getInetAddress();

    /**
     * Returns the address of the endpoint this Connection is connected to, or
     * <code>null</code> if it is unconnected.
     *
     * @return address of the endpoint.
     * <p/>
     * todo: do we really need this method because we have getInetAddress, InetSocketAddress and getEndPoint.
     */
    InetSocketAddress getRemoteSocketAddress();

    /**
     * Gets the {@link Address} of the other side of this Connection.
     * <p/>
     * todo: rename to get remoteAddress?
     *
     * @return the Address.
     */
    Address getEndPoint();

    /**
     * The remote port.
     * <p/>
     * todo: rename to getRemotePort?  And do we need it because we already have getEndPoint which returns an address
     * which includes port. It is only used in testing
     *
     * @return the remote port number to which this Connection is connected, or
     * 0 if the socket is not connected yet.
     */
    int getPort();
}
