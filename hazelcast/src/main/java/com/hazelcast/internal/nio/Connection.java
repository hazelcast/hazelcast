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

package com.hazelcast.internal.nio;

import com.hazelcast.internal.networking.OutboundFrame;
import com.hazelcast.cluster.Address;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.security.cert.Certificate;

/**
 * Represents a 'connection' between two machines. The most important implementation is the
 * {@link com.hazelcast.internal.nio.tcp.TcpIpConnection}.
 */
public interface Connection {

    /**
     * Checks if the Connection is alive.
     *
     * @return true if alive, false otherwise.
     */
    boolean isAlive();

    /**
     * Returns the clock time in milliseconds of the most recent read using this connection.
     *
     * @return the clock time of the most recent read
     */
    long lastReadTimeMillis();

    /**
     * Returns the clock time in milliseconds of the most recent write using this connection.
     *
     * @return the clock time of the most recent write.
     */
    long lastWriteTimeMillis();

    /**
     * Returns the {@link ConnectionType} of this Connection.
     *
     * @return the ConnectionType. It could be that <code>null</code> is returned.
     */
    ConnectionType getType();

    EndpointManager getEndpointManager();

    /**
     * Sets the type of the connection
     *
     * @param type to be set
     */
    void setType(ConnectionType type);

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
     * <p>
     * todo: do we really need this method because we have getInetAddress, InetSocketAddress and getEndPoint.
     */
    InetSocketAddress getRemoteSocketAddress();

    /**
     * Gets the {@link Address} of the other side of this Connection.
     * <p>
     * todo: rename to get remoteAddress?
     *
     * @return the Address.
     */
    Address getEndPoint();

    /**
     * The remote port.
     * <p>
     * todo: rename to getRemotePort?  And do we need it because we already have getEndPoint which returns an address
     * which includes port. It is only used in testing
     *
     * @return the remote port number to which this Connection is connected, or
     * 0 if the socket is not connected yet.
     */
    int getPort();

    /**
     * Writes a outbound frame so it can be received by the other side of the connection. No guarantees are
     * made that the frame is going to be received on the other side.
     * <p>
     * The frame could be stored in an internal queue before it actually is written, so this call
     * does not need to be a synchronous call.
     *
     * @param frame the frame to write.
     * @return false if the frame was not accepted to be written, e.g. because the Connection was not alive.
     * @throws NullPointerException if frame is null.
     */
    boolean write(OutboundFrame frame);

    /**
     * Closes this connection.
     * <p>
     * Pending packets on this connection are discarded
     * <p>
     * If the Connection is already closed, the call is ignored. So it can safely be called multiple times.
     *
     * @param reason the reason this connection is going to be closed. Is allowed to be null.
     * @param cause  the Throwable responsible for closing this connection. Is allowed to be null.
     */
    void close(String reason, Throwable cause);

    /**
     * Gets the reason this Connection was closed. Can be null if no reason was given or if the connection is still active. It
     * is purely meant for debugging to shed some light on why connections are closed.
     * <p>
     * This method is thread-safe and can be called at any moment.
     * <p>
     * If the connection is closed and no reason is available, it is very likely that the close cause does contain the reason
     * of closing.
     *
     * @return the reason this connection was closed.
     * @see #getCloseCause()
     * @see #close(String, Throwable)
     */
    String getCloseReason();

    /**
     * Gets the cause this Connection was closed. Can be null if no cause was given or if the connection is still active. It
     * is purely meant for debugging to shed some light on why connections are closed.
     * <p>
     * This method is thread-safe and can be called at any moment.
     *
     * @return the cause of closing this connection.
     * @see #getCloseReason() ()
     * @see #close(String, Throwable)
     */
    Throwable getCloseCause();

    /**
     * Returns certificate chain of the remote party.
     *
     * @return certificate chain (may be empty) or {@code null}
     */
    Certificate[] getRemoteCertificates();
}
