/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.networking.OutboundFrame;

import javax.annotation.Nullable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

/**
 * Represents a 'connection' between two machines.
 *
 * There are 2 important sub-interfaces:
 * <ol>
 *     <li>{@link com.hazelcast.internal.server.ServerConnection}</li>
 *     <li>{@link com.hazelcast.client.impl.connection.ClientConnection}</li>
 * </ol>
 *
 * For client or server specific behavior it is best to add the logic to these interfaces instead of in
 * this common interface.
 *
 * If you need to attach data to a connection, please consider using the attributeMap instead of adding
 * a lot of extra methods.
 */
public interface Connection {

    /**
     * Returns an attributeMap of this connection.
     *
     * @return the attribute map.
     */
    ConcurrentMap attributeMap();

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
     * Returns the address of the endpoint this Connection is connected to, or
     * <code>null</code> if it is unconnected.
     *
     * @return address of the endpoint.
     * <p>
     * todo: do we really need this method because we have getInetAddress, InetSocketAddress and getEndPoint.
     */
    @Nullable
    InetSocketAddress getRemoteSocketAddress();

    /**
     * Gets the {@link Address} of the other side of this Connection.
     *
     * @return the Address.
     */
    Address getRemoteAddress();

    /**
     * Sets the {@link Address} of the other side of this Connection.
     *
     * @param remoteAddress the remote address.
     */
    void setRemoteAddress(Address remoteAddress);

    /**
     * Gets the {@link UUID} of the other side of this connection.
     * The remote UUID of the connection set is not immediately
     * available after the connection is created.
     * For the member connections, it's set during the
     * {@link com.hazelcast.internal.cluster.impl.MemberHandshake} processing
     * For the client connections, it's set after client
     * authentication is performed.
     * If the other side of connection is not Hazelcast member or
     * native client (when the other side of connection is MEMCACHED
     * or REST client), this method always returns null.
     * @return null or the uuid of the remote endpoint of the connection.
     */
    @Nullable
    UUID getRemoteUuid();

    /**
     * Sets the {@link UUID} of the other side of this connection.
     *
     * @param remoteUuid the uuid of the remote endpoint of the connection.
     */
    void setRemoteUuid(UUID remoteUuid);

    /**
     * Returns remote address of this Connection.
     *
     * @return the remote address. The returned value could be <code>null</code> if the connection is not alive.
     */
    @Nullable
    InetAddress getInetAddress();

    /**
     * Writes an outbound frame, so it can be received by the other side of the connection. No guarantees are
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
     * Writes an outbound frame, so it can be received by the other side of the connection. Frame delivery is ordered
     * with respect to other calls to this method on the same connection instance. No guarantees are made that the frame
     * is going to be received on the other side. However, if the frame is delivered, then all previous ordered frames
     * sent through the same connection instance is guaranteed to be delivered.
     * <p>
     * The frame could be stored in an internal queue before it actually is written, so this call
     * does not need to be a synchronous call.
     *
     * @param frame the frame to write.
     * @return false if the frame was not accepted to be written, e.g. because the Connection was not alive.
     * @throws NullPointerException if frame is null.
     */
    default boolean writeOrdered(OutboundFrame frame) {
        return write(frame);
    }

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
}
