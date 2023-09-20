/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.config.tpc;

import com.hazelcast.spi.annotation.Beta;

import javax.annotation.Nonnull;
import java.util.Objects;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.Preconditions.checkPositive;

/**
 * Socket configuration for TPC. In TPC, each eventloop has its own
 * sockets.
 *
 * @see com.hazelcast.config.tpc.TpcConfig
 * @since 5.3
 */
@SuppressWarnings("checkstyle:JavadocVariable")
@Beta
public class TpcSocketConfig {
    /**
     * @see java.net.SocketOptions#SO_RCVBUF
     */
    public static final int DEFAULT_RECEIVE_BUFFER_SIZE_KB = 128;
    /**
     * @see java.net.SocketOptions#SO_SNDBUF
     */
    public static final int DEFAULT_SEND_BUFFER_SIZE_KB = 128;

    private String portRange = "11000-21000";
    private int receiveBufferSizeKB = DEFAULT_RECEIVE_BUFFER_SIZE_KB;
    private int sendBufferSizeKB = DEFAULT_SEND_BUFFER_SIZE_KB;

    /**
     * Gets the possible port range for TPC sockets to bind. Can't return
     * null.
     *
     * @return the port range string
     */
    @Nonnull
    public String getPortRange() {
        return portRange;
    }

    /**
     * Sets the possible port range for TPC sockets to bind. Can't return
     * null.
     *
     * @param portRange the port range to set
     * @return this TPC socket config
     * @throws IllegalArgumentException if portRange doesn't match {@code
     *                                  \d{1,5}-\d{1,5}} regular expression
     * @throws NullPointerException     if portRange is null
     */
    @Nonnull
    public TpcSocketConfig setPortRange(@Nonnull String portRange) {
        checkNotNull(portRange);
        if (!portRange.matches("\\d{1,5}-\\d{1,5}")) {
            throw new IllegalArgumentException("Invalid port range");
        }

        this.portRange = portRange;
        return this;
    }

    /**
     * Gets the receive-buffer size of the TPC sockets in kilobytes.
     *
     * @return the receive-buffer size of the TPC sockets in kilobytes
     * @see java.net.SocketOptions#SO_RCVBUF
     */
    public int getReceiveBufferSizeKB() {
        return receiveBufferSizeKB;
    }

    /**
     * Sets the receive-buffer size of the TPC sockets in kilobytes. Can't
     * return null.
     *
     * @param receiveBufferSizeKB the receive-buffer size of the TPC sockets in kilobytes
     * @return this TPC socket config
     * @throws IllegalArgumentException if receiveBufferSizeKB isn't positive
     * @see java.net.SocketOptions#SO_RCVBUF
     */
    @Nonnull
    public TpcSocketConfig setReceiveBufferSizeKB(int receiveBufferSizeKB) {
        this.receiveBufferSizeKB = checkPositive("receiveBufferSizeKB", receiveBufferSizeKB);
        return this;
    }

    /**
     * Gets the send-buffer size of the TPC sockets in kilobytes.
     *
     * @return the send-buffer size of the TPC sockets in kilobytes
     * @see java.net.SocketOptions#SO_SNDBUF
     */
    public int getSendBufferSizeKB() {
        return sendBufferSizeKB;
    }

    /**
     * Sets the send-buffer size of the TPC sockets in kilobytes. Can't
     * return null.
     *
     * @param sendBufferSizeKB the send-buffer size of the TPC sockets in kilobytes
     * @return this TPC socket config
     * @throws IllegalArgumentException if sendBufferSizeKB isn't positive
     * @see java.net.SocketOptions#SO_SNDBUF
     */
    @Nonnull
    public TpcSocketConfig setSendBufferSizeKB(int sendBufferSizeKB) {
        this.sendBufferSizeKB = checkPositive("sendBufferSizeKB", sendBufferSizeKB);
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TpcSocketConfig that = (TpcSocketConfig) o;
        return receiveBufferSizeKB == that.receiveBufferSizeKB
                && sendBufferSizeKB == that.sendBufferSizeKB
                && portRange.equals(that.portRange);
    }

    @Override
    public int hashCode() {
        return Objects.hash(portRange, receiveBufferSizeKB, sendBufferSizeKB);
    }

    @Override
    public String toString() {
        return "TpcSocketConfig{"
                + "portRange='" + portRange + '\''
                + ", receiveBufferSizeKB=" + receiveBufferSizeKB
                + ", sendBufferSizeKB=" + sendBufferSizeKB
                + '}';
    }
}
