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

package com.hazelcast.client.config;

/**
 * TCP Socket options
 */
public class SocketOptions {

    /**
     * constant for kilobyte
     */
    public static final int KILO_BYTE = 1024;

    /**
     * default buffer size of Bytes
     */
    public static final int DEFAULT_BUFFER_SIZE_BYTE = 128 * KILO_BYTE;

    static final int DEFAULT_BUFFER_SIZE_IN_KB = 128;

    static final int DEFAULT_LINGER_SECONDS = 3;

    // socket options

    private boolean tcpNoDelay = true;

    private boolean keepAlive = true;

    private boolean reuseAddress = true;

    private int lingerSeconds = DEFAULT_LINGER_SECONDS;

    private int bufferSizeInKB = DEFAULT_BUFFER_SIZE_IN_KB;

    public SocketOptions() {
    }

    public SocketOptions(SocketOptions socketOptions) {
        tcpNoDelay = socketOptions.tcpNoDelay;
        keepAlive = socketOptions.keepAlive;
        reuseAddress = socketOptions.reuseAddress;
        lingerSeconds = socketOptions.lingerSeconds;
        bufferSizeInKB = socketOptions.bufferSizeInKB;
    }

    /**
     * TCP_NODELAY socket option
     *
     * @return true if enabled
     */
    public boolean isTcpNoDelay() {
        return tcpNoDelay;
    }

    /**
     * Enable/disable TCP_NODELAY socket option.
     *
     * @param tcpNoDelay the TCP_NODELAY socket option
     * @return SocketOptions configured
     */
    public SocketOptions setTcpNoDelay(boolean tcpNoDelay) {
        this.tcpNoDelay = tcpNoDelay;
        return this;
    }

    /**
     * SO_KEEPALIVE socket option
     *
     * @return true if enabled
     */
    public boolean isKeepAlive() {
        return keepAlive;
    }

    /**
     * Enable/disable SO_KEEPALIVE socket option.
     *
     * @param keepAlive enabled if true
     * @return SocketOptions configured
     */
    public SocketOptions setKeepAlive(boolean keepAlive) {
        this.keepAlive = keepAlive;
        return this;
    }

    /**
     * SO_REUSEADDR socket option.
     *
     * @return true if enabled
     */
    public boolean isReuseAddress() {
        return reuseAddress;
    }

    /**
     * Enable/disable the SO_REUSEADDR socket option.
     *
     * @param reuseAddress enabled if true
     * @return SocketOptions configured
     */
    public SocketOptions setReuseAddress(boolean reuseAddress) {
        this.reuseAddress = reuseAddress;
        return this;
    }

    /**
     * Gets SO_LINGER with the specified linger time in seconds
     *
     * @return lingerSeconds value in seconds
     */
    public int getLingerSeconds() {
        return lingerSeconds;
    }

    /**
     * Enable/disable SO_LINGER with the specified linger time in seconds
     * If set to a value of 0 or less then it is disabled.
     *
     * Default value is {@link SocketOptions#DEFAULT_LINGER_SECONDS}
     *
     * @param lingerSeconds value in seconds
     * @return SocketOptions configured
     */
    public SocketOptions setLingerSeconds(int lingerSeconds) {
        this.lingerSeconds = lingerSeconds;
        return this;
    }

    /**
     * Gets the SO_SNDBUF and SO_RCVBUF option value
     *
     * @return bufferSize KB value
     */
    public int getBufferSize() {
        return bufferSizeInKB;
    }

    /**
     * Sets the SO_SNDBUF and SO_RCVBUF options to the specified value in KB
     *
     * @param bufferSize KB value
     * @return SocketOptions configured
     */
    public SocketOptions setBufferSize(int bufferSize) {
        this.bufferSizeInKB = bufferSize;
        return this;
    }

    @Override
    @SuppressWarnings({"checkstyle:npathcomplexity"})
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SocketOptions that = (SocketOptions) o;

        if (tcpNoDelay != that.tcpNoDelay) {
            return false;
        }
        if (keepAlive != that.keepAlive) {
            return false;
        }
        if (reuseAddress != that.reuseAddress) {
            return false;
        }
        if (lingerSeconds != that.lingerSeconds) {
            return false;
        }
        return bufferSizeInKB == that.bufferSizeInKB;
    }

    @Override
    public int hashCode() {
        int result = (tcpNoDelay ? 1 : 0);
        result = 31 * result + (keepAlive ? 1 : 0);
        result = 31 * result + (reuseAddress ? 1 : 0);
        result = 31 * result + lingerSeconds;
        result = 31 * result + bufferSizeInKB;
        return result;
    }

    @Override
    public String toString() {
        return "SocketOptions{"
                + "tcpNoDelay=" + tcpNoDelay
                + ", keepAlive=" + keepAlive
                + ", reuseAddress=" + reuseAddress
                + ", lingerSeconds=" + lingerSeconds
                + ", bufferSizeInKB=" + bufferSizeInKB
                + '}';
    }
}
