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
    public static final int DEFAULT_BUFFER_SIZE_BYTE = 32 * KILO_BYTE;

    static final int DEFAULT_BUFFER_SIZE = 32;

    // socket options

    private boolean tcpNoDelay = true;

    private boolean keepAlive = true;

    private boolean reuseAddress = true;

    private int lingerSeconds = 3;

    private int bufferSize = DEFAULT_BUFFER_SIZE;

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
     * @param tcpNoDelay
     * @return
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
     * @return lingerSeconds value in seconds
     */
    public int getLingerSeconds() {
        return lingerSeconds;
    }

    /**
     * Enable/disable SO_LINGER with the specified linger time in seconds
     *
     * @param lingerSeconds value in seconds
     * @return SocketOptions configured
     */
    public SocketOptions setLingerSeconds(int lingerSeconds) {
        this.lingerSeconds = lingerSeconds;
        return this;
    }

    /**
     * Gets the SO_SNDBUF and SO_RCVBUF options to the specified value in KB
     * @return bufferSize KB value
     */
    public int getBufferSize() {
        return bufferSize;
    }

    /**
     * Sets the SO_SNDBUF and SO_RCVBUF options to the specified value in KB
     *
     * @param bufferSize KB value
     * @return SocketOptions configured
     */
    public SocketOptions setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
        return this;
    }

}
