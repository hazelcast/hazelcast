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

public class SocketOptions {

    // socket options

    private boolean tcpNoDelay = true;

    private boolean keepAlive = true;

    private boolean reuseAddress = true;

    private int lingerSeconds = 3;

    private int bufferSize = 32;
    // in kb

    public boolean isTcpNoDelay() {
        return tcpNoDelay;
    }

    public SocketOptions setTcpNoDelay(boolean tcpNoDelay) {
        this.tcpNoDelay = tcpNoDelay;
        return this;
    }

    public boolean isKeepAlive() {
        return keepAlive;
    }

    public SocketOptions setKeepAlive(boolean keepAlive) {
        this.keepAlive = keepAlive;
        return this;
    }

    public boolean isReuseAddress() {
        return reuseAddress;
    }

    public SocketOptions setReuseAddress(boolean reuseAddress) {
        this.reuseAddress = reuseAddress;
        return this;
    }

    public int getLingerSeconds() {
        return lingerSeconds;
    }

    public SocketOptions setLingerSeconds(int lingerSeconds) {
        this.lingerSeconds = lingerSeconds;
        return this;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public SocketOptions setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
        return this;
    }

}
