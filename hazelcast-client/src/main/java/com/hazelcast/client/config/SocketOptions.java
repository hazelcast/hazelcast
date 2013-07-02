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
 * @author mdogan 5/17/13
 */
public class SocketOptions {

    // socket options

    private boolean socketTcpNoDelay = false;

    private boolean socketKeepAlive = true;

    private boolean socketReuseAddress = true;

    private int socketLingerSeconds = 3;

    private int socketTimeout = -1;

    private int socketBufferSize = 32; // in kb


    public boolean isSocketTcpNoDelay() {
        return socketTcpNoDelay;
    }

    public void setSocketTcpNoDelay(boolean socketTcpNoDelay) {
        this.socketTcpNoDelay = socketTcpNoDelay;
    }

    public boolean isSocketKeepAlive() {
        return socketKeepAlive;
    }

    public void setSocketKeepAlive(boolean socketKeepAlive) {
        this.socketKeepAlive = socketKeepAlive;
    }

    public boolean isSocketReuseAddress() {
        return socketReuseAddress;
    }

    public void setSocketReuseAddress(boolean socketReuseAddress) {
        this.socketReuseAddress = socketReuseAddress;
    }

    public int getSocketLingerSeconds() {
        return socketLingerSeconds;
    }

    public void setSocketLingerSeconds(int socketLingerSeconds) {
        this.socketLingerSeconds = socketLingerSeconds;
    }

    public int getSocketTimeout() {
        return socketTimeout;
    }

    public void setSocketTimeout(int socketTimeout) {
        this.socketTimeout = socketTimeout;
    }

    public int getSocketBufferSize() {
        return socketBufferSize;
    }

    public void setSocketBufferSize(int socketBufferSize) {
        this.socketBufferSize = socketBufferSize;
    }
}
