/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ConnectionManager;
import com.hazelcast.nio.ConnectionType;
import com.hazelcast.nio.OutboundFrame;
import com.hazelcast.test.mocknetwork.MockConnectionManager;
import com.hazelcast.util.Clock;
import com.hazelcast.util.ExceptionUtil;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.concurrent.atomic.AtomicBoolean;

class DroppingConnection implements Connection {

    final Address endpoint;
    final long timestamp = Clock.currentTimeMillis();
    private final ConnectionManager connectionManager;
    private AtomicBoolean isClosing = new AtomicBoolean(false);

    DroppingConnection(Address endpoint, ConnectionManager connectionManager) {
        this.endpoint = endpoint;
        this.connectionManager = connectionManager;
    }

    @Override
    public Throwable getCloseCause() {
        return null;
    }

    @Override
    public String getCloseReason() {
        return null;
    }

    @Override
    public boolean write(OutboundFrame frame) {
        return true;
    }

    @Override
    public boolean isAlive() {
        return true;
    }

    @Override
    public long lastReadTimeMillis() {
        return timestamp;
    }

    @Override
    public long lastWriteTimeMillis() {
        return timestamp;
    }

    @Override
    public void close(String msg, Throwable cause) {
        if (connectionManager instanceof MockConnectionManager) {
            if (isClosing.compareAndSet(false, true)) {
                ((MockConnectionManager)connectionManager).destroyConnection(this);
            }
        }
    }

    @Override
    public void setType(ConnectionType type) {
        //NO OP
    }

    @Override
    public ConnectionType getType() {
        return ConnectionType.MEMBER;
    }

    @Override
    public boolean isClient() {
        return false;
    }

    @Override
    public InetAddress getInetAddress() {
        try {
            return endpoint.getInetAddress();
        } catch (UnknownHostException e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    @Override
    public InetSocketAddress getRemoteSocketAddress() {
        try {
            return endpoint.getInetSocketAddress();
        } catch (UnknownHostException e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    @Override
    public Address getEndPoint() {
        return endpoint;
    }

    @Override
    public int getPort() {
        return endpoint.getPort();
    }
}
