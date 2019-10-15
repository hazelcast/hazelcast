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

package com.hazelcast.internal.nio.tcp;

import com.hazelcast.internal.networking.OutboundFrame;
import com.hazelcast.cluster.Address;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.nio.ConnectionLifecycleListener;
import com.hazelcast.internal.nio.EndpointManager;
import com.hazelcast.internal.nio.ConnectionType;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.internal.util.ExceptionUtil;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.security.cert.Certificate;
import java.util.concurrent.atomic.AtomicBoolean;

class DroppingConnection implements Connection {

    private final Address endpoint;
    private final long timestamp = Clock.currentTimeMillis();
    private final EndpointManager connectionManager;
    private final ConnectionLifecycleListener lifecycleListener;
    private AtomicBoolean isAlive = new AtomicBoolean(true);

    DroppingConnection(ConnectionLifecycleListener lifecycleListener, Address endpoint, EndpointManager connectionManager) {
        this.endpoint = endpoint;
        this.connectionManager = connectionManager;
        this.lifecycleListener = lifecycleListener;
    }

    @Override
    public EndpointManager getEndpointManager() {
        return connectionManager;
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
        return isAlive.get();
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
        if (!isAlive.compareAndSet(true, false)) {
            return;
        }
        lifecycleListener.onConnectionClose(this, cause, false);
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

    @Override
    public Certificate[] getRemoteCertificates() {
        return null;
    }
}
