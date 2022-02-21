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

package com.hazelcast.internal.server;

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.networking.OutboundFrame;
import com.hazelcast.internal.nio.ConnectionLifecycleListener;
import com.hazelcast.internal.nio.ConnectionType;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.internal.util.ExceptionUtil;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

class DroppingServerConnection implements ServerConnection {

    private final Address remoteAddress;
    private final UUID remoteUuid;
    private final long timestamp = Clock.currentTimeMillis();
    private final ServerConnectionManager connectionManager;
    private final ConnectionLifecycleListener lifecycleListener;
    private final AtomicBoolean isAlive = new AtomicBoolean(true);
    private final ConcurrentMap attributeMap = new ConcurrentHashMap();

    DroppingServerConnection(
            ConnectionLifecycleListener lifecycleListener,
            Address remoteAddress,
            UUID remoteUuid,
            ServerConnectionManager connectionManager
    ) {
        this.remoteAddress = remoteAddress;
        this.remoteUuid = remoteUuid;
        this.connectionManager = connectionManager;
        this.lifecycleListener = lifecycleListener;
    }

    @Override
    public ConcurrentMap attributeMap() {
        return attributeMap;
    }

    @Override
    public ServerConnectionManager getConnectionManager() {
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
    public void setConnectionType(String connectionType) {
        //NO OP
    }

    @Override
    public String getConnectionType() {
        return ConnectionType.MEMBER;
    }

    @Override
    public boolean isClient() {
        return false;
    }

    @Override
    public InetSocketAddress getRemoteSocketAddress() {
        try {
            return remoteAddress.getInetSocketAddress();
        } catch (UnknownHostException e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    @Override
    public void setRemoteAddress(Address remoteAddress) {
    }

    @Override
    public void setRemoteUuid(UUID remoteUuid) {
    }

    @Override
    public UUID getRemoteUuid() {
        return remoteUuid;
    }

    @Override
    public Address getRemoteAddress() {
        return remoteAddress;
    }

    @Override
    public InetAddress getInetAddress() {
        return null;
    }
}
