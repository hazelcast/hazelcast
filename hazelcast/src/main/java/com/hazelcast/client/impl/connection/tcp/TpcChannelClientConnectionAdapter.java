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

package com.hazelcast.client.impl.connection.tcp;

import com.hazelcast.client.impl.connection.ClientConnection;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.spi.EventHandler;
import com.hazelcast.cluster.Address;
import com.hazelcast.internal.networking.Channel;
import com.hazelcast.internal.networking.OutboundFrame;

import javax.annotation.Nullable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

public class TpcChannelClientConnectionAdapter implements ClientConnection {

    private final Channel channel;

    public TpcChannelClientConnectionAdapter(Channel channel) {
        this.channel = channel;
    }

    @Override
    public boolean write(OutboundFrame frame) {
        return channel.write(frame);
    }

    @Override
    public Address getRemoteAddress() {
        return (Address) channel.attributeMap().get(Address.class);
    }

    @Override
    public String getCloseReason() {
        return "The TPC channel " + channel + " is closed";
    }

    @Override
    public Throwable getCloseCause() {
        return null;
    }

    @Override
    public void handleClientMessage(ClientMessage message) {
        throw new UnsupportedOperationException("Not supported for TPC channels");
    }

    @Override
    public EventHandler getEventHandler(long correlationId) {
        throw new UnsupportedOperationException("Not supported for TPC channels");
    }

    @Override
    public void removeEventHandler(long correlationId) {
        throw new UnsupportedOperationException("Not supported for TPC channels");
    }

    @Override
    public void addEventHandler(long correlationId, EventHandler handler) {
        throw new UnsupportedOperationException("Not supported for TPC channels");
    }

    @Override
    public void setClusterUuid(UUID uuid) {
        throw new UnsupportedOperationException("Not supported for TPC channels");
    }

    @Override
    public UUID getClusterUuid() {
        throw new UnsupportedOperationException("Not supported for TPC channels");
    }

    @Override
    public Map<Long, EventHandler> getEventHandlers() {
        throw new UnsupportedOperationException("Not supported for TPC channels");
    }

    @Nullable
    @Override
    public Channel[] getTpcChannels() {
        throw new UnsupportedOperationException("Not supported for TPC channels");
    }

    @Override
    public ConcurrentMap attributeMap() {
        throw new UnsupportedOperationException("Not supported for TPC channels");
    }

    @Override
    public boolean isAlive() {
        throw new UnsupportedOperationException("Not supported for TPC channels");
    }

    @Override
    public long lastReadTimeMillis() {
        throw new UnsupportedOperationException("Not supported for TPC channels");
    }

    @Override
    public long lastWriteTimeMillis() {
        throw new UnsupportedOperationException("Not supported for TPC channels");
    }

    @Nullable
    @Override
    public InetSocketAddress getRemoteSocketAddress() {
        throw new UnsupportedOperationException("Not supported for TPC channels");
    }

    @Override
    public void setRemoteAddress(Address remoteAddress) {
        throw new UnsupportedOperationException("Not supported for TPC channels");
    }

    @Nullable
    @Override
    public UUID getRemoteUuid() {
        throw new UnsupportedOperationException("Not supported for TPC channels");
    }

    @Override
    public void setRemoteUuid(UUID remoteUuid) {
        throw new UnsupportedOperationException("Not supported for TPC channels");
    }

    @Nullable
    @Override
    public InetAddress getInetAddress() {
        throw new UnsupportedOperationException("Not supported for TPC channels");
    }

    @Override
    public void close(String reason, Throwable cause) {
        throw new UnsupportedOperationException("Not supported for TPC channels");
    }
}
