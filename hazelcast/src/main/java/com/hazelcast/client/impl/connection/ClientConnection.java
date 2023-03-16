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

package com.hazelcast.client.impl.connection;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.spi.EventHandler;
import com.hazelcast.internal.networking.Channel;
import com.hazelcast.internal.nio.Connection;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.UUID;

/**
 * The ClientConnection is connection that lives on the client side on behalf of a Java client.
 *
 * On the server side there will be a {@link com.hazelcast.internal.server.ServerConnection}.
 *
 * Use this class to add client specific method.
 */
public interface ClientConnection extends Connection {

    /**
     * Start the processing of a ClientMessage.
     *
     * @param message the message to process.
     */
    void handleClientMessage(ClientMessage message);

    EventHandler getEventHandler(long correlationId);

    void removeEventHandler(long correlationId);

    void addEventHandler(long correlationId, EventHandler handler);

    void setClusterUuid(UUID uuid);

    UUID getClusterUuid();

    // used in tests
    Map<Long, EventHandler> getEventHandlers();

    /**
     * Returns the TPC channels associated with this connection,
     * or {@code null}, if the client is not TPC-aware or the
     * TPC is disabled on the server-side.
     */
    @Nullable
    Channel[] getTpcChannels();
}
