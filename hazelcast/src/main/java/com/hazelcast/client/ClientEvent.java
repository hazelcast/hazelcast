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

package com.hazelcast.client;

import com.hazelcast.core.Client;
import com.hazelcast.core.ClientType;
import com.hazelcast.spi.annotation.PrivateApi;

import java.net.InetSocketAddress;
import java.util.UUID;

/**
 * Event used for notification of client connection and disconnection
 *
 * //todo: this is bad modelling. An event is not a client.
 */
@PrivateApi
public class ClientEvent implements Client {

    private final UUID uuid;
    private final ClientEventType eventType;
    private final InetSocketAddress address;
    private final ClientType clientType;

    public ClientEvent(UUID uuid, ClientEventType eventType, InetSocketAddress address, ClientType clientType) {
        this.uuid = uuid;
        this.eventType = eventType;
        this.address = address;
        this.clientType = clientType;
    }

    @Override
    public String getUuid() {
        return uuid.toString();
    }

    @Override
    public UUID getUUID() {
        return uuid;
    }

    @Override
    public InetSocketAddress getSocketAddress() {
        return address;
    }

    @Override
    public ClientType getClientType() {
        return clientType;
    }

    public ClientEventType getEventType() {
        return eventType;
    }

    @Override
    public String toString() {
        return "ClientEvent{"
                + "uuid='" + uuid + '\''
                + ", eventType=" + eventType
                + ", address=" + address
                + ", clientType=" + clientType
                + '}';
    }
}
