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

package com.hazelcast.client.impl;

import com.hazelcast.client.Client;

import java.net.InetSocketAddress;
import java.util.Set;
import java.util.UUID;

/**
 * Event used for notification of client connection and disconnection
 */
public class ClientEvent implements Client {

    private final UUID uuid;
    private final ClientEventType eventType;
    private final InetSocketAddress address;
    private final String clientType;
    private final String name;
    private final Set<String> labels;

    public ClientEvent(UUID uuid, ClientEventType eventType, InetSocketAddress address, String clientType, String name,
                       Set<String> labels) {
        this.uuid = uuid;
        this.eventType = eventType;
        this.address = address;
        this.clientType = clientType;
        this.name = name;
        this.labels = labels;
    }

    @Override
    public UUID getUuid() {
        return uuid;
    }

    @Override
    public InetSocketAddress getSocketAddress() {
        return address;
    }

    @Override
    public String getClientType() {
        return clientType;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Set<String> getLabels() {
        return labels;
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
                + ", clientType=" + clientType + ", name='" + name + '\'' + ", attributes=" + labels
                + '}';
    }
}
