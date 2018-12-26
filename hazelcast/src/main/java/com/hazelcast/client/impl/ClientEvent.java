/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.Client;
import com.hazelcast.core.ClientType;

import java.net.InetSocketAddress;
import java.util.Map;

/**
 * Event used for notification of client connection and disconnection
 */
public class ClientEvent implements Client {

    private final String uuid;
    private final ClientEventType eventType;
    private final InetSocketAddress address;
    private final ClientType clientType;
    private final String name;
    private final Map<String, String> attributes;

    public ClientEvent(String uuid, ClientEventType eventType, InetSocketAddress address,
                       ClientType clientType, String name, Map<String, String> attributes) {
        this.uuid = uuid;
        this.eventType = eventType;
        this.address = address;
        this.clientType = clientType;
        this.name = name;
        this.attributes = attributes;
    }

    @Override
    public String getUuid() {
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

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Map<String, String> getAttributes() {
        return attributes;
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
                + ", name='" + name + '\''
                + ", attributes=" + attributes
                + '}';
    }
}
