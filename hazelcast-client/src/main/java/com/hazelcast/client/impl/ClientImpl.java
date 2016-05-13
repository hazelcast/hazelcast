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

package com.hazelcast.client.impl;

import com.hazelcast.core.Client;
import com.hazelcast.core.ClientType;

import java.net.InetSocketAddress;
import java.util.UUID;

/**
 * Default {@link com.hazelcast.core.Client} implementation.
 */
public class ClientImpl implements Client {

    private final String uuidString;
    private final InetSocketAddress socketAddress;
    private final UUID uuid;

    public ClientImpl(String uuid, InetSocketAddress socketAddress) {
        this.uuidString = uuid;
        this.uuid = UUID.fromString(uuidString);
        this.socketAddress = socketAddress;
    }

    @Override
    public UUID getUUID() {
        return uuid;
    }

    @Override
    public String getUuid() {
        return uuidString;
    }

    @Override
    public InetSocketAddress getSocketAddress() {
        return socketAddress;
    }

    @Override
    public ClientType getClientType() {
        return ClientType.JAVA;
    }
}
