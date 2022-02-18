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
import com.hazelcast.internal.nio.ConnectionType;

import java.net.InetSocketAddress;
import java.util.Set;
import java.util.UUID;

/**
 * Default {@link Client} implementation.
 */
public class ClientImpl implements Client {

    private final UUID uuid;
    private final InetSocketAddress socketAddress;
    private final String name;
    private final Set<String> labels;

    public ClientImpl(UUID uuid, InetSocketAddress socketAddress, String name, Set<String> labels) {
        this.uuid = uuid;
        this.socketAddress = socketAddress;
        this.name = name;
        this.labels = labels;
    }

    @Override
    public UUID getUuid() {
        return uuid;
    }

    @Override
    public InetSocketAddress getSocketAddress() {
        return socketAddress;
    }

    @Override
    public String getClientType() {
        return ConnectionType.JAVA_CLIENT;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Set<String> getLabels() {
        return labels;
    }
}
