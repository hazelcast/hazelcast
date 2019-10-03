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

package com.hazelcast.client.impl.management;

import com.hazelcast.client.impl.ClientDelegatingFuture;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.codec.MCChangeClusterStateRequestCodec;
import com.hazelcast.client.impl.protocol.codec.MCGetMapConfigRequestCodec;
import com.hazelcast.client.impl.protocol.codec.MCUpdateMapConfigRequestCodec;
import com.hazelcast.client.impl.protocol.codec.holder.MapConfigHolder;
import com.hazelcast.client.impl.spi.impl.ClientInvocation;
import com.hazelcast.cluster.ClusterState;
import com.hazelcast.internal.serialization.InternalSerializationService;

public class ManagementCenterService {

    private final HazelcastClientInstanceImpl client;
    private final InternalSerializationService serializationService;

    public ManagementCenterService(HazelcastClientInstanceImpl client,
                                   InternalSerializationService serializationService) {

        this.client = client;
        this.serializationService = serializationService;
    }

    public ClientDelegatingFuture<Void> changeClusterState(ClusterState newState) {
        ClientInvocation invocation = new ClientInvocation(
                client,
                MCChangeClusterStateRequestCodec.encodeRequest(newState.name()),
                null
        );
        return new ClientDelegatingFuture<>(
                invocation.invoke(),
                serializationService,
                clientMessage -> {
                    MCChangeClusterStateRequestCodec.decodeResponse(clientMessage);
                    return null;
                }
        );
    }

    public ClientDelegatingFuture<MapConfigHolder> getMapConfig(String map) {
        ClientInvocation invocation = new ClientInvocation(
                client,
                MCGetMapConfigRequestCodec.encodeRequest(map),
                map
        );
        return new ClientDelegatingFuture<>(
                invocation.invoke(),
                serializationService,
                clientMessage -> MCGetMapConfigRequestCodec.decodeResponse(clientMessage).response,
                true
        );
    }

    public ClientDelegatingFuture<Void> updateMapConfig(String map, MapConfigHolder newMapConfig) {
        ClientInvocation invocation = new ClientInvocation(
                client,
                MCUpdateMapConfigRequestCodec.encodeRequest(map, newMapConfig),
                null
        );
        return new ClientDelegatingFuture<>(
                invocation.invoke(),
                serializationService,
                clientMessage -> {
                    MCUpdateMapConfigRequestCodec.decodeResponse(clientMessage);
                    return null;
                }
        );
    }
}
