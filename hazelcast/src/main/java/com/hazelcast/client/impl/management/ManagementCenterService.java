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
import com.hazelcast.client.impl.clientside.ClientMessageDecoder;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.MCChangeClusterStateCodec;
import com.hazelcast.client.impl.protocol.codec.MCGetMapConfigCodec;
import com.hazelcast.client.impl.protocol.codec.MCReadMetricsCodec;
import com.hazelcast.client.impl.protocol.codec.MCUpdateMapConfigCodec;
import com.hazelcast.client.impl.protocol.codec.holder.MapConfigHolder;
import com.hazelcast.client.impl.spi.impl.ClientInvocation;
import com.hazelcast.cluster.ClusterState;
import com.hazelcast.cluster.Member;
import com.hazelcast.internal.metrics.managementcenter.MetricsResultSet;
import com.hazelcast.internal.serialization.InternalSerializationService;

import java.util.concurrent.CompletableFuture;

import javax.annotation.Nonnull;

public class ManagementCenterService {

    private final HazelcastClientInstanceImpl client;
    private final InternalSerializationService serializationService;

    public ManagementCenterService(HazelcastClientInstanceImpl client,
                                   InternalSerializationService serializationService) {

        this.client = client;
        this.serializationService = serializationService;
    }

    /**
     * Reads the metrics journal for a given number starting from a specific sequence.
     */
    @Nonnull
    public CompletableFuture<MetricsResultSet> readMetricsAsync(Member member, long startSequence) {
        ClientMessage request = MCReadMetricsCodec.encodeRequest(member.getUuid(), startSequence);
        ClientInvocation invocation = new ClientInvocation(client, request, null, member.getAddress());

        ClientMessageDecoder<MetricsResultSet> decoder = (clientMessage) -> {
            MCReadMetricsCodec.ResponseParameters response = MCReadMetricsCodec.decodeResponse(clientMessage);
            return new MetricsResultSet(response.nextSequence, response.elements);
        };

        return new ClientDelegatingFuture<>(invocation.invoke(), serializationService, decoder, false);
    }

    /**
     * Changes the cluster's state.
     */
    @Nonnull
    public CompletableFuture<Void> changeClusterState(ClusterState newState) {
        ClientInvocation invocation = new ClientInvocation(
                client,
                MCChangeClusterStateCodec.encodeRequest(newState.ordinal()),
                null
        );
        return new ClientDelegatingFuture<>(
                invocation.invoke(),
                serializationService,
                clientMessage -> {
                    MCChangeClusterStateCodec.decodeResponse(clientMessage);
                    return null;
                }
        );
    }

    /**
     * Gets the config ƒor a given map.
     */
    @Nonnull
    public CompletableFuture<MapConfigHolder> getMapConfig(String map) {
        ClientInvocation invocation = new ClientInvocation(
                client,
                MCGetMapConfigCodec.encodeRequest(map),
                map
        );
        return new ClientDelegatingFuture<>(
                invocation.invoke(),
                serializationService,
                clientMessage -> MCGetMapConfigCodec.decodeResponse(clientMessage).response,
                true
        );
    }

    /**
     * Updates the config of a given map.
     */
    @Nonnull
    public CompletableFuture<Void> updateMapConfig(String map, MapConfigHolder newMapConfig) {
        ClientInvocation invocation = new ClientInvocation(
                client,
                MCUpdateMapConfigCodec.encodeRequest(map, newMapConfig),
                null
        );
        return new ClientDelegatingFuture<>(
                invocation.invoke(),
                serializationService,
                clientMessage -> {
                    MCUpdateMapConfigCodec.decodeResponse(clientMessage);
                    return null;
                }
        );
    }
}
