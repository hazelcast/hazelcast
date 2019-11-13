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
import com.hazelcast.client.impl.protocol.codec.MCApplyMCConfigCodec;
import com.hazelcast.client.impl.protocol.codec.MCChangeClusterStateCodec;
import com.hazelcast.client.impl.protocol.codec.MCGetMapConfigCodec;
import com.hazelcast.client.impl.protocol.codec.MCGetMemberConfigCodec;
import com.hazelcast.client.impl.protocol.codec.MCGetSystemPropertiesCodec;
import com.hazelcast.client.impl.protocol.codec.MCGetThreadDumpCodec;
import com.hazelcast.client.impl.protocol.codec.MCGetTimedMemberStateCodec;
import com.hazelcast.client.impl.protocol.codec.MCMatchMCConfigCodec;
import com.hazelcast.client.impl.protocol.codec.MCPromoteLiteMemberCodec;
import com.hazelcast.client.impl.protocol.codec.MCReadMetricsCodec;
import com.hazelcast.client.impl.protocol.codec.MCRunGcCodec;
import com.hazelcast.client.impl.protocol.codec.MCShutdownMemberCodec;
import com.hazelcast.client.impl.protocol.codec.MCUpdateMapConfigCodec;
import com.hazelcast.client.impl.spi.impl.ClientInvocation;
import com.hazelcast.cluster.ClusterState;
import com.hazelcast.cluster.Member;
import com.hazelcast.internal.management.TimedMemberState;
import com.hazelcast.internal.management.dto.ClientBwListDTO;
import com.hazelcast.internal.metrics.managementcenter.MetricsResultSet;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.util.MapUtil;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * Only works for smart clients, i.e. doesn't work for unisocket clients.
 */
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
        checkNotNull(newState);

        ClientInvocation invocation = new ClientInvocation(
                client,
                MCChangeClusterStateCodec.encodeRequest(newState.getId()),
                null
        );
        return new ClientDelegatingFuture<>(
                invocation.invoke(),
                serializationService,
                clientMessage -> null
        );
    }

    /**
     * Gets the config ƒor a given map from a random member.
     */
    @Nonnull
    public CompletableFuture<MCMapConfig> getMapConfig(String map) {
        checkNotNull(map);

        return doGetMapConfig(null, map);
    }

    /**
     * Gets the config ƒor a given map from a specific member.
     */
    @Nonnull
    public CompletableFuture<MCMapConfig> getMapConfig(Member member, String map) {
        checkNotNull(member);
        checkNotNull(map);

        return doGetMapConfig(member, map);
    }

    private CompletableFuture<MCMapConfig> doGetMapConfig(Member member, String map) {
        ClientInvocation invocation;
        if (member == null) {
            invocation = new ClientInvocation(
                    client,
                    MCGetMapConfigCodec.encodeRequest(map),
                    map
            );
        } else {
            invocation = new ClientInvocation(
                    client,
                    MCGetMapConfigCodec.encodeRequest(map),
                    map,
                    member.getAddress()
            );
        }

        return new ClientDelegatingFuture<>(
                invocation.invoke(),
                serializationService,
                clientMessage -> {
                    MCGetMapConfigCodec.ResponseParameters response =
                            MCGetMapConfigCodec.decodeResponse(clientMessage);
                    return MCMapConfig.fromResponse(response);
                },
                true
        );
    }

    /**
     * Updates the config of a given map on a given member.
     */
    @Nonnull
    public CompletableFuture<Void> updateMapConfig(Member member,
                                                   UpdateMapConfigParameters parameters) {
        checkNotNull(member);
        checkNotNull(parameters);
        checkNotNull(parameters.getEvictionPolicy());
        checkNotNull(parameters.getMaxSizePolicy());

        ClientInvocation invocation = new ClientInvocation(
                client,
                MCUpdateMapConfigCodec.encodeRequest(
                        parameters.getMap(),
                        parameters.getTimeToLiveSeconds(),
                        parameters.getMaxIdleSeconds(),
                        parameters.getEvictionPolicy().getId(),
                        parameters.isReadBackupData(),
                        parameters.getMaxSize(),
                        parameters.getMaxSizePolicy().getId()),
                parameters.getMap(),
                member.getAddress()
        );
        return new ClientDelegatingFuture<>(
                invocation.invoke(),
                serializationService,
                clientMessage -> null
        );
    }

    /**
     * Gets the config of a given member rendered as XML.
     */
    @Nonnull
    public CompletableFuture<String> getMemberConfig(Member member) {
        checkNotNull(member);

        ClientInvocation invocation = new ClientInvocation(
                client,
                MCGetMemberConfigCodec.encodeRequest(),
                null,
                member.getAddress()
        );
        return new ClientDelegatingFuture<>(
                invocation.invoke(),
                serializationService,
                clientMessage -> MCGetMemberConfigCodec.decodeResponse(clientMessage).configXml
        );
    }

    /**
     * Runs GC on a given member.
     */
    @Nonnull
    public CompletableFuture<Void> runGc(Member member) {
        checkNotNull(member);

        ClientInvocation invocation = new ClientInvocation(
                client,
                MCRunGcCodec.encodeRequest(),
                null,
                member.getAddress()
        );
        return new ClientDelegatingFuture<>(
                invocation.invoke(),
                serializationService,
                clientMessage -> null
        );
    }

    /**
     * Gets thread dump of a given member.
     *
     * @param member        {@link Member} to get the thread dump of
     * @param dumpDeadLocks whether only dead-locked threads or all threads should be dumped.
     */
    @Nonnull
    public CompletableFuture<String> getThreadDump(Member member, boolean dumpDeadLocks) {
        checkNotNull(member);

        ClientInvocation invocation = new ClientInvocation(
                client,
                MCGetThreadDumpCodec.encodeRequest(dumpDeadLocks),
                null,
                member.getAddress()
        );
        return new ClientDelegatingFuture<>(
                invocation.invoke(),
                serializationService,
                clientMessage -> MCGetThreadDumpCodec.decodeResponse(clientMessage).threadDump
        );
    }

    /**
     * Shuts down a given member.
     *
     * @param member {@link Member} to shut down
     */
    public void shutdownMember(Member member) {
        checkNotNull(member);

        ClientInvocation invocation = new ClientInvocation(
                client,
                MCShutdownMemberCodec.encodeRequest(),
                null,
                member.getAddress()
        );
        invocation.invoke();
    }

    /**
     * Promotes a lite member to a data member.
     *
     * @param member {@link Member} to promote
     */
    @Nonnull
    public CompletableFuture<Void> promoteLiteMember(Member member) {
        checkNotNull(member);

        ClientInvocation invocation = new ClientInvocation(
                client,
                MCPromoteLiteMemberCodec.encodeRequest(),
                null,
                member.getAddress()
        );

        return new ClientDelegatingFuture<>(
                invocation.invoke(),
                serializationService,
                clientMessage -> null
        );
    }

    /**
     * Gets system properties of a given member.
     *
     * @param member {@link Member} to get system properties of.
     */
    @Nonnull
    public CompletableFuture<Map<String, String>> getSystemProperties(Member member) {
        checkNotNull(member);

        ClientInvocation invocation = new ClientInvocation(
                client,
                MCGetSystemPropertiesCodec.encodeRequest(),
                null,
                member.getAddress()
        );

        return new ClientDelegatingFuture<>(
                invocation.invoke(),
                serializationService,
                clientMessage -> {
                    List<Entry<String, String>> systemProperties
                            = MCGetSystemPropertiesCodec.decodeResponse(clientMessage).systemProperties;

                    Map<String, String> result = MapUtil.createHashMap(systemProperties.size());
                    for (Entry<String, String> property : systemProperties) {
                        result.put(property.getKey(), property.getValue());
                    }
                    return result;
                }
        );
    }

    /**
     * Gets the latest {@link TimedMemberState} of the member it's called on.
     */
    @Nonnull
    public CompletableFuture<Optional<String>> getTimedMemberState(Member member) {
        checkNotNull(member);

        ClientInvocation invocation = new ClientInvocation(
                client,
                MCGetTimedMemberStateCodec.encodeRequest(),
                null,
                member.getAddress());

        return new ClientDelegatingFuture<>(
                invocation.invoke(),
                serializationService,
                clientMessage -> Optional.ofNullable(
                        MCGetTimedMemberStateCodec.decodeResponse(clientMessage).timedMemberStateJson)
        );
    }

    /**
     * Checks if local MC config (client B/W list) on a given member has the same ETag as provided.
     *
     * @param member    target member
     * @param eTag      ETag value of MC config to match with (should be the latest value from MC)
     * @return          operation future object with match result: <code>true</code> if config ETags match
     */
    @Nonnull
    public CompletableFuture<Boolean> matchMCConfig(Member member, String eTag) {
        checkNotNull(member);
        checkNotNull(eTag);

        ClientInvocation invocation = new ClientInvocation(
                client,
                MCMatchMCConfigCodec.encodeRequest(eTag),
                null,
                member.getAddress()
        );
        return new ClientDelegatingFuture<>(
                invocation.invoke(),
                serializationService,
                clientMessage -> {
                    MCMatchMCConfigCodec.ResponseParameters response =
                            MCMatchMCConfigCodec.decodeResponse(clientMessage);
                    return response.response;
                },
                false
        );
    }

    /**
     * Applies the MC config (client B/W list) on a given member.
     *
     * @param member        target member
     * @param eTag          ETag of the new config
     * @param clientBwList  new config for client B/W list filtering
     * @return              operation future object
     */
    @Nonnull
    public CompletableFuture<Void> applyMCConfig(Member member, String eTag, ClientBwListDTO clientBwList) {
        checkNotNull(member);
        checkNotNull(eTag);
        checkNotNull(clientBwList);
        checkNotNull(clientBwList.mode);
        checkNotNull(clientBwList.entries);

        ClientInvocation invocation = new ClientInvocation(
                client,
                MCApplyMCConfigCodec.encodeRequest(eTag, clientBwList.mode.getId(), clientBwList.entries),
                null,
                member.getAddress()
        );
        return new ClientDelegatingFuture<>(
                invocation.invoke(),
                serializationService,
                clientMessage -> null
        );
    }
}
