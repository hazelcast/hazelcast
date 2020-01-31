/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.impl.protocol.codec.MCAddWanBatchPublisherConfigCodec;
import com.hazelcast.client.impl.protocol.codec.MCApplyMCConfigCodec;
import com.hazelcast.client.impl.protocol.codec.MCChangeClusterStateCodec;
import com.hazelcast.client.impl.protocol.codec.MCChangeClusterVersionCodec;
import com.hazelcast.client.impl.protocol.codec.MCChangeWanReplicationStateCodec;
import com.hazelcast.client.impl.protocol.codec.MCCheckWanConsistencyCodec;
import com.hazelcast.client.impl.protocol.codec.MCClearWanQueuesCodec;
import com.hazelcast.client.impl.protocol.codec.MCGetCPMembersCodec;
import com.hazelcast.client.impl.protocol.codec.MCGetClusterMetadataCodec;
import com.hazelcast.client.impl.protocol.codec.MCGetMapConfigCodec;
import com.hazelcast.client.impl.protocol.codec.MCGetMemberConfigCodec;
import com.hazelcast.client.impl.protocol.codec.MCGetSystemPropertiesCodec;
import com.hazelcast.client.impl.protocol.codec.MCGetThreadDumpCodec;
import com.hazelcast.client.impl.protocol.codec.MCGetTimedMemberStateCodec;
import com.hazelcast.client.impl.protocol.codec.MCInterruptHotRestartBackupCodec;
import com.hazelcast.client.impl.protocol.codec.MCMatchMCConfigCodec;
import com.hazelcast.client.impl.protocol.codec.MCPollMCEventsCodec;
import com.hazelcast.client.impl.protocol.codec.MCPromoteLiteMemberCodec;
import com.hazelcast.client.impl.protocol.codec.MCPromoteToCPMemberCodec;
import com.hazelcast.client.impl.protocol.codec.MCReadMetricsCodec;
import com.hazelcast.client.impl.protocol.codec.MCRemoveCPMemberCodec;
import com.hazelcast.client.impl.protocol.codec.MCResetCPSubsystemCodec;
import com.hazelcast.client.impl.protocol.codec.MCRunConsoleCommandCodec;
import com.hazelcast.client.impl.protocol.codec.MCRunGcCodec;
import com.hazelcast.client.impl.protocol.codec.MCRunScriptCodec;
import com.hazelcast.client.impl.protocol.codec.MCShutdownClusterCodec;
import com.hazelcast.client.impl.protocol.codec.MCShutdownMemberCodec;
import com.hazelcast.client.impl.protocol.codec.MCTriggerForceStartCodec;
import com.hazelcast.client.impl.protocol.codec.MCTriggerHotRestartBackupCodec;
import com.hazelcast.client.impl.protocol.codec.MCTriggerPartialStartCodec;
import com.hazelcast.client.impl.protocol.codec.MCUpdateMapConfigCodec;
import com.hazelcast.client.impl.protocol.codec.MCWanSyncMapCodec;
import com.hazelcast.client.impl.spi.impl.ClientInvocation;
import com.hazelcast.cluster.ClusterState;
import com.hazelcast.cluster.Member;
import com.hazelcast.cp.CPSubsystemManagementService;
import com.hazelcast.internal.management.TimedMemberState;
import com.hazelcast.internal.management.dto.CPMemberDTO;
import com.hazelcast.internal.management.dto.ClientBwListDTO;
import com.hazelcast.internal.management.dto.MCEventDTO;
import com.hazelcast.internal.metrics.managementcenter.MetricsResultSet;
import com.hazelcast.internal.nio.ConnectionType;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.util.MapUtil;
import com.hazelcast.spi.properties.HazelcastProperty;
import com.hazelcast.version.Version;
import com.hazelcast.wan.WanPublisherState;
import com.hazelcast.wan.impl.AddWanConfigResult;
import com.hazelcast.wan.impl.WanSyncType;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.wan.impl.WanSyncType.ALL_MAPS;
import static com.hazelcast.wan.impl.WanSyncType.SINGLE_MAP;

/**
 * Only works for smart clients, i.e. doesn't work for unisocket clients.
 */
@SuppressWarnings({"checkstyle:methodcount"})
public class ManagementCenterService {

    /**
     * Internal property for enabling MC client mode ({@link ConnectionType#MC_JAVA_CLIENT}).
     */
    public static final HazelcastProperty MC_CLIENT_MODE_PROP
            = new HazelcastProperty("hazelcast.client.internal.mc.mode", false);

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
        ClientInvocation invocation = new ClientInvocation(client, request, null, member.getUuid());

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
                    member.getUuid()
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
                member.getUuid()
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
                member.getUuid()
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
                member.getUuid()
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
                member.getUuid()
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
                member.getUuid()
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
                member.getUuid()
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
     * @param member {@link Member} to get system properties of
     */
    @Nonnull
    public CompletableFuture<Map<String, String>> getSystemProperties(Member member) {
        checkNotNull(member);

        ClientInvocation invocation = new ClientInvocation(
                client,
                MCGetSystemPropertiesCodec.encodeRequest(),
                null,
                member.getUuid()
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
     *
     * @param member {@link Member} to get {@link TimedMemberState} of
     */
    @Nonnull
    public CompletableFuture<Optional<String>> getTimedMemberState(Member member) {
        checkNotNull(member);

        ClientInvocation invocation = new ClientInvocation(
                client,
                MCGetTimedMemberStateCodec.encodeRequest(),
                null,
                member.getUuid()
        );

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
     * @param member target member
     * @param eTag   ETag value of MC config to match with (should be the latest value from MC)
     * @return operation future object with match result: <code>true</code> if config ETags match
     */
    @Nonnull
    public CompletableFuture<Boolean> matchMCConfig(Member member, String eTag) {
        checkNotNull(member);
        checkNotNull(eTag);

        ClientInvocation invocation = new ClientInvocation(
                client,
                MCMatchMCConfigCodec.encodeRequest(eTag),
                null,
                member.getUuid()
        );

        return new ClientDelegatingFuture<>(
                invocation.invoke(),
                serializationService,
                clientMessage -> {
                    MCMatchMCConfigCodec.ResponseParameters response =
                            MCMatchMCConfigCodec.decodeResponse(clientMessage);
                    return response.result;
                },
                false
        );
    }

    /**
     * Applies the MC config (client B/W list) on a given member.
     *
     * @param member       target member
     * @param eTag         ETag of the new config
     * @param clientBwList new config for client B/W list filtering
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
                member.getUuid()
        );

        return new ClientDelegatingFuture<>(
                invocation.invoke(),
                serializationService,
                clientMessage -> null
        );
    }

    /**
     * Gets the current metadata of the cluster.
     *
     * @param member {@link Member} to get current cluster metadata of
     */
    @Nonnull
    public CompletableFuture<MCClusterMetadata> getClusterMetadata(Member member) {
        checkNotNull(member);

        ClientInvocation invocation = new ClientInvocation(
                client,
                MCGetClusterMetadataCodec.encodeRequest(),
                null,
                member.getUuid()
        );

        return new ClientDelegatingFuture<>(
                invocation.invoke(),
                serializationService,
                clientMessage -> {
                    MCGetClusterMetadataCodec.ResponseParameters response =
                            MCGetClusterMetadataCodec.decodeResponse(clientMessage);
                    return MCClusterMetadata.fromResponse(response);
                }
        );
    }

    /**
     * Shuts down the cluster.
     */
    public void shutdownCluster() {
        ClientInvocation invocation = new ClientInvocation(
                client,
                MCShutdownClusterCodec.encodeRequest(),
                null
        );
        invocation.invoke();
    }

    /**
     * Changes the cluster version
     *
     * @param version new cluster version
     */
    @Nonnull
    public CompletableFuture<Void> changeClusterVersion(Version version) {
        ClientInvocation invocation = new ClientInvocation(
                client,
                MCChangeClusterVersionCodec.encodeRequest(version.getMajor(), version.getMinor()),
                null
        );

        return new ClientDelegatingFuture<>(
                invocation.invoke(),
                serializationService,
                clientMessage -> null
        );
    }

    /**
     * Runs the script on a given member.
     *
     * @param member target member
     * @param engine the name of script engine which will be used for the execution
     * @param script the script to execute
     * @return operation future object with script execution output
     */
    @Nonnull
    public CompletableFuture<String> runScript(Member member, String engine, String script) {
        checkNotNull(member);
        checkNotNull(script);
        checkNotNull(engine);

        ClientInvocation invocation = new ClientInvocation(
                client,
                MCRunScriptCodec.encodeRequest(engine, script),
                null,
                member.getUuid()
        );

        return new ClientDelegatingFuture<>(
                invocation.invoke(),
                serializationService,
                clientMessage -> MCRunScriptCodec.decodeResponse(clientMessage).result
        );
    }

    /**
     * Runs the console command on a given member.
     *
     * @param member    target member
     * @param namespace namespace to be set before the command is executed (optional)
     * @param command   the command to execute
     * @return operation future object with command execution output
     */
    @Nonnull
    public CompletableFuture<String> runConsoleCommand(Member member, String namespace, String command) {
        checkNotNull(member);
        checkNotNull(command);

        ClientInvocation invocation = new ClientInvocation(
                client,
                MCRunConsoleCommandCodec.encodeRequest(namespace, command),
                null,
                member.getUuid()
        );

        return new ClientDelegatingFuture<>(
                invocation.invoke(),
                serializationService,
                clientMessage -> MCRunConsoleCommandCodec.decodeResponse(clientMessage).result
        );
    }

    /**
     * Stop, pause or resume WAN replication for the given {@code wanReplicationName} and
     * {@code wanPublisherId} on the given {@link Member}.
     *
     * @param member             {@link Member} to change WAN replication state on
     * @param wanReplicationName name of the WAN replication to change state of
     * @param wanPublisherId     ID of the WAN publisher to change state of
     * @param newState           new state for the WAN publisher
     */
    @SuppressWarnings("unused")
    @Nonnull
    public CompletableFuture<Void> changeWanReplicationState(Member member,
                                                             String wanReplicationName,
                                                             String wanPublisherId,
                                                             WanPublisherState newState) {
        checkNotNull(member);
        checkNotNull(wanReplicationName);
        checkNotNull(wanPublisherId);
        checkNotNull(newState);

        ClientInvocation invocation = new ClientInvocation(
                client,
                MCChangeWanReplicationStateCodec.encodeRequest(wanReplicationName, wanPublisherId, newState.getId()),
                null,
                member.getUuid()
        );

        return new ClientDelegatingFuture<>(
                invocation.invoke(),
                serializationService,
                clientMessage -> null
        );
    }

    /**
     * Clear WAN replication queues for the given {@code wanReplicationName} and
     * {@code wanPublisherId} on the given {@link Member}.
     *
     * @param member             {@link Member} to clear WAN replication queues on
     * @param wanReplicationName name of the WAN replication to clear queues of
     * @param wanPublisherId     ID of the WAN publisher to clear queues of
     */
    @SuppressWarnings("unused")
    @Nonnull
    public CompletableFuture<Void> clearWanQueues(Member member,
                                                  String wanReplicationName,
                                                  String wanPublisherId) {
        checkNotNull(member);
        checkNotNull(wanReplicationName);
        checkNotNull(wanPublisherId);

        ClientInvocation invocation = new ClientInvocation(
                client,
                MCClearWanQueuesCodec.encodeRequest(wanReplicationName, wanPublisherId),
                null,
                member.getUuid()
        );

        return new ClientDelegatingFuture<>(
                invocation.invoke(),
                serializationService,
                clientMessage -> null
        );
    }

    /**
     * Add a new WAN replication configuration.
     *
     * @param config the new WAN replication configuration
     * @return a {@link CompletableFuture} that holds the IDs for the WAN publishers which were
     * added to the configuration, and the ones that are ignored/not added to the configuration
     */
    @SuppressWarnings("unused")
    @Nonnull
    public CompletableFuture<AddWanConfigResult> addWanReplicationConfig(MCWanBatchPublisherConfig config) {
        checkNotNull(config);

        ClientInvocation invocation = new ClientInvocation(
                client,
                MCAddWanBatchPublisherConfigCodec.encodeRequest(
                        config.getName(),
                        config.getTargetCluster(),
                        config.getPublisherId(),
                        config.getEndpoints(),
                        config.getQueueCapacity(),
                        config.getBatchSize(),
                        config.getBatchMaxDelayMillis(),
                        config.getResponseTimeoutMillis(),
                        config.getAckType().getId(),
                        config.getQueueFullBehaviour().getId()
                ),
                null
        );

        return new ClientDelegatingFuture<>(
                invocation.invoke(),
                serializationService,
                clientMessage -> {
                    MCAddWanBatchPublisherConfigCodec.ResponseParameters response =
                            MCAddWanBatchPublisherConfigCodec.decodeResponse(clientMessage);
                    return new AddWanConfigResult(response.addedPublisherIds, response.ignoredPublisherIds);
                }
        );
    }

    /**
     * Initiate WAN sync for a specific map.
     *
     * @param wanReplicationName name of the WAN replication to initiate WAN sync for
     * @param wanPublisherId     ID of the WAN publisher to initiate WAN sync for
     * @param mapName            name of the map to trigger WAN sync on
     * @return a {@link CompletableFuture} that holds the UUID of the synchronization
     */
    @SuppressWarnings("unused")
    @Nonnull
    public CompletableFuture<UUID> wanSyncMap(String wanReplicationName,
                                              String wanPublisherId,
                                              String mapName) {
        checkNotNull(wanReplicationName);
        checkNotNull(wanPublisherId);
        checkNotNull(mapName);

        return wanSyncMap(wanReplicationName, wanPublisherId, SINGLE_MAP, mapName);
    }

    /**
     * Initiate WAN sync for all maps.
     *
     * @param wanReplicationName name of the WAN replication to initiate WAN sync for
     * @param wanPublisherId     ID of the WAN publisher to initiate WAN sync for
     * @return a {@link CompletableFuture} that holds the UUID of the synchronization
     */
    @SuppressWarnings("unused")
    @Nonnull
    public CompletableFuture<UUID> wanSyncAllMaps(String wanReplicationName,
                                                  String wanPublisherId) {
        checkNotNull(wanReplicationName);
        checkNotNull(wanPublisherId);

        return wanSyncMap(wanReplicationName, wanPublisherId, ALL_MAPS, null);
    }


    private CompletableFuture<UUID> wanSyncMap(String wanReplicationName,
                                               String wanPublisherId,
                                               WanSyncType syncType,
                                               String map) {

        ClientInvocation invocation = new ClientInvocation(
                client,
                MCWanSyncMapCodec.encodeRequest(
                        wanReplicationName, wanPublisherId, syncType.getType(), map),
                null
        );

        return new ClientDelegatingFuture<>(
                invocation.invoke(),
                serializationService,
                clientMessage -> MCWanSyncMapCodec.decodeResponse(clientMessage).uuid
        );
    }

    /**
     * Initiate WAN consistency check for a specific map.
     *
     * @param wanReplicationName name of the WAN replication to check WAN consistency for
     * @param wanPublisherId     ID of the WAN publisher to check WAN consistency for
     * @param mapName            name of the map to check WAN consistency for
     * @return a {@link CompletableFuture} that holds the UUID of the WAN consistency check
     */
    @SuppressWarnings("unused")
    @Nonnull
    public CompletableFuture<UUID> checkWanConsistency(String wanReplicationName,
                                                       String wanPublisherId,
                                                       String mapName) {
        checkNotNull(wanReplicationName);
        checkNotNull(wanPublisherId);
        checkNotNull(mapName);

        ClientInvocation invocation = new ClientInvocation(
                client,
                MCCheckWanConsistencyCodec.encodeRequest(wanReplicationName, wanPublisherId, mapName),
                null
        );

        return new ClientDelegatingFuture<>(
                invocation.invoke(),
                serializationService,
                clientMessage -> MCCheckWanConsistencyCodec.decodeResponse(clientMessage).uuid
        );
    }

    /**
     * Polls pending events from the member it's called on.
     *
     * @param member target member
     */
    @Nonnull
    public CompletableFuture<List<MCEventDTO>> pollMCEvents(Member member) {
        checkNotNull(member);

        ClientInvocation invocation = new ClientInvocation(
                client,
                MCPollMCEventsCodec.encodeRequest(),
                null,
                member.getUuid()
        );

        return new ClientDelegatingFuture<>(
                invocation.invoke(),
                serializationService,
                clientMessage -> MCPollMCEventsCodec.decodeResponse(clientMessage).events
        );
    }

    /**
     * Returns the current list of CP members.
     *
     * @return list of CP members
     *
     * @see CPSubsystemManagementService#getCPMembers()
     */
    @Nonnull
    public CompletableFuture<List<CPMemberDTO>> getCPMembers() {
        ClientInvocation invocation = new ClientInvocation(
                client,
                MCGetCPMembersCodec.encodeRequest(),
                null
        );

        return new ClientDelegatingFuture<>(
                invocation.invoke(),
                serializationService,
                clientMessage -> MCGetCPMembersCodec.decodeResponse(clientMessage).cpMembers
                        .stream()
                        .map(e -> new CPMemberDTO(e.getKey(), e.getValue()))
                        .collect(Collectors.toList())
        );
    }

    /**
     * Promotes the given member to the CP role.
     *
     * @param member member to be promoted
     *
     * @see CPSubsystemManagementService#promoteToCPMember()
     */
    @Nonnull
    public CompletableFuture<Void> promoteToCPMember(Member member) {
        checkNotNull(member);

        ClientInvocation invocation = new ClientInvocation(
                client,
                MCPromoteToCPMemberCodec.encodeRequest(),
                null,
                member.getUuid()
        );

        return new ClientDelegatingFuture<>(
                invocation.invoke(),
                serializationService,
                clientMessage -> null
        );
    }

    /**
     * Removes the given unreachable CP member from the active CP members list
     * and all CP groups it belongs to.
     *
     * @param cpMemberUuid UUID of unreachable member
     *
     * @see CPSubsystemManagementService#removeCPMember(UUID)
     */
    @Nonnull
    public CompletableFuture<Void> removeCPMember(UUID cpMemberUuid) {
        checkNotNull(cpMemberUuid);

        ClientInvocation invocation = new ClientInvocation(
                client,
                MCRemoveCPMemberCodec.encodeRequest(cpMemberUuid),
                null
        );

        return new ClientDelegatingFuture<>(
                invocation.invoke(),
                serializationService,
                clientMessage -> null
        );
    }

    /**
     * Wipes and resets the whole CP Subsystem state and initializes it
     * as if the Hazelcast cluster is starting up initially.
     *
     * @see CPSubsystemManagementService#reset()
     */
    @Nonnull
    public CompletableFuture<Void> resetCPSubsystem() {
        // this operation must be executed on master
        Member masterAddress = client.getClientClusterService().getMasterMember();
        if (masterAddress == null) {
            throw new IllegalStateException("Master member is not known yet.");
        }
        ClientInvocation invocation = new ClientInvocation(
                client,
                MCResetCPSubsystemCodec.encodeRequest(),
                null,
                masterAddress.getUuid()
        );

        return new ClientDelegatingFuture<>(
                invocation.invoke(),
                serializationService,
                clientMessage -> null
        );
    }

    /**
     * Triggers a partial restart process
     *
     * @return  a {@link CompletableFuture} that returns true if the partial restart
     *          was successfully initiated, false otherwise
     */
    @SuppressWarnings("unused")
    @Nonnull
    public CompletableFuture<Boolean> triggerPartialStart() {
        ClientInvocation invocation = new ClientInvocation(
                client,
                MCTriggerPartialStartCodec.encodeRequest(),
                null
        );

        return new ClientDelegatingFuture<>(
                invocation.invoke(),
                serializationService,
                clientMessage -> MCTriggerPartialStartCodec.decodeResponse(clientMessage).result
        );
    }

    /**
     * Forces the cluster to start
     *
     * @return  a {@link CompletableFuture} that returns true if the forced start
     *          was successfully initiated, false otherwise
     */
    @SuppressWarnings("unused")
    @Nonnull
    public CompletableFuture<Boolean> triggerForceStart() {
        ClientInvocation invocation = new ClientInvocation(
                client,
                MCTriggerForceStartCodec.encodeRequest(),
                null
        );

        return new ClientDelegatingFuture<>(invocation.invoke(),
                serializationService,
                clientMessage -> MCTriggerForceStartCodec.decodeResponse(clientMessage).result
        );
    }

    /**
     * Triggers the hot restart backup process
     *
     * @return an empty {@code CompletableFuture} after the hot restart process started
     */
    @SuppressWarnings("unused")
    @Nonnull
    public CompletableFuture<Void> triggerHotRestartBackup() {
        ClientInvocation invocation = new ClientInvocation(
                client,
                MCTriggerHotRestartBackupCodec.encodeRequest(),
                null
        );

        return new ClientDelegatingFuture<>(invocation.invoke(),
                serializationService,
                clientMessage -> null
        );
    }

    /**
     * Interrupts the hot restart backup process
     *
     * @return an empty {@code CompletableFuture} after the hot restart process got interrupted
     */
    @SuppressWarnings("unused")
    @Nonnull
    public CompletableFuture<Void> interruptHotRestartBackup() {
        ClientInvocation invocation = new ClientInvocation(
                client,
                MCInterruptHotRestartBackupCodec.encodeRequest(),
                null
        );

        return new ClientDelegatingFuture<>(invocation.invoke(),
                serializationService,
                clientMessage -> null
        );
    }
}
