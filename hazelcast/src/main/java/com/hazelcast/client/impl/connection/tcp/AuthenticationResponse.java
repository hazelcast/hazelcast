/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.connection.tcp;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ClientAuthenticationCodec;
import com.hazelcast.client.impl.protocol.codec.ClientAuthenticationCustomCodec;
import com.hazelcast.cluster.Address;
import com.hazelcast.internal.cluster.MemberInfo;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

/**
 * Represents the combined authentication response parameters
 * of the various authentication response messages.
 * <p/>
 * Any option on the AuthenticationResponse is optional and hence each
 * client needs to deal with the fact that the value might not exist.
 */
public final class AuthenticationResponse {
    private final byte status;
    private final Address address;
    private final UUID memberUuid;
    private final byte serializationVersion;
    private final String serverHazelcastVersion;
    private final int partitionCount;
    private final UUID clusterId;
    private final boolean failoverSupported;
    private final List<Integer> tpcPorts;
    private final byte[] tpcToken;

    /**
     * True if the {@link #memberListVersion} is received from the member, false otherwise.
     * If this is false, {@link #memberListVersion} has the default value for its type.
     */
    private final boolean isMemberListVersionExists;

    /**
     * True if the {@link #partitionListVersion} is received from the member, false otherwise.
     * If this is false, {@link #partitionListVersion} has the default value for its type.
     */
    private final boolean isPartitionListVersionExists;

    /**
     * True if the {@link #keyValuePairs} is received from the member, false otherwise.
     * If this is false, {@link #keyValuePairs} has the default value for its type.
     */
    private final boolean isKeyValuePairsExists;

    private final int memberListVersion;
    private final List<MemberInfo> memberInfos;
    private final int partitionListVersion;
    private final List<Entry<UUID, List<Integer>>> partitions;
    private final Map<String, String> keyValuePairs;

    @SuppressWarnings("checkstyle:parameternumber")
    private AuthenticationResponse(byte status,
                                   Address address,
                                   UUID memberUuid,
                                   byte serializationVersion,
                                   String serverHazelcastVersion,
                                   int partitionCount,
                                   UUID clusterId,
                                   boolean failoverSupported,
                                   List<Integer> tpcPorts,
                                   byte[] tpcToken,
                                   boolean isMemberListVersionExists,
                                   int memberListVersion,
                                   List<MemberInfo> memberInfos,
                                   boolean isPartitionListVersionExists,
                                   int partitionListVersion,
                                   List<Entry<UUID, List<Integer>>> partitions,
                                   boolean isKeyValuePairsExists,
                                   Map<String, String> keyValuePairs) {
        this.status = status;
        this.address = address;
        this.memberUuid = memberUuid;
        this.serializationVersion = serializationVersion;
        this.serverHazelcastVersion = serverHazelcastVersion;
        this.partitionCount = partitionCount;
        this.clusterId = clusterId;
        this.failoverSupported = failoverSupported;
        this.tpcPorts = tpcPorts;
        this.tpcToken = tpcToken;
        this.isMemberListVersionExists = isMemberListVersionExists;
        this.memberListVersion = memberListVersion;
        this.memberInfos = memberInfos;
        this.isPartitionListVersionExists = isPartitionListVersionExists;
        this.partitionListVersion = partitionListVersion;
        this.partitions = partitions;
        this.isKeyValuePairsExists = isKeyValuePairsExists;
        this.keyValuePairs = keyValuePairs;
    }

    /**
     * Returns a byte that represents the authentication status. It can be:
     * <ul>
     *     <li>AUTHENTICATED(0)</li>
     *     <li>CREDENTIALS_FAILED(1)</li>
     *     <li>SERIALIZATION_VERSION_MISMATCH(2)</li>
     *     <li>NOT_ALLOWED_IN_CLUSTER(3)</li>
     * </ul>
     */
    public byte getStatus() {
        return status;
    }

    /**
     * Returns the address of the Hazelcast member which sends the
     * authentication response.
     */
    @Nullable
    public Address getAddress() {
        return address;
    }

    /**
     * Returns the UUID of the Hazelcast member which sends the
     * authentication response.
     */
    @Nullable
    public UUID getMemberUuid() {
        return memberUuid;
    }

    /**
     * Returns the server-side supported serialization version to
     * inform client-side.
     */
    public byte getSerializationVersion() {
        return serializationVersion;
    }

    /**
     * Returns the version of the Hazelcast member which sends the
     * authentication response.
     */
    public String getServerHazelcastVersion() {
        return serverHazelcastVersion;
    }

    /**
     * Returns the partition count of the cluster.
     */
    public int getPartitionCount() {
        return partitionCount;
    }

    /**
     * Returns the UUID of the cluster that the client is authenticated.
     */
    public UUID getClusterId() {
        return clusterId;
    }

    /**
     * Returns {@code true} if server supports clients with failover feature.
     */
    public boolean isFailoverSupported() {
        return failoverSupported;
    }

    /**
     * Returns the list of TPC ports or {@code null} if TPC is disabled.
     */
    @Nullable
    public List<Integer> getTpcPorts() {
        return tpcPorts;
    }

    /**
     * Returns the token to be used to authenticate TPC channels
     * or {@code null} if TPC is disabled.
     */
    @Nullable
    public byte[] getTpcToken() {
        return tpcToken;
    }

    public boolean isMemberListVersionExists() {
        return isMemberListVersionExists;
    }

    public boolean isPartitionListVersionExists() {
        return isPartitionListVersionExists;
    }

    public boolean isKeyValuePairsExists() {
        return isKeyValuePairsExists;
    }

    public int getMemberListVersion() {
        return memberListVersion;
    }

    public List<MemberInfo> getMemberInfos() {
        return memberInfos;
    }

    public int getPartitionListVersion() {
        return partitionListVersion;
    }

    public List<Entry<UUID, List<Integer>>> getPartitions() {
        return partitions;
    }

    public Map<String, String> getKeyValuePairs() {
        return keyValuePairs;
    }

    public static AuthenticationResponse from(ClientMessage message) {
        switch (message.getMessageType()) {
            case ClientAuthenticationCodec.RESPONSE_MESSAGE_TYPE:
                return fromAuthenticationCodec(message);
            case ClientAuthenticationCustomCodec.RESPONSE_MESSAGE_TYPE:
                return fromAuthenticationCustomCodec(message);
            default:
                throw new IllegalStateException("Unexpected response message type");
        }
    }

    private static AuthenticationResponse fromAuthenticationCodec(ClientMessage message) {
        ClientAuthenticationCodec.ResponseParameters parameters = ClientAuthenticationCodec.decodeResponse(message);
        return new AuthenticationResponse(
                parameters.status,
                parameters.address,
                parameters.memberUuid,
                parameters.serializationVersion,
                parameters.serverHazelcastVersion,
                parameters.partitionCount,
                parameters.clusterId,
                parameters.failoverSupported,
                parameters.tpcPorts,
                parameters.tpcToken,
                parameters.isMemberListVersionExists,
                parameters.memberListVersion,
                parameters.memberInfos,
                parameters.isPartitionListVersionExists,
                parameters.partitionListVersion,
                parameters.partitions,
                parameters.isKeyValuePairsExists,
                parameters.keyValuePairs
        );
    }

    private static AuthenticationResponse fromAuthenticationCustomCodec(ClientMessage message) {
        ClientAuthenticationCustomCodec.ResponseParameters parameters = ClientAuthenticationCustomCodec.decodeResponse(message);
        return new AuthenticationResponse(
                parameters.status,
                parameters.address,
                parameters.memberUuid,
                parameters.serializationVersion,
                parameters.serverHazelcastVersion,
                parameters.partitionCount,
                parameters.clusterId,
                parameters.failoverSupported,
                parameters.tpcPorts,
                parameters.tpcToken,
                parameters.isMemberListVersionExists,
                parameters.memberListVersion,
                parameters.memberInfos,
                parameters.isPartitionListVersionExists,
                parameters.partitionListVersion,
                parameters.partitions,
                parameters.isKeyValuePairsExists,
                parameters.keyValuePairs
        );
    }
}
