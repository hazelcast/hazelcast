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

package com.hazelcast.client.impl.protocol.task;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ClientAuthenticationCustomCodec;
import com.hazelcast.cluster.Member;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.cluster.Address;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.security.SimpleTokenCredentials;
import com.hazelcast.spi.partition.IPartitionService;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Custom Authentication with custom credential impl
 */
public class AuthenticationCustomCredentialsMessageTask
        extends AuthenticationBaseMessageTask<ClientAuthenticationCustomCodec.RequestParameters> {

    public AuthenticationCustomCredentialsMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected String getClientType() {
        return parameters.clientType;
    }

    @Override
    @SuppressWarnings("checkstyle:npathcomplexity")
    protected ClientAuthenticationCustomCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        ClientAuthenticationCustomCodec.RequestParameters parameters = ClientAuthenticationCustomCodec
                .decodeRequest(clientMessage);
        UUID uuid = parameters.uuid;
        assert uuid != null;
        clientUuid = uuid;
        credentials = new SimpleTokenCredentials(parameters.credentials.toByteArray());
        clientSerializationVersion = parameters.serializationVersion;
        clientVersion = parameters.clientHazelcastVersion;
        clientName = parameters.clientName;
        labels = Collections.unmodifiableSet(new HashSet<>(parameters.labels));
        partitionCount = parameters.partitionCount;
        clusterId = parameters.clusterId;
        return parameters;
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return (ClientMessage) response;
    }

    @Override
    protected ClientMessage encodeAuth(byte status, Address thisAddress, UUID uuid, byte version, int partitionCount,
                                       UUID clusterId) {
        Set<Member> members = clientEngine.getClusterService().getMembers();
        IPartitionService partitionService = clientEngine.getPartitionService();
        Map<Address, List<Integer>> partitionsMap = partitionService.getMemberPartitionsMap();
        int partitionStateVersion = partitionService.getPartitionStateVersion();

        return ClientAuthenticationCustomCodec
                .encodeResponse(status, thisAddress, uuid, version, getMemberBuildInfo().getVersion(), partitionCount, clusterId,
                        members, partitionsMap.entrySet(), partitionStateVersion);
    }

    @Override
    public String getServiceName() {
        return null;
    }

    @Override
    public String getDistributedObjectName() {
        return null;
    }

    @Override
    public String getMethodName() {
        return null;
    }

    @Override
    public Object[] getParameters() {
        return null;
    }

}
