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

import com.hazelcast.client.impl.client.ClientPrincipal;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.newcodecs.Authentication;
import com.hazelcast.cluster.Member;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.security.UsernamePasswordCredentials;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;

/**
 * Default Authentication with username password handling task
 */
public class AuthenticationMessageTask extends AuthenticationBaseMessageTask<Authentication.Request> {

    public AuthenticationMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Authentication.Request decodeClientMessage() {
        final Authentication.Request parameters = Authentication.Request.decode(clientMessage);
        final String uuid = parameters.uuid;
        final String ownerUuid = parameters.ownerUuid;
        if (uuid != null && uuid.length() > 0) {
            principal = new ClientPrincipal(uuid, ownerUuid);
        }
        credentials = new UsernamePasswordCredentials(parameters.username, parameters.password);
        clientSerializationVersion = parameters.serializationVersion;

        clientVersion = parameters.clientHazelcastVersion.orElse(null);
        clientName = parameters.clientName.orElse(null);
        labels = parameters.labels.map(strings -> Collections.unmodifiableSet(new HashSet<>(strings))).
                orElse(Collections.emptySet());
        partitionCount = parameters.partitionCount.orElse(null);
        clusterId = parameters.clusterId.orElse(null);
        return parameters;
    }

    @Override
    protected Authentication.Request decodeClientMessage(ClientMessage clientMessage) {
        //TODO SANCAR delete this
        return null;
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return (ClientMessage) response;
    }

    @Override
    protected ClientMessage encodeAuth(byte status, Address thisAddress, String uuid, String ownerUuid, byte version,
                                       List<Member> cleanedUpMembers, int partitionCount, String clusterId) {
        return Authentication.Response.encode(status, thisAddress, uuid, ownerUuid, version, getMemberBuildInfo().getVersion(),
                cleanedUpMembers, partitionCount, clusterId);
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

    @Override
    protected boolean isOwnerConnection() {
        return parameters.isOwnerConnection;
    }

    @Override
    protected String getClientType() {
        return parameters.clientType;
    }

}
