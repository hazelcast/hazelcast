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

package com.hazelcast.client.impl.protocol.task.replicatedmap;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ReplicatedMapPutCodec;
import com.hazelcast.client.impl.protocol.task.AbstractPartitionMessageTask;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.replicatedmap.impl.operation.PutOperation;
import com.hazelcast.replicatedmap.impl.operation.VersionResponsePair;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.ReplicatedMapPermission;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.security.Permission;
import java.util.concurrent.TimeUnit;

public class ReplicatedMapPutMessageTask
        extends AbstractPartitionMessageTask<ReplicatedMapPutCodec.RequestParameters> {

    public ReplicatedMapPutMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Operation prepareOperation() {
        return new PutOperation(parameters.name, parameters.key, parameters.value, parameters.ttl);
    }

    @Override
    protected ReplicatedMapPutCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return ReplicatedMapPutCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        VersionResponsePair versionResponsePair = (VersionResponsePair) response;
        return ReplicatedMapPutCodec.encodeResponse(serializationService.toData(versionResponsePair.getResponse()));
    }

    @Override
    public String getServiceName() {
        return ReplicatedMapService.SERVICE_NAME;
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public String getMethodName() {
        return "put";
    }

    @Override
    public Permission getRequiredPermission() {
        return new ReplicatedMapPermission(parameters.name, ActionConstants.ACTION_PUT);
    }

    @Override
    public Object[] getParameters() {
        if (parameters.ttl > 0) {
            return new Object[]{parameters.key, parameters.value, parameters.ttl, TimeUnit.MILLISECONDS};
        }
        return new Object[]{parameters.key, parameters.value};
    }

}
