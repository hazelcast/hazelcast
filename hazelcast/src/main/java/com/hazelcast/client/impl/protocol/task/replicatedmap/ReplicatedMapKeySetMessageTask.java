/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.impl.protocol.codec.ReplicatedMapKeySetCodec;
import com.hazelcast.client.impl.protocol.task.AbstractPartitionMessageTask;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.replicatedmap.impl.client.ReplicatedMapKeys;
import com.hazelcast.replicatedmap.impl.operation.KeySetOperation;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.ReplicatedMapPermission;
import com.hazelcast.spi.Operation;

import java.security.Permission;

public class ReplicatedMapKeySetMessageTask
        extends AbstractPartitionMessageTask<ReplicatedMapKeySetCodec.RequestParameters> {

    public ReplicatedMapKeySetMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Operation prepareOperation() {
        return new KeySetOperation(parameters.name);
    }

    @Override
    protected ReplicatedMapKeySetCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return ReplicatedMapKeySetCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        ReplicatedMapKeys keySet = (ReplicatedMapKeys) response;
        return ReplicatedMapKeySetCodec.encodeResponse(keySet.getKeys());
    }

    @Override
    public String getServiceName() {
        return ReplicatedMapService.SERVICE_NAME;
    }

    public Permission getRequiredPermission() {
        return new ReplicatedMapPermission(parameters.name, ActionConstants.ACTION_READ);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public String getMethodName() {
        return "keySet";
    }

    @Override
    public Object[] getParameters() {
        return null;
    }
}
