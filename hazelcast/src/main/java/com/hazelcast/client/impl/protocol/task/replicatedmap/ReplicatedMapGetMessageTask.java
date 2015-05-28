/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.impl.protocol.codec.ReplicatedMapGetCodec;
import com.hazelcast.client.impl.protocol.task.AbstractCallableMessageTask;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.replicatedmap.impl.record.ReplicatedRecordStore;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.ReplicatedMapPermission;

import java.security.Permission;

public class ReplicatedMapGetMessageTask
        extends AbstractCallableMessageTask<ReplicatedMapGetCodec.RequestParameters> {

    public ReplicatedMapGetMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Object call() throws Exception {
        ReplicatedMapService replicatedMapService = getService(ReplicatedMapService.SERVICE_NAME);
        ReplicatedRecordStore recordStore = replicatedMapService.getReplicatedRecordStore(parameters.name, true);
        Object returnValue = recordStore.get(parameters.key);
        return serializationService.toData(returnValue);
    }

    @Override
    protected ReplicatedMapGetCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return ReplicatedMapGetCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return ReplicatedMapGetCodec.encodeResponse((Data) response);
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
        return "get";
    }

    @Override
    public Permission getRequiredPermission() {
        return new ReplicatedMapPermission(parameters.name, ActionConstants.ACTION_READ);
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{parameters.key};
    }
}
