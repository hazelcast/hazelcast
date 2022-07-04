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

package com.hazelcast.client.impl.protocol.task.multimap;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.MultiMapContainsEntryCodec;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.multimap.impl.operations.ContainsEntryOperation;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.MultiMapPermission;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.security.Permission;

/**
 * Client Protocol Task for handling messages with type ID:
 * {@link com.hazelcast.client.impl.protocol.codec.MultiMapMessageType#MULTIMAP_CONTAINSENTRY}
 */
public class MultiMapContainsEntryMessageTask
        extends AbstractMultiMapPartitionMessageTask<MultiMapContainsEntryCodec.RequestParameters> {

    public MultiMapContainsEntryMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Operation prepareOperation() {
        ContainsEntryOperation operation = new ContainsEntryOperation(parameters.name, parameters.key, parameters.value);
        operation.setThreadId(parameters.threadId);
        return operation;
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return MultiMapContainsEntryCodec.encodeResponse((Boolean) response);
    }

    @Override
    protected MultiMapContainsEntryCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return MultiMapContainsEntryCodec.decodeRequest(clientMessage);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public String getMethodName() {
        return "containsEntry";
    }

    @Override
    public Permission getRequiredPermission() {
        return new MultiMapPermission(parameters.name, ActionConstants.ACTION_READ);
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{parameters.key, parameters.value};
    }
}

