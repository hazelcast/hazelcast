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

package com.hazelcast.client.impl.protocol.task.atomicreference;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.AtomicReferenceContainsCodec;
import com.hazelcast.client.impl.protocol.task.AbstractPartitionMessageTask;
import com.hazelcast.cp.internal.datastructures.unsafe.atomicreference.AtomicReferenceService;
import com.hazelcast.cp.internal.datastructures.unsafe.atomicreference.operations.ContainsOperation;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.AtomicReferencePermission;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.security.Permission;

public class AtomicReferenceContainsMessageTask
        extends AbstractPartitionMessageTask<AtomicReferenceContainsCodec.RequestParameters> {

    public AtomicReferenceContainsMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Operation prepareOperation() {
        return new ContainsOperation(parameters.name, parameters.expected);
    }

    @Override
    protected AtomicReferenceContainsCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return AtomicReferenceContainsCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return AtomicReferenceContainsCodec.encodeResponse((Boolean) response);
    }

    @Override
    public String getServiceName() {
        return AtomicReferenceService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return new AtomicReferencePermission(parameters.name, ActionConstants.ACTION_READ);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public String getMethodName() {
        return "contains";
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{parameters.expected};
    }
}
