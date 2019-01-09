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
import com.hazelcast.client.impl.protocol.codec.AtomicReferenceAlterCodec;
import com.hazelcast.client.impl.protocol.task.AbstractPartitionMessageTask;
import com.hazelcast.concurrent.atomicreference.AtomicReferenceService;
import com.hazelcast.concurrent.atomicreference.operations.AlterOperation;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.AtomicReferencePermission;
import com.hazelcast.spi.Operation;

import java.security.Permission;

public class AtomicReferenceAlterMessageTask
        extends AbstractPartitionMessageTask<AtomicReferenceAlterCodec.RequestParameters> {

    public AtomicReferenceAlterMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Operation prepareOperation() {
        return new AlterOperation(parameters.name, parameters.function);
    }

    @Override
    protected AtomicReferenceAlterCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return AtomicReferenceAlterCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return AtomicReferenceAlterCodec.encodeResponse();
    }

    @Override
    public String getServiceName() {
        return AtomicReferenceService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return new AtomicReferencePermission(parameters.name, ActionConstants.ACTION_MODIFY);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public String getMethodName() {
        return "alter";
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{parameters.function};
    }
}
