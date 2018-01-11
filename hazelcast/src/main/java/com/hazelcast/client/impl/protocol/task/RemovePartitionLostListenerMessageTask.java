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

package com.hazelcast.client.impl.protocol.task;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ClientRemovePartitionLostListenerCodec;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.nio.Connection;

import java.security.Permission;

public class RemovePartitionLostListenerMessageTask
        extends AbstractRemoveListenerMessageTask<ClientRemovePartitionLostListenerCodec.RequestParameters> {

    public RemovePartitionLostListenerMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected boolean deRegisterListener() {
        final InternalPartitionService service = getService(InternalPartitionService.SERVICE_NAME);
        return service.removePartitionLostListener(parameters.registrationId);
    }

    @Override
    protected String getRegistrationId() {
        return parameters.registrationId;
    }

    @Override
    protected ClientRemovePartitionLostListenerCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return ClientRemovePartitionLostListenerCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return ClientRemovePartitionLostListenerCodec.encodeResponse((Boolean) response);
    }

    @Override
    public String getServiceName() {
        return InternalPartitionService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }

    @Override
    public String getDistributedObjectName() {
        return null;
    }

    @Override
    public String getMethodName() {
        return "removePartitionLostListener";
    }

}
