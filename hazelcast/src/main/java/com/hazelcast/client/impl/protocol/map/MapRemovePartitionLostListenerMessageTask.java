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

package com.hazelcast.client.impl.protocol.map;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.parameters.BooleanResultParameters;
import com.hazelcast.client.impl.protocol.parameters.MapRemovePartitionLostListenerParameters;
import com.hazelcast.client.impl.protocol.task.AbstractCallableMessageTask;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.partition.InternalPartitionService;

import java.security.Permission;

public class MapRemovePartitionLostListenerMessageTask
        extends AbstractCallableMessageTask<MapRemovePartitionLostListenerParameters> {


    public MapRemovePartitionLostListenerMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected ClientMessage call() {
        InternalPartitionService service = getService(InternalPartitionService.SERVICE_NAME);
        boolean success = service.removePartitionLostListener(parameters.registrationId);
        return BooleanResultParameters.encode(success);
    }

    @Override
    protected MapRemovePartitionLostListenerParameters decodeClientMessage(ClientMessage clientMessage) {
        return MapRemovePartitionLostListenerParameters.decode(clientMessage);
    }

    @Override
    public String getServiceName() {
        return InternalPartitionService.SERVICE_NAME;
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }

    @Override
    public String getMethodName() {
        return "removePartitionLostListener";
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{parameters.registrationId};
    }
}
