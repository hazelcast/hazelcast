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

package com.hazelcast.client.impl.protocol.task.multimap;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.parameters.BooleanResultParameters;
import com.hazelcast.client.impl.protocol.parameters.MultiMapPutParameters;
import com.hazelcast.client.impl.protocol.task.AbstractPartitionMessageTask;
import com.hazelcast.instance.Node;
import com.hazelcast.multimap.impl.MultiMapService;
import com.hazelcast.multimap.impl.operations.PutOperation;
import com.hazelcast.nio.Connection;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.MultiMapPermission;
import com.hazelcast.spi.Operation;

import java.security.Permission;

/**
 * Client Protocol Task for handling messages with type id:
 * {@link com.hazelcast.client.impl.protocol.parameters.MultiMapMessageType#MULTIMAP_PUT}
 */
public class MultiMapPutMessageTask extends AbstractPartitionMessageTask<MultiMapPutParameters> {

    public MultiMapPutMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Operation prepareOperation() {
        return new PutOperation(parameters.name, parameters.key, parameters.threadId, parameters.value, -1);
    }

    @Override
    protected MultiMapPutParameters decodeClientMessage(ClientMessage clientMessage) {
        return MultiMapPutParameters.decode(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return BooleanResultParameters.encode((Boolean) response);
    }

    @Override
    public String getServiceName() {
        return MultiMapService.SERVICE_NAME;
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public Permission getRequiredPermission() {
        return new MultiMapPermission(parameters.name, ActionConstants.ACTION_PUT);
    }

    @Override
    public String getMethodName() {
        return "put";
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{parameters.key, parameters.value};
    }

}

