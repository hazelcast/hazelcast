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

package com.hazelcast.client.impl.protocol.task.list;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ListAddAllCodec;
import com.hazelcast.client.impl.protocol.task.AbstractPartitionMessageTask;
import com.hazelcast.collection.impl.collection.operations.CollectionAddAllOperation;
import com.hazelcast.collection.impl.list.ListService;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.ListPermission;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.security.Permission;
import java.util.List;

/**
 * Client Protocol Task for handling messages with type ID:
 * {@link com.hazelcast.client.impl.protocol.codec.ListMessageType#LIST_ADDALL}
 */
public class ListAddAllMessageTask
        extends AbstractPartitionMessageTask<ListAddAllCodec.RequestParameters> {

    public ListAddAllMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Operation prepareOperation() {
        return new CollectionAddAllOperation(parameters.name, (List<Data>) parameters.valueList);
    }

    @Override
    protected ListAddAllCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return ListAddAllCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return ListAddAllCodec.encodeResponse((Boolean) response);
    }

    @Override
    public String getServiceName() {
        return ListService.SERVICE_NAME;
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{parameters.valueList};
    }

    @Override
    public Permission getRequiredPermission() {
        return new ListPermission(parameters.name, ActionConstants.ACTION_ADD);
    }

    @Override
    public String getMethodName() {
        return "addAll";
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

}
