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

package com.hazelcast.client.impl.protocol.task.map;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ContinuousQuerySetReadCursorCodec;
import com.hazelcast.client.impl.protocol.task.AbstractPartitionMessageTask;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.querycache.subscriber.operation.SetReadCursorOperation;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.security.Permission;

/**
 * Client Protocol Task for handling messages with type ID:
 * {@link com.hazelcast.client.impl.protocol.codec.ContinuousQueryMessageType#CONTINUOUSQUERY_SETREADCURSOR}
 */
public class MapSetReadCursorMessageTask
        extends AbstractPartitionMessageTask<ContinuousQuerySetReadCursorCodec.RequestParameters> {

    public MapSetReadCursorMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected ContinuousQuerySetReadCursorCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return ContinuousQuerySetReadCursorCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return ContinuousQuerySetReadCursorCodec.encodeResponse((Boolean) response);
    }

    @Override
    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.mapName;
    }

    @Override
    public String getMethodName() {
        return null;
    }

    @Override
    public Object[] getParameters() {
        return null;
    }

    @Override
    protected Operation prepareOperation() {
        return new SetReadCursorOperation(parameters.mapName, parameters.cacheName, parameters.sequence, getPartitionId());
    }
}
