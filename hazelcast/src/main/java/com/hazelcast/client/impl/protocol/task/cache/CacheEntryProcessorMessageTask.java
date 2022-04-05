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

package com.hazelcast.client.impl.protocol.task.cache;

import com.hazelcast.cache.impl.CacheOperationProvider;
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.impl.operation.CacheEntryProcessorOperation;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.CacheEntryProcessorCodec;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.CachePermission;
import com.hazelcast.spi.impl.operationservice.Operation;

import javax.cache.processor.EntryProcessor;
import java.security.Permission;
import java.util.ArrayList;

/**
 * This client request  specifically calls {@link CacheEntryProcessorOperation} on the server side.
 *
 * @see CacheEntryProcessorOperation
 */
public class CacheEntryProcessorMessageTask
        extends AbstractCacheMessageTask<CacheEntryProcessorCodec.RequestParameters> {

    public CacheEntryProcessorMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Operation prepareOperation() {
        CacheService service = getService(getServiceName());
        CacheOperationProvider operationProvider = getOperationProvider(parameters.name);
        EntryProcessor entryProcessor = (EntryProcessor) service.toObject(parameters.entryProcessor);
        ArrayList argumentsList = new ArrayList(parameters.arguments.size());
        for (Data data : parameters.arguments) {
            argumentsList.add(service.toObject(data));
        }
        return operationProvider
                .createEntryProcessorOperation(parameters.key, parameters.completionId, entryProcessor
                        , argumentsList.toArray());
    }

    @Override
    protected CacheEntryProcessorCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return CacheEntryProcessorCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return CacheEntryProcessorCodec.encodeResponse(serializationService.toData(response));
    }

    @Override
    public Permission getRequiredPermission() {
        return new CachePermission(parameters.name, ActionConstants.ACTION_READ,
                ActionConstants.ACTION_REMOVE, ActionConstants.ACTION_PUT);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{parameters.key, parameters.entryProcessor, parameters.arguments};
    }

    @Override
    public String getMethodName() {
        return "invoke";
    }
}
