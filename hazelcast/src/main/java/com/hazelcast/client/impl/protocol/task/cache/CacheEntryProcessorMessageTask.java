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

package com.hazelcast.client.impl.protocol.task.cache;

import com.hazelcast.cache.impl.CacheOperationProvider;
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.impl.operation.CacheEntryProcessorOperation;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.parameters.CacheEntryProcessorParameters;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;

import javax.cache.processor.EntryProcessor;
import java.util.ArrayList;

/**
 * This client request  specifically calls {@link CacheEntryProcessorOperation} on the server side.
 *
 * @see CacheEntryProcessorOperation
 */
public class CacheEntryProcessorMessageTask
        extends AbstractCacheMessageTask<CacheEntryProcessorParameters> {

    public CacheEntryProcessorMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Operation prepareOperation() {
        CacheService service = getService(getServiceName());
        CacheOperationProvider operationProvider = getOperationProvider(parameters.name);
        //completionId now uses CorrelationId where both are non-correlated and unique
        int completionId = clientMessage.getCorrelationId();
        EntryProcessor entryProcessor = (EntryProcessor) service.toObject(parameters.entryProcessor);
        ArrayList argumentsList = new ArrayList(parameters.arguments.size());
        for (Data data : parameters.arguments) {
            argumentsList.add(service.toObject(data));
        }
        return operationProvider
                .createEntryProcessorOperation(parameters.key, completionId, entryProcessor, argumentsList.toArray());
    }

    @Override
    protected CacheEntryProcessorParameters decodeClientMessage(ClientMessage clientMessage) {
        return CacheEntryProcessorParameters.decode(clientMessage);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }
}
