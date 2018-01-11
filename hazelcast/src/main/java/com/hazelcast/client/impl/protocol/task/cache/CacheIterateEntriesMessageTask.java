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

package com.hazelcast.client.impl.protocol.task.cache;

import com.hazelcast.cache.impl.CacheEntryIterationResult;
import com.hazelcast.cache.impl.CacheOperationProvider;
import com.hazelcast.cache.impl.operation.CacheEntryIteratorOperation;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.CacheIterateEntriesCodec;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.CachePermission;
import com.hazelcast.spi.Operation;

import java.security.Permission;
import java.util.Collections;
import java.util.Map;

/**
 * This client request specifically calls {@link CacheEntryIteratorOperation} on the server side.
 *
 * @see CacheEntryIteratorOperation
 */
public class CacheIterateEntriesMessageTask
        extends AbstractCacheMessageTask<CacheIterateEntriesCodec.RequestParameters> {

    public CacheIterateEntriesMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Operation prepareOperation() {
        CacheOperationProvider operationProvider = getOperationProvider(parameters.name);
        return operationProvider.createEntryIteratorOperation(parameters.tableIndex, parameters.batch);
    }

    @Override
    protected CacheIterateEntriesCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return CacheIterateEntriesCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        if (response == null) {
            return CacheIterateEntriesCodec.encodeResponse(0, Collections.<Map.Entry<Data, Data>>emptyList());
        }
        CacheEntryIterationResult iteratorResult = (CacheEntryIterationResult) response;
        return CacheIterateEntriesCodec.encodeResponse(iteratorResult.getTableIndex(), iteratorResult.getEntries());
    }

    @Override
    public Permission getRequiredPermission() {
        return new CachePermission(parameters.name, ActionConstants.ACTION_READ);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public Object[] getParameters() {
        return null;
    }

    @Override
    public String getMethodName() {
        return "iterator";
    }
}
