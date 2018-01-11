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

import com.hazelcast.cache.impl.CacheClearResponse;
import com.hazelcast.cache.impl.CacheOperationProvider;
import com.hazelcast.cache.impl.operation.CacheRemoveAllOperationFactory;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.CacheRemoveAllKeysCodec;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.CachePermission;
import com.hazelcast.spi.OperationFactory;

import javax.cache.CacheException;
import java.security.Permission;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * This client request  specifically calls {@link CacheRemoveAllOperationFactory} on the server side.
 *
 * @see CacheRemoveAllOperationFactory
 */
public class CacheRemoveAllKeysMessageTask
        extends AbstractCacheAllPartitionsTask<CacheRemoveAllKeysCodec.RequestParameters> {

    public CacheRemoveAllKeysMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected CacheRemoveAllKeysCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return CacheRemoveAllKeysCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return CacheRemoveAllKeysCodec.encodeResponse();
    }

    @Override
    protected OperationFactory createOperationFactory() {
        CacheOperationProvider operationProvider = getOperationProvider(parameters.name);
        Set<Data> keys = new HashSet<Data>(parameters.keys);
        return operationProvider.createRemoveAllOperationFactory(keys, parameters.completionId);
    }

    @Override
    protected ClientMessage reduce(Map<Integer, Object> map) {
        for (Map.Entry<Integer, Object> entry : map.entrySet()) {
            if (entry.getValue() == null) {
                continue;
            }
            final CacheClearResponse cacheClearResponse = (CacheClearResponse) nodeEngine.toObject(entry.getValue());
            final Object response = cacheClearResponse.getResponse();
            if (response instanceof CacheException) {
                throw (CacheException) response;
            }
        }
        return null;
    }

    @Override
    public Permission getRequiredPermission() {
        return new CachePermission(parameters.name, ActionConstants.ACTION_REMOVE);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{parameters.keys};
    }

    @Override
    public String getMethodName() {
        return "removeAll";
    }

}

