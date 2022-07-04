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

import com.hazelcast.cache.impl.CacheClearResponse;
import com.hazelcast.cache.impl.CacheOperationProvider;
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.impl.operation.CacheClearOperationFactory;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.CacheClearCodec;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.CachePermission;
import com.hazelcast.spi.impl.operationservice.OperationFactory;

import javax.cache.CacheException;
import java.security.Permission;
import java.util.Map;

/**
 * This client request  specifically calls {@link CacheClearOperationFactory} on the server side.
 *
 * @see CacheClearOperationFactory
 */
public class CacheClearMessageTask
        extends AbstractCacheAllPartitionsTask<String> {

    public CacheClearMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected String decodeClientMessage(ClientMessage clientMessage) {
        return CacheClearCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return CacheClearCodec.encodeResponse();
    }

    @Override
    protected OperationFactory createOperationFactory() {
        CacheOperationProvider operationProvider = getOperationProvider(parameters);
        return operationProvider.createClearOperationFactory();
    }


    @Override
    protected Object reduce(Map<Integer, Object> map) {
        for (Map.Entry<Integer, Object> entry : map.entrySet()) {
            if (entry.getValue() == null) {
                continue;
            }
            final CacheClearResponse cacheClearResponse = nodeEngine.toObject(entry.getValue());
            final Object response = cacheClearResponse.getResponse();
            if (response instanceof CacheException) {
                throw (CacheException) response;
            }
        }

        return null;
    }

    @Override
    public Permission getRequiredPermission() {
        return new CachePermission(parameters, ActionConstants.ACTION_REMOVE);
    }

    @Override
    public String getServiceName() {
        return CacheService.SERVICE_NAME;
    }

    @Override
    public String getDistributedObjectName() {
        return parameters;
    }

    @Override
    public Object[] getParameters() {
        return null;
    }

    @Override
    public String getMethodName() {
        return "clear";
    }

}
