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

import com.hazelcast.cache.impl.CacheClearResponse;
import com.hazelcast.cache.impl.CacheOperationProvider;
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.impl.operation.CacheClearOperationFactory;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.parameters.CacheClearParameters;
import com.hazelcast.client.impl.protocol.parameters.MapIntBooleanResultParameters;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.spi.OperationFactory;

import javax.cache.CacheException;
import java.util.HashMap;
import java.util.Map;

/**
 * This client request  specifically calls {@link CacheClearOperationFactory} on the server side.
 *
 * @see CacheClearOperationFactory
 */
public class CacheClearMessageTask
        extends AbstractCacheAllPartitionsTask<CacheClearParameters> {

    public CacheClearMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected CacheClearParameters decodeClientMessage(ClientMessage clientMessage) {
        return CacheClearParameters.decode(clientMessage);
    }

    @Override
    protected OperationFactory createOperationFactory() {
        CacheOperationProvider operationProvider = getOperationProvider(parameters.name);
        return operationProvider.createClearOperationFactory();
    }

    @Override
    protected ClientMessage reduce(Map<Integer, Object> map) {
        Map<Integer, Boolean> resultMap = new HashMap<Integer, Boolean>();
        CacheService service = getService(getServiceName());
        for (Map.Entry<Integer, Object> entry : map.entrySet()) {
            CacheClearResponse cacheClearResponse = (CacheClearResponse) service.toObject(entry.getValue());
            final Object response = cacheClearResponse.getResponse();
            if (response instanceof CacheException) {
                throw (CacheException) response;
            }
            resultMap.put(entry.getKey(), (Boolean) response);
        }
        return MapIntBooleanResultParameters.encode(resultMap);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

}
