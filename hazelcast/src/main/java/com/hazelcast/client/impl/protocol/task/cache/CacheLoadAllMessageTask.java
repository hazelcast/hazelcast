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
import com.hazelcast.cache.impl.operation.CacheLoadAllOperationFactory;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.parameters.CacheLoadAllParameters;
import com.hazelcast.client.impl.protocol.parameters.DataEntryListResultParameters;
import com.hazelcast.instance.Node;
import com.hazelcast.map.impl.MapEntrySet;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.OperationFactory;

import java.security.Permission;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * This client request  specifically calls {@link CacheLoadAllOperationFactory} on the server side.
 *
 * @see CacheLoadAllOperationFactory
 */
public class CacheLoadAllMessageTask
        extends AbstractCacheAllPartitionsTask<CacheLoadAllParameters> {

    public CacheLoadAllMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected CacheLoadAllParameters decodeClientMessage(ClientMessage clientMessage) {
        return CacheLoadAllParameters.decode(clientMessage);
    }

    @Override
    protected OperationFactory createOperationFactory() {
        CacheOperationProvider operationProvider = getOperationProvider(parameters.name);
        return operationProvider.createLoadAllOperationFactory(parameters.keys, parameters.replaceExistingValues);
    }

    @Override
    protected ClientMessage reduce(Map<Integer, Object> map) {
        CacheService service = getService(getServiceName());
        Map<Data, Data> reducedMap = new HashMap<Data, Data>(map.size());
        for (Map.Entry<Integer, Object> entry : map.entrySet()) {
            MapEntrySet mapEntrySet = (MapEntrySet) service.toObject(entry.getValue());
            Set<Map.Entry<Data, Data>> entrySet = mapEntrySet.getEntrySet();
            for (Map.Entry<Data, Data> dataEntry : entrySet) {
                reducedMap.put(dataEntry.getKey(), dataEntry.getValue());
            }
        }
        return DataEntryListResultParameters.encode(reducedMap);
    }

    @Override
    public String getServiceName() {
        return CacheService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public String getMethodName() {
        return null;
    }

    @Override
    public Object[] getParameters() {
        return null;
    }
}
