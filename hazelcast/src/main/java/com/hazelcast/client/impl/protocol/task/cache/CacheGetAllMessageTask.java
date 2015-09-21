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
import com.hazelcast.cache.impl.operation.CacheGetAllOperationFactory;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.CacheGetAllCodec;
import com.hazelcast.instance.Node;
import com.hazelcast.map.impl.MapEntrySet;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.CachePermission;
import com.hazelcast.spi.OperationFactory;

import javax.cache.expiry.ExpiryPolicy;
import java.security.Permission;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * This client request  specifically calls {@link CacheGetAllOperationFactory} on the server side.
 *
 * @see CacheGetAllOperationFactory
 */
public class CacheGetAllMessageTask
        extends AbstractCacheAllPartitionsTask<CacheGetAllCodec.RequestParameters> {

    public CacheGetAllMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected CacheGetAllCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return CacheGetAllCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return CacheGetAllCodec.encodeResponse((Set<Map.Entry<Data, Data>>) response);
    }

    @Override
    protected OperationFactory createOperationFactory() {
        CacheOperationProvider operationProvider = getOperationProvider(parameters.name);
        CacheService service = getService(getServiceName());
        ExpiryPolicy expiryPolicy = (ExpiryPolicy) service.toObject(parameters.expiryPolicy);
        return operationProvider.createGetAllOperationFactory((Set<Data>) parameters.keys, expiryPolicy);
    }

    @Override
    protected Object reduce(Map<Integer, Object> map) {
        Set<Map.Entry<Data, Data>> reducedMap = new HashSet<Map.Entry<Data, Data>>(map.size());
        for (Map.Entry<Integer, Object> entry : map.entrySet()) {
            MapEntrySet mapEntrySet = (MapEntrySet) nodeEngine.toObject(entry.getValue());
            Set<Map.Entry<Data, Data>> entrySet = mapEntrySet.getEntrySet();
            for (Map.Entry<Data, Data> dataEntry : entrySet) {
                reducedMap.add(dataEntry);
            }
        }
        return reducedMap;
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
    public String getMethodName() {
        return "getAll";
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{parameters.keys};
    }

}
