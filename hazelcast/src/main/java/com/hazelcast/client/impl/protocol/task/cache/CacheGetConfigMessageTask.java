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

import com.hazelcast.cache.impl.operation.CacheGetConfigOperation;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.CacheGetConfigCodec;
import com.hazelcast.client.impl.protocol.task.AbstractAddressMessageTask;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.LegacyCacheConfig;
import com.hazelcast.instance.BuildInfo;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.properties.GroupProperty;

import java.security.Permission;

import static com.hazelcast.cache.impl.ICacheService.SERVICE_NAME;

/**
 * This client request specifically calls {@link CacheGetConfigOperation} on the server side.
 *
 * @see CacheGetConfigOperation
 */
public class CacheGetConfigMessageTask
        extends AbstractAddressMessageTask<CacheGetConfigCodec.RequestParameters> {

    public CacheGetConfigMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Address getAddress() {
        return nodeEngine.getThisAddress();
    }

    @Override
    protected Operation prepareOperation() {
        return new CacheGetConfigOperation(parameters.name, parameters.simpleName);
    }

    @Override
    public String getServiceName() {
        return SERVICE_NAME;
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
    public Permission getRequiredPermission() {
        return null;
    }

    @Override
    protected CacheGetConfigCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return CacheGetConfigCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        Data responseData = serializeCacheConfig(response);

        return CacheGetConfigCodec.encodeResponse(responseData);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    private Data serializeCacheConfig(Object response) {
        Data responseData = null;
        if (BuildInfo.UNKNOWN_HAZELCAST_VERSION == endpoint.getClientVersion()) {
            boolean compatibilityEnabled = nodeEngine.getProperties().getBoolean(GroupProperty.COMPATIBILITY_3_6_CLIENT_ENABLED);
            if (compatibilityEnabled) {
                responseData = nodeEngine.toData(response == null ? null : new LegacyCacheConfig((CacheConfig) response));
            }
        }

        if (null == responseData) {
            responseData = nodeEngine.toData(response);
        }
        return responseData;
    }
}
