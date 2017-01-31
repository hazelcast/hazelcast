/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.impl.operation.CacheCreateConfigOperation;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.CacheCreateConfigCodec;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.LegacyCacheConfig;
import com.hazelcast.instance.BuildInfo;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.properties.GroupProperty;

import java.security.Permission;

/**
 * This client request  specifically calls {@link CacheCreateConfigOperation} on the server side.
 *
 * @see CacheCreateConfigOperation
 */
public class CacheCreateConfigMessageTask
        extends AbstractCacheMessageTask<CacheCreateConfigCodec.RequestParameters> {

    public CacheCreateConfigMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Operation prepareOperation() {
        CacheConfig cacheConfig = extractCacheConfigFromMessage();

        return new CacheCreateConfigOperation(cacheConfig, parameters.createAlsoOnOthers);
    }

    private CacheConfig extractCacheConfigFromMessage() {
        int clientVersion = getClientVersion();
        CacheConfig cacheConfig = null;
        if (BuildInfo.UNKNOWN_HAZELCAST_VERSION == clientVersion) {
            boolean compatibilityEnabled = nodeEngine.getProperties().getBoolean(GroupProperty.COMPATIBILITY_3_6_CLIENT_ENABLED);
            if (compatibilityEnabled) {
                LegacyCacheConfig legacyCacheConfig = nodeEngine.toObject(parameters.cacheConfig, LegacyCacheConfig.class);
                if (null == legacyCacheConfig) {
                    return null;
                }
                return legacyCacheConfig.getConfigAndReset();
            }
        }

        return (CacheConfig) nodeEngine.toObject(parameters.cacheConfig);
    }

    @Override
    protected CacheCreateConfigCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return CacheCreateConfigCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        Data responseData = serializeCacheConfig(response);
        return CacheCreateConfigCodec.encodeResponse(responseData);
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
        return null;
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
