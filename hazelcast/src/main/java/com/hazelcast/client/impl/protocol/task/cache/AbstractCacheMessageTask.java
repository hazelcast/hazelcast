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

import com.hazelcast.cache.CacheNotExistsException;
import com.hazelcast.cache.impl.CacheOperationProvider;
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.impl.ICacheService;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.task.AbstractPartitionMessageTask;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.LegacyCacheConfig;
import com.hazelcast.instance.BuildInfo;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.properties.GroupProperty;

import java.security.Permission;

/**
 * Abstract Cache request to handle InMemoryFormat which needed for operation provider
 */
public abstract class AbstractCacheMessageTask<P>
        extends AbstractPartitionMessageTask<P> {

    protected AbstractCacheMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    protected CacheOperationProvider getOperationProvider(String name) {
        ICacheService service = getService(CacheService.SERVICE_NAME);
        final CacheConfig cacheConfig = service.getCacheConfig(name);
        if (cacheConfig == null) {
            throw new CacheNotExistsException("Cache " + name + " is already destroyed or not created yet, on "
                    + nodeEngine.getLocalMember());
        }
        final InMemoryFormat inMemoryFormat = cacheConfig.getInMemoryFormat();
        return service.getCacheOperationProvider(name, inMemoryFormat);
    }

    @Override
    public String getServiceName() {
        return CacheService.SERVICE_NAME;
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

    protected Data serializeCacheConfig(Object response) {
        Data responseData = null;
        if (BuildInfo.UNKNOWN_HAZELCAST_VERSION == getClientVersion()) {
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
