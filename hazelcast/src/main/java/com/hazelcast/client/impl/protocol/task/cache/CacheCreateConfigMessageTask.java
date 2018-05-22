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

import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.impl.ICacheService;
import com.hazelcast.cache.impl.PreJoinCacheConfig;
import com.hazelcast.cache.impl.merge.policy.CacheMergePolicyProvider;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.CacheCreateConfigCodec;
import com.hazelcast.client.impl.protocol.task.AbstractMessageTask;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.LegacyCacheConfig;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.instance.BuildInfo;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.properties.GroupProperty;

import java.security.Permission;

import static com.hazelcast.internal.config.ConfigValidator.checkCacheConfig;
import static com.hazelcast.internal.config.MergePolicyValidator.checkMergePolicySupportsInMemoryFormat;

/**
 * Creates the given CacheConfig on all members of the cluster.
 *
 * @see ICacheService#createCacheConfigOnAllMembers(PreJoinCacheConfig)
 */
public class CacheCreateConfigMessageTask
        extends AbstractMessageTask<CacheCreateConfigCodec.RequestParameters>
        implements ExecutionCallback {

    public CacheCreateConfigMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected void processMessage() {
        CacheConfig cacheConfig = extractCacheConfigFromMessage();
        CacheService cacheService = getService(CacheService.SERVICE_NAME);

        if (cacheConfig != null) {
            CacheMergePolicyProvider mergePolicyProvider = cacheService.getMergePolicyProvider();
            checkCacheConfig(cacheConfig, mergePolicyProvider);

            Object mergePolicy = mergePolicyProvider.getMergePolicy(cacheConfig.getMergePolicy());
            checkMergePolicySupportsInMemoryFormat(cacheConfig.getName(), mergePolicy, cacheConfig.getInMemoryFormat(),
                    nodeEngine.getClusterService().getClusterVersion(), true, logger);

            ICompletableFuture future = cacheService.createCacheConfigOnAllMembersAsync(PreJoinCacheConfig.of(cacheConfig));
            future.andThen(this);
        } else {
            sendResponse(null);
        }
    }

    private CacheConfig extractCacheConfigFromMessage() {
        int clientVersion = endpoint.getClientVersion();
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

    @Override
    public void onResponse(Object response) {
        sendResponse(response);
    }

    @Override
    public void onFailure(Throwable t) {
        handleProcessingFailure(t);
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
