/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.CacheCreateConfigCodec;
import com.hazelcast.client.impl.protocol.codec.holder.CacheConfigHolder;
import com.hazelcast.client.impl.protocol.task.AbstractMessageTask;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.spi.merge.SplitBrainMergePolicyProvider;

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
        // parameters.cacheConfig is not nullable by protocol definition, hence no need for null check
        CacheConfig cacheConfig = parameters.cacheConfig.asCacheConfig(serializationService);
        CacheService cacheService = getService(CacheService.SERVICE_NAME);

        SplitBrainMergePolicyProvider mergePolicyProvider = nodeEngine.getSplitBrainMergePolicyProvider();
        checkCacheConfig(cacheConfig, mergePolicyProvider);

        Object mergePolicy = mergePolicyProvider.getMergePolicy(cacheConfig.getMergePolicyConfig().getPolicy());
        checkMergePolicySupportsInMemoryFormat(cacheConfig.getName(), mergePolicy, cacheConfig.getInMemoryFormat(), true, logger);

        ICompletableFuture future = cacheService.createCacheConfigOnAllMembersAsync(PreJoinCacheConfig.of(cacheConfig));
        future.andThen(this);
    }

    @Override
    protected CacheCreateConfigCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return CacheCreateConfigCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        CacheConfig cacheConfig = (CacheConfig) response;
        return CacheCreateConfigCodec.encodeResponse(CacheConfigHolder.of(cacheConfig, serializationService));
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
}
