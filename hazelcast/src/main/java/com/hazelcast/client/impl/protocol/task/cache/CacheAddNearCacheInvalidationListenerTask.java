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

import com.hazelcast.cache.impl.CacheContext;
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.client.impl.ClientEndpoint;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.CacheAddNearCacheInvalidationListenerCodec;
import com.hazelcast.client.impl.protocol.task.AbstractAddListenerMessageTask;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nearcache.impl.invalidation.Invalidation;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.serialization.Data;

import java.security.Permission;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static com.hazelcast.spi.impl.InternalCompletableFuture.newCompletedFuture;

public class CacheAddNearCacheInvalidationListenerTask
        extends AbstractAddListenerMessageTask<CacheAddNearCacheInvalidationListenerCodec.RequestParameters> {

    public CacheAddNearCacheInvalidationListenerTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected CompletableFuture<UUID> processInternal() {
        CacheService cacheService = getService(CacheService.SERVICE_NAME);
        CacheContext cacheContext = cacheService.getOrCreateCacheContext(parameters.name);
        NearCacheInvalidationListener listener
                = new NearCacheInvalidationListener(endpoint, cacheContext,
                nodeEngine.getLocalMember().getUuid(), clientMessage.getCorrelationId());

        return parameters.localOnly ? newCompletedFuture(
                cacheService.registerLocalListener(parameters.name, listener)) : (CompletableFuture<UUID>) cacheService
                .registerListenerAsync(parameters.name, listener);
    }

    private final class NearCacheInvalidationListener extends AbstractCacheClientNearCacheInvalidationListener {

        NearCacheInvalidationListener(ClientEndpoint endpoint, CacheContext cacheContext,
                                      UUID localMemberUuid, long correlationId) {
            super(endpoint, cacheContext, localMemberUuid, correlationId);
        }

        @Override
        protected ClientMessage encodeBatchInvalidation(String name, List<Data> keys, List<UUID> sourceUuids,
                                                        List<UUID> partitionUuids, List<Long> sequences) {
            return CacheAddNearCacheInvalidationListenerCodec.encodeCacheBatchInvalidationEvent(name, keys,
                    sourceUuids, partitionUuids, sequences);
        }

        @Override
        protected ClientMessage encodeSingleInvalidation(String name, Data key, UUID sourceUuid,
                                                         UUID partitionUuid, long sequence) {
            return CacheAddNearCacheInvalidationListenerCodec.encodeCacheInvalidationEvent(name, key,
                    sourceUuid, partitionUuid, sequence);
        }

        @Override
        protected void sendMessageWithOrderKey(ClientMessage clientMessage, Object orderKey) {
            sendClientMessage(orderKey, clientMessage);
        }

        @Override
        protected boolean canSendInvalidation(Invalidation invalidation) {
            return true;
        }
    }

    @Override
    protected CacheAddNearCacheInvalidationListenerCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return CacheAddNearCacheInvalidationListenerCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return CacheAddNearCacheInvalidationListenerCodec.encodeResponse((UUID) response);
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

    @Override
    public String getServiceName() {
        return CacheService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }

}
