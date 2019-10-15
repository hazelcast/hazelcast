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

import com.hazelcast.cache.impl.CacheContext;
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.client.impl.ClientEndpoint;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.CacheAddInvalidationListenerCodec;
import com.hazelcast.client.impl.protocol.task.AbstractCallableMessageTask;
import com.hazelcast.client.impl.protocol.task.ListenerMessageTask;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nearcache.impl.invalidation.Invalidation;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.nio.serialization.Data;

import java.security.Permission;
import java.util.List;
import java.util.UUID;

public class Pre38CacheAddInvalidationListenerTask
        extends AbstractCallableMessageTask<CacheAddInvalidationListenerCodec.RequestParameters>
        implements ListenerMessageTask {

    public Pre38CacheAddInvalidationListenerTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Object call() {
        CacheService cacheService = getService(CacheService.SERVICE_NAME);
        CacheContext cacheContext = cacheService.getOrCreateCacheContext(parameters.name);
        UUID uuid = nodeEngine.getLocalMember().getUuid();
        long correlationId = clientMessage.getCorrelationId();
        Pre38NearCacheInvalidationListener listener
                = new Pre38NearCacheInvalidationListener(endpoint, cacheContext, uuid, correlationId);
        UUID registrationId =
                cacheService.addInvalidationListener(parameters.name, listener, parameters.localOnly);
        endpoint.addListenerDestroyAction(CacheService.SERVICE_NAME, parameters.name, registrationId);
        return registrationId;
    }

    /**
     * This listener is here to be used with server versions < 3.8.
     * Because new improvements for Near Cache eventual consistency cannot work with server versions < 3.8.
     */
    private final class Pre38NearCacheInvalidationListener extends AbstractCacheClientNearCacheInvalidationListener {

        Pre38NearCacheInvalidationListener(ClientEndpoint endpoint, CacheContext cacheContext,
                                           UUID localMemberUuid, long correlationId) {
            super(endpoint, cacheContext, localMemberUuid, correlationId);
        }

        @Override
        protected ClientMessage encodeBatchInvalidation(String name, List<Data> keys, List<UUID> sourceUuids,
                                                        List<UUID> partitionUuids, List<Long> sequences) {
            return CacheAddInvalidationListenerCodec.encodeCacheBatchInvalidationEvent(name, keys, sourceUuids,
                    partitionUuids, sequences);
        }

        @Override
        protected ClientMessage encodeSingleInvalidation(String name, Data key, UUID sourceUuid,
                                                         UUID partitionUuid, long sequence) {
            return CacheAddInvalidationListenerCodec.encodeCacheInvalidationEvent(name, key, sourceUuid,
                    partitionUuid, sequence);
        }

        @Override
        protected void sendMessageWithOrderKey(ClientMessage clientMessage, Object orderKey) {
            sendClientMessage(orderKey, clientMessage);
        }

        @Override
        protected boolean canSendInvalidation(Invalidation invalidation) {
            return !endpoint.getUuid().equals(invalidation.getSourceUuid());
        }
    }

    @Override
    protected CacheAddInvalidationListenerCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return CacheAddInvalidationListenerCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return CacheAddInvalidationListenerCodec.encodeResponse((UUID) response);
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
