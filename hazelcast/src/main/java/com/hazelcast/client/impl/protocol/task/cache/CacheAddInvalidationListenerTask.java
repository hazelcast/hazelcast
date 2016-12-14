/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.cache.impl.CacheEventListener;
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.CacheAddInvalidationListenerCodec;
import com.hazelcast.client.impl.protocol.task.AbstractCallableMessageTask;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.nearcache.impl.invalidation.BatchNearCacheInvalidation;
import com.hazelcast.internal.nearcache.impl.invalidation.Invalidation;
import com.hazelcast.internal.nearcache.impl.invalidation.SingleNearCacheInvalidation;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.NotifiableEventListener;

import java.security.Permission;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static com.hazelcast.client.impl.protocol.codec.CacheAddInvalidationListenerCodec.encodeCacheBatchInvalidationEvent;
import static com.hazelcast.client.impl.protocol.codec.CacheAddInvalidationListenerCodec.encodeCacheInvalidationEvent;

public class CacheAddInvalidationListenerTask
        extends AbstractCallableMessageTask<CacheAddInvalidationListenerCodec.RequestParameters> {

    public CacheAddInvalidationListenerTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Object call() {
        final ClientEndpoint endpoint = getEndpoint();
        CacheService cacheService = getService(CacheService.SERVICE_NAME);
        CacheContext cacheContext = cacheService.getOrCreateCacheContext(parameters.name);
        CacheInvalidationEventListener listener = new CacheInvalidationEventListener(endpoint, cacheContext);
        String registrationId =
                cacheService.addInvalidationListener(parameters.name, listener, parameters.localOnly);
        endpoint.addListenerDestroyAction(CacheService.SERVICE_NAME, parameters.name, registrationId);
        return registrationId;
    }

    private final class CacheInvalidationEventListener
            implements CacheEventListener, NotifiableEventListener<CacheService> {

        private final ClientEndpoint endpoint;
        private final CacheContext cacheContext;

        private CacheInvalidationEventListener(ClientEndpoint endpoint, CacheContext cacheContext) {
            this.endpoint = endpoint;
            this.cacheContext = cacheContext;
        }

        @Override
        public void handleEvent(Object eventObject) {
            if (!endpoint.isAlive() || !(eventObject instanceof Invalidation)) {
                return;
            }

            ClientMessage message = getClientMessage(eventObject);
            if (message != null) {
                sendClientMessage(((Invalidation) eventObject).getName(), message);
            }
        }

        private ClientMessage getClientMessage(Object eventObject) {
            if (eventObject instanceof SingleNearCacheInvalidation) {
                SingleNearCacheInvalidation message = (SingleNearCacheInvalidation) eventObject;
                // Since we already filtered as source uuid, no need to send source uuid to client
                // TODO Maybe don't send name also to client
                return encodeCacheInvalidationEvent(message.getName(), message.getKey(), message.getSourceUuid(),
                        message.getPartitionUuid(), message.getSequence());
            }


            if (eventObject instanceof BatchNearCacheInvalidation) {
                BatchNearCacheInvalidation message = (BatchNearCacheInvalidation) eventObject;
                List<Invalidation> invalidationMessages = message.getInvalidations();

                int size = invalidationMessages.size();
                List<Data> filteredKeys = new ArrayList<Data>(size);
                List<String> sourceUuids = new ArrayList<String>(invalidationMessages.size());
                List<UUID> partitionUuids = new ArrayList<UUID>(invalidationMessages.size());
                List<Long> sequences = new ArrayList<Long>(invalidationMessages.size());

                for (Invalidation invalidationMessage : invalidationMessages) {
                    filteredKeys.add(invalidationMessage.getKey());
                    sourceUuids.add(invalidationMessage.getSourceUuid());
                    partitionUuids.add(invalidationMessage.getPartitionUuid());
                    sequences.add(invalidationMessage.getSequence());
                }
                // Since we already filtered keys as source uuid, no need to send source uuid list to client
                // TODO Maybe don't send name also to client
                return encodeCacheBatchInvalidationEvent(message.getName(), filteredKeys, sourceUuids, partitionUuids, sequences);
            }

            throw new IllegalArgumentException("Unknown cache invalidation message type " + eventObject);
        }

        @Override
        public void onRegister(CacheService cacheService, String serviceName,
                               String topic, EventRegistration registration) {
            cacheContext.increaseInvalidationListenerCount();
        }

        @Override
        public void onDeregister(CacheService cacheService, String serviceName,
                                 String topic, EventRegistration registration) {
            cacheContext.decreaseInvalidationListenerCount();
        }

    }

    @Override
    protected CacheAddInvalidationListenerCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return CacheAddInvalidationListenerCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return CacheAddInvalidationListenerCodec.encodeResponse((String) response);
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
