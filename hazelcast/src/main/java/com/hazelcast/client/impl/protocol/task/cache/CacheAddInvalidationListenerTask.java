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
import com.hazelcast.cache.impl.client.CacheBatchInvalidationMessage;
import com.hazelcast.cache.impl.client.CacheInvalidationMessage;
import com.hazelcast.cache.impl.client.CacheSingleInvalidationMessage;
import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.CacheAddInvalidationListenerCodec;
import com.hazelcast.client.impl.protocol.task.AbstractCallableMessageTask;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.NotifiableEventListener;

import java.security.Permission;
import java.util.ArrayList;
import java.util.List;

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
            if (!endpoint.isAlive()) {
                return;
            }
            if (eventObject instanceof CacheInvalidationMessage) {
                String targetUuid = endpoint.getUuid();
                if (eventObject instanceof CacheSingleInvalidationMessage) {
                    CacheSingleInvalidationMessage message = (CacheSingleInvalidationMessage) eventObject;
                    if (!targetUuid.equals(message.getSourceUuid())) {
                        // Since we already filtered as source uuid, no need to send source uuid to client
                        // TODO Maybe don't send name also to client
                        ClientMessage eventMessage =
                                CacheAddInvalidationListenerCodec
                                        .encodeCacheInvalidationEvent(message.getName(),
                                                                      message.getKey(),
                                                                      null);
                        sendClientMessage(message.getName(), eventMessage);
                    }
                } else if (eventObject instanceof CacheBatchInvalidationMessage) {
                    CacheBatchInvalidationMessage message = (CacheBatchInvalidationMessage) eventObject;
                    List<CacheSingleInvalidationMessage> invalidationMessages =
                            message.getInvalidationMessages();
                    List<Data> filteredKeys = new ArrayList<Data>(invalidationMessages.size());
                    for (CacheSingleInvalidationMessage invalidationMessage : invalidationMessages) {
                        if (!targetUuid.equals(invalidationMessage.getSourceUuid())) {
                            filteredKeys.add(invalidationMessage.getKey());
                        }
                    }
                    // Since we already filtered keys as source uuid, no need to send source uuid list to client
                    // TODO Maybe don't send name also to client
                    ClientMessage eventMessage =
                            CacheAddInvalidationListenerCodec
                                .encodeCacheBatchInvalidationEvent(message.getName(),
                                                                   filteredKeys,
                                                                   null);
                    sendClientMessage(message.getName(), eventMessage);
                }
            }
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
