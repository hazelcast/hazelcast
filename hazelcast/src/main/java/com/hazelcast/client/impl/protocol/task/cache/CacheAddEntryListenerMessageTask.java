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
import com.hazelcast.cache.impl.CacheEventData;
import com.hazelcast.cache.impl.CacheEventListener;
import com.hazelcast.cache.impl.CacheEventSet;
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.client.impl.ClientEndpoint;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.CacheAddEntryListenerCodec;
import com.hazelcast.client.impl.protocol.task.AbstractAddListenerMessageTask;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.internal.services.ListenerWrapperEventFilter;
import com.hazelcast.internal.services.NotifiableEventListener;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.CachePermission;
import com.hazelcast.spi.impl.eventservice.EventRegistration;

import java.security.Permission;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static com.hazelcast.spi.impl.InternalCompletableFuture.newCompletedFuture;

/**
 * Client request which registers an event listener on behalf of the client and delegates the received events
 * back to client.
 *
 * @see CacheService#registerListener(String, CacheEventListener, boolean localOnly)
 */
public class CacheAddEntryListenerMessageTask
        extends AbstractAddListenerMessageTask<CacheAddEntryListenerCodec.RequestParameters> {

    public CacheAddEntryListenerMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected CompletableFuture<UUID> processInternal() {
        final CacheService service = getService(CacheService.SERVICE_NAME);
        CacheEntryListener cacheEntryListener = new CacheEntryListener(endpoint, this);

        if (parameters.localOnly) {
            return newCompletedFuture(service.registerLocalListener(parameters.name, cacheEntryListener, cacheEntryListener));
        }

        return service.registerListenerAsync(parameters.name, cacheEntryListener, cacheEntryListener);
    }

    @Override
    protected void addDestroyAction(UUID registrationId) {
        final CacheService service = getService(CacheService.SERVICE_NAME);
        endpoint.addDestroyAction(registrationId, () -> service.deregisterListener(parameters.name, registrationId));
    }

    private static final class CacheEntryListener
            implements CacheEventListener,
            NotifiableEventListener<CacheService>,
            ListenerWrapperEventFilter {

        private final ClientEndpoint endpoint;
        private final CacheAddEntryListenerMessageTask cacheAddEntryListenerMessageTask;

        private CacheEntryListener(ClientEndpoint endpoint,
                                   CacheAddEntryListenerMessageTask cacheAddEntryListenerMessageTask) {
            this.endpoint = endpoint;
            this.cacheAddEntryListenerMessageTask = cacheAddEntryListenerMessageTask;
        }

        private Data getPartitionKey(Object eventObject) {
            Data partitionKey = null;
            if (eventObject instanceof CacheEventSet) {
                Set<CacheEventData> events = ((CacheEventSet) eventObject).getEvents();
                if (events.size() > 1) {
                    partitionKey = new HeapData();
                } else if (events.size() == 1) {
                    partitionKey = events.iterator().next().getDataKey();
                }
            } else if (eventObject instanceof CacheEventData) {
                partitionKey = ((CacheEventData) eventObject).getDataKey();
            }
            return partitionKey;
        }

        @Override
        public void handleEvent(Object eventObject) {
            if (!endpoint.isAlive()) {
                return;
            }
            if (eventObject instanceof CacheEventSet) {
                CacheEventSet ces = (CacheEventSet) eventObject;
                Data partitionKey = getPartitionKey(eventObject);
                ClientMessage clientMessage =
                        CacheAddEntryListenerCodec.
                                encodeCacheEvent(ces.getEventType().getType(), ces.getEvents(), ces.getCompletionId());
                cacheAddEntryListenerMessageTask.sendClientMessage(partitionKey, clientMessage);
            }
        }

        @Override
        public void onRegister(CacheService service, String serviceName,
                               String topic, EventRegistration registration) {
            CacheContext cacheContext = service.getOrCreateCacheContext(topic);
            cacheContext.increaseCacheEntryListenerCount();
        }

        @Override
        public void onDeregister(CacheService service, String serviceName,
                                 String topic, EventRegistration registration) {
            CacheContext cacheContext = service.getOrCreateCacheContext(topic);
            cacheContext.decreaseCacheEntryListenerCount();
        }

        @Override
        public Object getListener() {
            return this;
        }

        @Override
        public boolean eval(Object event) {
            return true;
        }

    }

    @Override
    protected CacheAddEntryListenerCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return CacheAddEntryListenerCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return CacheAddEntryListenerCodec.encodeResponse((UUID) response);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
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
        return new CachePermission(parameters.name, ActionConstants.ACTION_LISTEN);
    }

    @Override
    public String getMethodName() {
        return "registerCacheEntryListener";
    }

}
