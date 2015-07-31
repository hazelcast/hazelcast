/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.CacheAddEntryListenerCodec;
import com.hazelcast.client.impl.protocol.task.AbstractCallableMessageTask;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.impl.HeapData;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.ListenerWrapperEventFilter;
import com.hazelcast.spi.NotifiableEventListener;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.Serializable;
import java.security.Permission;
import java.util.Set;

/**
 * Client request which registers an event listener on behalf of the client and delegates the received events
 * back to client.
 *
 * @see CacheService#registerListener(String, CacheEventListener)
 */
public class CacheAddEntryListenerMessageTask
        extends AbstractCallableMessageTask<CacheAddEntryListenerCodec.RequestParameters> {

    public CacheAddEntryListenerMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Object call() {
        final ClientEndpoint endpoint = getEndpoint();
        final CacheService service = getService(CacheService.SERVICE_NAME);
        CacheEntryListener cacheEntryListener = new CacheEntryListener(endpoint, this);
        return service.registerListener(parameters.name, cacheEntryListener, cacheEntryListener);
    }

    @SuppressFBWarnings(value = "SE_NO_SERIALVERSIONID",
            justification = "Class is Serializable, but doesn't define serialVersionUID")
    private static final class CacheEntryListener
            implements CacheEventListener,
                       NotifiableEventListener<CacheService>,
                       ListenerWrapperEventFilter,
                       Serializable {

        private final transient ClientEndpoint endpoint;
        private final transient CacheAddEntryListenerMessageTask cacheAddEntryListenerMessageTask;

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
        return CacheAddEntryListenerCodec.encodeResponse((String) response);
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
