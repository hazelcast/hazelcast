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

package com.hazelcast.cache.impl.client;

import com.hazelcast.cache.impl.CacheContext;
import com.hazelcast.cache.impl.CacheEventData;
import com.hazelcast.cache.impl.CacheEventListener;
import com.hazelcast.cache.impl.CacheEventSet;
import com.hazelcast.cache.impl.CachePortableHook;
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.client.impl.client.CallableClientRequest;
import com.hazelcast.client.impl.client.RetryableRequest;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.impl.HeapData;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.ListenerWrapperEventFilter;
import com.hazelcast.spi.NotifiableEventListener;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.io.Serializable;
import java.security.Permission;
import java.util.Set;

/**
 * Client request which registers an event listener on behalf of the client and delegates the received events
 * back to client.
 *
 * @see com.hazelcast.cache.impl.CacheService#registerListener(String, com.hazelcast.cache.impl.CacheEventListener)
 */
public class CacheAddEntryListenerRequest
        extends CallableClientRequest
        implements RetryableRequest {

    private String name;

    public CacheAddEntryListenerRequest() {
    }

    public CacheAddEntryListenerRequest(String name) {
        this.name = name;
    }

    @Override
    public Object call() {
        final ClientEndpoint endpoint = getEndpoint();
        final CacheService service = getService();
        CacheEntryListener cacheEntryListener = new CacheEntryListener(getCallId(), endpoint);
        return service.registerListener(name, cacheEntryListener, cacheEntryListener);
    }

    @SuppressFBWarnings(value = "SE_NO_SERIALVERSIONID",
            justification = "Class is Serializable, but doesn't define serialVersionUID")
    private static final class CacheEntryListener
            implements CacheEventListener,
                       NotifiableEventListener<CacheService>,
                       ListenerWrapperEventFilter,
                       Serializable {

        private final int callId;
        private final transient ClientEndpoint endpoint;

        private CacheEntryListener(int callId, ClientEndpoint endpoint) {
            this.callId = callId;
            this.endpoint = endpoint;
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
            if (endpoint.isAlive()) {
                Data partitionKey = getPartitionKey(eventObject);
                endpoint.sendEvent(partitionKey, eventObject, callId);
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
    public String getServiceName() {
        return CacheService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return CachePortableHook.F_ID;
    }

    @Override
    public int getClassId() {
        return CachePortableHook.ADD_ENTRY_LISTENER;
    }

    @Override
    public void write(PortableWriter writer)
            throws IOException {
        super.write(writer);
        writer.writeUTF("n", name);
    }

    @Override
    public void read(PortableReader reader)
            throws IOException {
        super.read(reader);
        name = reader.readUTF("n");
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }

}
