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

import com.hazelcast.cache.impl.AbstractCacheService;
import com.hazelcast.cache.impl.CachePortableHook;
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.impl.ICacheService;
import com.hazelcast.cache.impl.event.CachePartitionLostEvent;
import com.hazelcast.cache.impl.event.CachePartitionLostEventFilter;
import com.hazelcast.cache.impl.event.CachePartitionLostListener;
import com.hazelcast.cache.impl.event.InternalCachePartitionLostListenerAdapter;
import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.client.impl.client.CallableClientRequest;
import com.hazelcast.client.impl.client.RetryableRequest;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.spi.EventFilter;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.impl.PortableCachePartitionLostEvent;

import java.io.IOException;
import java.security.Permission;

public class CacheAddPartitionLostListenerRequest extends CallableClientRequest
        implements RetryableRequest {


    private String name;

    public CacheAddPartitionLostListenerRequest() {
    }

    public CacheAddPartitionLostListenerRequest(String name) {
        this.name = name;
    }

    @Override
    public Object call() {
        final ClientEndpoint endpoint = getEndpoint();

        final CachePartitionLostListener listener = new CachePartitionLostListener() {
            @Override
            public void partitionLost(CachePartitionLostEvent event) {
                if (endpoint.isAlive()) {
                    final PortableCachePartitionLostEvent portableEvent =
                            new PortableCachePartitionLostEvent(event.getPartitionId(), event.getMember().getUuid());
                    endpoint.sendEvent(null, portableEvent, getCallId());
                }
            }
        };

        final InternalCachePartitionLostListenerAdapter listenerAdapter =
                new InternalCachePartitionLostListenerAdapter(listener);
        final EventFilter filter = new CachePartitionLostEventFilter();
        final ICacheService service = getService();
        final EventRegistration registration = service.getNodeEngine().
                getEventService().registerListener(AbstractCacheService.SERVICE_NAME,
                name, filter, listenerAdapter);
        final String registrationId = registration.getId();
        endpoint.addListenerDestroyAction(CacheService.SERVICE_NAME, name, registrationId);

        return registrationId;
    }


    @Override
    public void write(PortableWriter writer) throws IOException {
        writer.writeUTF("name", name);
    }

    @Override
    public void read(PortableReader reader) throws IOException {
        name = reader.readUTF("name");
    }

    @Override
    public String getServiceName() {
        return ICacheService.SERVICE_NAME;
    }
    @Override
    public String getMethodName() {
        return "addCachePartitionLostListener";
    }

    @Override
    public int getFactoryId() {
        return CachePortableHook.F_ID;
    }

    @Override
    public int getClassId() {
        return CachePortableHook.ADD_CACHE_PARTITION_LOST_LISTENER;
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }

    @Override
    public String getDistributedObjectName() {
        return name;
    }
}
