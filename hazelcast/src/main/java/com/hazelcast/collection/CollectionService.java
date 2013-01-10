/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.collection;

import com.hazelcast.collection.multimap.ObjectMultiMapProxy;
import com.hazelcast.collection.processor.EntryProcessor;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.EntryListener;
import com.hazelcast.instance.ThreadContext;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.spi.*;

import java.util.Collection;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;

/**
 * @ali 1/1/13
 */
public class CollectionService implements ManagedService, RemoteService, EventPublishingService<CollectionEvent, EntryListener> {

    private NodeEngine nodeEngine;

    public static final String COLLECTION_SERVICE_NAME = "hz:impl:collectionService";

    private final ConcurrentMap<String, CollectionProxy> proxies = new ConcurrentHashMap<String, CollectionProxy>();
    private final ConcurrentMap<ListenerKey, String> eventRegistrations = new ConcurrentHashMap<ListenerKey, String>();

    private final CollectionPartitionContainer[] partitionContainers;

    public CollectionService(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
        partitionContainers = new CollectionPartitionContainer[nodeEngine.getPartitionCount()];
    }

    public CollectionContainer getCollectionContainer(int partitionId, String name) {
        return partitionContainers[partitionId].getCollectionContainer(name);
    }

    public CollectionContainer getOrCreateCollectionContainer(int partitionId, String name) {
        return partitionContainers[partitionId].getOrCreateCollectionContainer(name);
    }

    public CollectionPartitionContainer getPartitionContainer(int partitionId) {
        return partitionContainers[partitionId];
    }

    public void init(NodeEngine nodeEngine, Properties properties) {
        this.nodeEngine = nodeEngine;
        int partitionCount = nodeEngine.getPartitionCount();
        for (int i = 0; i < partitionCount; i++) {
            partitionContainers[i] = new CollectionPartitionContainer(this);
        }
    }

    public void destroy() {
    }

    Object createNew(String name) {
        CollectionProxy proxy = proxies.get(name);
        return proxy.createNew();
    }

    public ServiceProxy getProxy(Object... params) {
        final String name = String.valueOf(params[0]);
        CollectionProxy proxy;
        if ((proxy = proxies.get(name)) != null) {
            return proxy;
        }
        final CollectionProxyType type = (CollectionProxyType) params[1];
        boolean dataProxy = params.length > 2 && Boolean.TRUE.equals(params[2]);
        if (type.equals(CollectionProxyType.LIST)) {

        } else if (type.equals(CollectionProxyType.SET)) {

        } else if (type.equals(CollectionProxyType.MULTI_MAP)) {
            if (dataProxy) {

            } else {
                proxy = new ObjectMultiMapProxy(name, this, nodeEngine);
            }
        }
        final CollectionProxy currentProxy = proxies.putIfAbsent(name, proxy);
        return currentProxy != null ? currentProxy : proxy;
    }

    public Collection<ServiceProxy> getProxies() {
        return new HashSet<ServiceProxy>(proxies.values());
    }

    public Set<Data> localKeySet(String name) {
        Set<Data> keySet = new HashSet<Data>();
        for (CollectionPartitionContainer partitionContainer : partitionContainers) {
            CollectionContainer container = partitionContainer.getCollectionContainer(name);
            if (container != null) {
                keySet.addAll(container.keySet());
            }
        }
        return keySet;
    }

    public Set<Data> localKeySet(String name, int partitionId) {
        CollectionContainer container = partitionContainers[partitionId].getCollectionContainer(name);
        return container != null ? container.keySet() : null;
    }

    public SerializationService getSerializationService() {
        return nodeEngine.getSerializationService();
    }

    public NodeEngine getNodeEngine(){
        return nodeEngine;
    }

    public void addEntryListener(String name, EntryListener listener, Data key, boolean includeValue, boolean local) {
        ListenerKey listenerKey = new ListenerKey(name, key, listener);
        String id = eventRegistrations.putIfAbsent(listenerKey, "tempId");
        if (id != null) {
            return;
        }
        EventService eventService = nodeEngine.getEventService();
        EventRegistration registration = null;
        if (local) {
            registration = eventService.registerLocalListener(COLLECTION_SERVICE_NAME, name, new CollectionEventFilter(includeValue, key), listener);
        } else {
            registration = eventService.registerListener(COLLECTION_SERVICE_NAME, name, new CollectionEventFilter(includeValue, key), listener);
        }

        eventRegistrations.put(listenerKey, registration.getId());
    }

    public void removeEntryListener(String name, EntryListener listener, Data key) {
        ListenerKey listenerKey = new ListenerKey(name, key, listener);
        String id = eventRegistrations.remove(listenerKey);
        if (id != null) {
            EventService eventService = nodeEngine.getEventService();
            eventService.deregisterListener(COLLECTION_SERVICE_NAME, name, id);
        }
    }

    public void dispatchEvent(CollectionEvent event, EntryListener listener) {
        EntryEvent entryEvent = new EntryEvent(event.getName(), nodeEngine.getCluster().getMember(event.getCaller()),
                event.getEventType().getType(), nodeEngine.toObject(event.getKey()), nodeEngine.toObject(event.getValue()));
        if (event.eventType.equals(EntryEventType.ADDED)){
            listener.entryAdded(entryEvent);
        }
        else if (event.eventType.equals(EntryEventType.REMOVED)){
            listener.entryRemoved(entryEvent);
        }
    }

    public <T> T process(String name, Data dataKey, EntryProcessor processor) {
        try {
            int partitionId = nodeEngine.getPartitionId(dataKey);
            CollectionOperation operation = new CollectionOperation(name, dataKey, processor, partitionId);
            operation.setThreadId(ThreadContext.get().getThreadId());
            Invocation inv = nodeEngine.getOperationService().createInvocationBuilder(CollectionService.COLLECTION_SERVICE_NAME, operation, partitionId).build();
            Future f = inv.invoke();
            return (T) nodeEngine.toObject(f.get());
        } catch (Throwable throwable) {
            throw new RuntimeException(throwable);
        }
    }
}
