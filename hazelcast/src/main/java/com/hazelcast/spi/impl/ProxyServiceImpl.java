/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.impl;

import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.DistributedObjectEvent;
import com.hazelcast.core.DistributedObjectListener;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.*;
import com.hazelcast.spi.exception.DistributedObjectDestroyedException;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;
import com.hazelcast.util.executor.StripedRunnable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import static com.hazelcast.core.DistributedObjectEvent.EventType.CREATED;
import static com.hazelcast.core.DistributedObjectEvent.EventType.DESTROYED;

/**
 * @author mdogan 1/11/13
 */
public class ProxyServiceImpl implements ProxyService, EventPublishingService<DistributedObjectEvent, Object> {

    static final String SERVICE_NAME = "hz:core:proxyService";

    private final NodeEngineImpl nodeEngine;
    private final ConcurrentMap<String, ProxyRegistry> registries = new ConcurrentHashMap<String, ProxyRegistry>();
    private final ConcurrentMap<String, DistributedObjectListener> listeners
            = new ConcurrentHashMap<String, DistributedObjectListener>();
    private final ILogger logger;

    ProxyServiceImpl(NodeEngineImpl nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.logger = nodeEngine.getLogger(ProxyService.class.getName());
    }

    void init() {
        nodeEngine.getEventService().registerListener(SERVICE_NAME, SERVICE_NAME, new Object());
    }

    private final ConstructorFunction<String, ProxyRegistry> registryConstructor
            = new ConstructorFunction<String, ProxyRegistry>() {
        public ProxyRegistry createNew(String serviceName) {
            return new ProxyRegistry(serviceName);
        }
    };

    public void initializeDistributedObject(String serviceName, Object objectId) {
        if (serviceName == null) {
            throw new NullPointerException("Service name is required!");
        }
        if (objectId == null) {
            throw new NullPointerException("Object id is required!");
        }
        getDistributedObject(serviceName, objectId);
    }

    public DistributedObject getDistributedObject(String serviceName, Object objectId) {
        if (serviceName == null) {
            throw new NullPointerException("Service name is required!");
        }
        if (objectId == null) {
            throw new NullPointerException("Object id is required!");
        }
        ProxyRegistry registry = ConcurrencyUtil.getOrPutIfAbsent(registries, serviceName, registryConstructor);
        return registry.getProxy(objectId);
    }

    public void destroyDistributedObject(String serviceName, Object objectId) {
        if (serviceName == null) {
            throw new NullPointerException("Service name is required!");
        }
        if (objectId == null) {
            throw new NullPointerException("Object id is required!");
        }
        Collection<MemberImpl> members = nodeEngine.getClusterService().getMemberList();
        Collection<Future> calls = new ArrayList<Future>(members.size());
        for (MemberImpl member : members) {
            if (member.localMember()) continue;

            Invocation inv = nodeEngine.getOperationService().createInvocationBuilder(SERVICE_NAME,
                    new DistributedObjectDestroyOperation(serviceName, objectId), member.getAddress())
                    .setTryCount(10).build();
            calls.add(inv.invoke());
        }
        destroyLocalDistributedObject(serviceName, objectId);
        for (Future f : calls) {
            try {
                f.get(3, TimeUnit.SECONDS);
            } catch (Exception e) {
                logger.finest(e);
            }
        }
        ProxyRegistry registry = registries.get(serviceName);
        if (registry != null) {
            registry.destroyProxy(objectId);
        }
    }

    private void destroyLocalDistributedObject(String serviceName, Object objectId) {
        final RemoteService service = nodeEngine.getService(serviceName);
        if (service != null) {
            service.destroyDistributedObject(objectId);
        }
        nodeEngine.waitNotifyService.cancelWaitingOps(serviceName, objectId, new DistributedObjectDestroyedException(serviceName, objectId));
    }

    public Collection<DistributedObject> getDistributedObjects(String serviceName) {
        if (serviceName == null) {
            throw new NullPointerException("Service name is required!");
        }
        Collection<DistributedObject> objects = new LinkedList<DistributedObject>();
        ProxyRegistry registry = registries.get(serviceName);
        if (registry != null) {
            objects.addAll(registry.proxies.values());
        }
        return objects;
    }

    public Collection<DistributedObject> getAllDistributedObjects() {
        Collection<DistributedObject> objects = new LinkedList<DistributedObject>();
        for (ProxyRegistry registry : registries.values()) {
            objects.addAll(registry.proxies.values());
        }
        return objects;
    }

    public String addProxyListener(DistributedObjectListener distributedObjectListener) {
        final String id = UUID.randomUUID().toString();
        listeners.put(id, distributedObjectListener);
        return id;
    }

    public boolean removeProxyListener(String registrationId) {
        return listeners.remove(registrationId) != null;
    }

    public void dispatchEvent(final DistributedObjectEvent event, Object ignore) {
        final String serviceName = event.getServiceName();
        if (event.getEventType() == CREATED) {
            try {
                final ProxyRegistry registry = ConcurrencyUtil.getOrPutIfAbsent(registries, serviceName, registryConstructor);
                registry.getProxy(event.getObjectId());

                if (!registry.contains(event.getObjectId())) {
                    for (DistributedObjectListener listener : listeners.values()) {
                        listener.distributedObjectCreated(event);
                    }
                }
            } catch (HazelcastInstanceNotActiveException ignored) {
            }
        } else {
            final ProxyRegistry registry = registries.get(serviceName);
            if (registry != null) {
                registry.removeProxy(event.getObjectId());
            }
            for (DistributedObjectListener listener : listeners.values()) {
                listener.distributedObjectDestroyed(event);
            }
        }
    }

    private class ProxyRegistry {

        final String serviceName;
        final RemoteService service;
        final ConcurrentMap<Object, DistributedObject> proxies = new ConcurrentHashMap<Object, DistributedObject>();

        private ProxyRegistry(String serviceName) {
            this.serviceName = serviceName;
            this.service = nodeEngine.getService(serviceName);
            if (service == null) {
                if (nodeEngine.isActive()) {
                    throw new IllegalArgumentException("Unknown service: " + serviceName);
                } else {
                    throw new HazelcastInstanceNotActiveException();
                }
            }
        }

        DistributedObject getProxy(final Object objectId) {
            DistributedObject proxy = proxies.get(objectId);
            if (proxy == null) {
                if (!nodeEngine.isActive()) {
                    throw new HazelcastInstanceNotActiveException();
                }
                proxy = service.createDistributedObject(objectId);
                DistributedObject current = proxies.putIfAbsent(objectId, proxy);
                if (current == null) {
                    if (proxy instanceof InitializingObject) {
                        try {
                            ((InitializingObject) proxy).initialize();
                        } catch (Exception e) {
                            logger.warning("Error while initializing proxy: " + proxy, e);
                        }
                    }
                    final DistributedObjectEvent event = createEvent(objectId, CREATED);
                    publish(event);
                    nodeEngine.eventService.executeEvent(new StripedRunnable() {
                        public void run() {
                            for (DistributedObjectListener listener : listeners.values()) {
                                listener.distributedObjectCreated(event);
                            }
                        }
                        public int getKey() {
                            return objectId.hashCode();
                        }
                    });
                } else {
                    proxy = current;
                }
            }
            return proxy;
        }

        void destroyProxy(Object objectId) {
            if (proxies.remove(objectId) != null) {
                final DistributedObjectEvent event = createEvent(objectId, DESTROYED);
                publish(event);
            }
        }

        void removeProxy(Object objectId) {
            proxies.remove(objectId);
        }

        private void publish(DistributedObjectEvent event) {
            final EventService eventService = nodeEngine.getEventService();
            final Collection<EventRegistration> registrations = eventService.getRegistrations(SERVICE_NAME, SERVICE_NAME);
            eventService.publishEvent(SERVICE_NAME, registrations, event, event.getObjectId().hashCode());
        }

        private DistributedObjectEvent createEvent(Object objectId, DistributedObjectEventImpl.EventType type) {
            final DistributedObjectEventImpl event = new DistributedObjectEventImpl(type, serviceName, objectId);
            event.setHazelcastInstance(nodeEngine.getNode().hazelcastInstance);
            return event;
        }

        private boolean contains(Object objectId) {
            return proxies.containsKey(objectId);
        }

        void destroy() {
            for (DistributedObject distributedObject : proxies.values()) {
                if (distributedObject instanceof AbstractDistributedObject) {
                    DistributedObjectAccessor.onNodeShutdown((AbstractDistributedObject) distributedObject);
                }
            }
            proxies.clear();
        }
    }

    public static class DistributedObjectDestroyOperation extends AbstractOperation {

        private String serviceName;
        private Object objectId;

        public DistributedObjectDestroyOperation() {
        }

        public DistributedObjectDestroyOperation(String serviceName, Object objectId) {
            this.serviceName = serviceName;
            this.objectId = objectId;
        }

        public void run() throws Exception {
            ProxyServiceImpl proxyService = getService();
            ProxyRegistry registry = proxyService.registries.get(serviceName);
            if (registry != null) {
                registry.removeProxy(objectId);
            }
            proxyService.destroyLocalDistributedObject(serviceName, objectId);
        }

        public boolean returnsResponse() {
            return true;
        }

        public Object getResponse() {
            return Boolean.TRUE;
        }

        @Override
        protected void writeInternal(ObjectDataOutput out) throws IOException {
            super.writeInternal(out);
            out.writeUTF(serviceName);
            out.writeObject(objectId);
        }

        @Override
        protected void readInternal(ObjectDataInput in) throws IOException {
            super.readInternal(in);
            serviceName = in.readUTF();
            objectId = in.readObject();
        }
    }

    void shutdown() {
        for (ProxyRegistry registry : registries.values()) {
            registry.destroy();
        }
        registries.clear();
        listeners.clear();
    }
}
