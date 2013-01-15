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

package com.hazelcast.spi.impl;

import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.DistributedObjectEvent;
import com.hazelcast.core.DistributedObjectListener;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.concurrent.*;
import java.util.logging.Level;

import static com.hazelcast.core.DistributedObjectEvent.EventType.CREATED;
import static com.hazelcast.core.DistributedObjectEvent.EventType.DESTROYED;

/**
 * @mdogan 1/11/13
 */
public class ProxyServiceImpl implements ProxyService, EventPublishingService<DistributedObjectEvent, Object> {

    static final String NAME = "hz:core:proxyService";

    private final NodeEngineImpl nodeEngine;
    private final ConcurrentMap<String, ProxyRegistry> registries = new ConcurrentHashMap<String, ProxyRegistry>();
    private final Collection<DistributedObjectListener> listeners
            = Collections.newSetFromMap(new ConcurrentHashMap<DistributedObjectListener, Boolean>());
    private final ILogger logger;

    ProxyServiceImpl(NodeEngineImpl nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.logger = nodeEngine.getLogger(ProxyService.class.getName());
    }

    void init() {
        nodeEngine.getEventService().registerListener(NAME, NAME, new Object());
    }

    public ServiceProxy getProxy(String serviceName, Object proxyId) {
        ProxyRegistry registry = registries.get(serviceName);
        if (registry == null) {
            registry = new ProxyRegistry(serviceName);
            ProxyRegistry current = registries.putIfAbsent(serviceName, registry);
            registry = current != null ? current : registry;
        }
        return registry.getProxy(proxyId);
    }

    public ServiceProxy getProxy(Class<? extends RemoteService> serviceClass, Object proxyId) {
        Collection services = nodeEngine.serviceManager.getServices(serviceClass);
        for (Object service : services) {
            if (serviceClass.isAssignableFrom(service.getClass())) {
                return getProxy(((RemoteService) service).getServiceName(), proxyId);
            }
        }
        throw new IllegalArgumentException();
    }

    public void destroyProxy(String serviceName, Object proxyId) {
        Collection<MemberImpl> members = nodeEngine.getClusterService().getMemberList();
        Collection<Future> calls = new ArrayList<Future>(members.size());
        for (MemberImpl member : members) {
            if (member.localMember()) continue;

            Invocation inv = nodeEngine.getOperationService().createInvocationBuilder(NAME,
                    new DistributedObjectDestroyOperation(serviceName, proxyId), member.getAddress())
                    .setTryCount(10).build();
            calls.add(inv.invoke());
        }
        destroyDistributedObject(serviceName, proxyId);
        for (Future f : calls) {
            try {
                f.get(3, TimeUnit.SECONDS);
            } catch (Exception e) {
                logger.log(Level.FINEST, e.getMessage(), e);
            }
        }
        ProxyRegistry registry = registries.get(serviceName);
        if (registry != null) {
            registry.destroyProxy(proxyId);
        }
    }

    public Collection<DistributedObject> getProxies(String serviceName) {
        Collection<DistributedObject> objects = new LinkedList<DistributedObject>();
        ProxyRegistry registry = registries.get(serviceName);
        if (registry != null) {
            objects.addAll(registry.proxies.values());
        }
        return objects;
    }

    public Collection<DistributedObject> getAllProxies() {
        Collection<DistributedObject> objects = new LinkedList<DistributedObject>();
        for (ProxyRegistry registry : registries.values()) {
            objects.addAll(registry.proxies.values());
        }
        return objects;
    }

    public void addProxyListener(DistributedObjectListener distributedObjectListener) {
        listeners.add(distributedObjectListener);
    }

    public void removeProxyListener(DistributedObjectListener distributedObjectListener) {
        listeners.remove(distributedObjectListener);
    }

    public void dispatchEvent(final DistributedObjectEvent event, Object ignore) {
        final String serviceName = event.getServiceName();
        final ProxyRegistry registry = registries.get(serviceName);
        if (event.getEventType() == CREATED) {
            if (registry == null || !registry.contains(event.getObjectId())) {
                for (DistributedObjectListener listener : listeners) {
                    listener.distributedObjectCreated(event);
                }
            }
        } else {
            if (registry != null) {
                registry.removeProxy(event.getObjectId());
            }
            for (DistributedObjectListener listener : listeners) {
                listener.distributedObjectDestroyed(event);
            }
        }
    }

    private void destroyDistributedObject(String serviceName, Object objectId) {
        final RemoteService service = nodeEngine.serviceManager.getService(serviceName);
        if (service != null) {
            service.destroyDistributedObject(objectId);
        }
    }

    private class ProxyRegistry {

        final RemoteService service;

        final ConcurrentMap<Object, ServiceProxy> proxies = new ConcurrentHashMap<Object, ServiceProxy>();

        private ProxyRegistry(String serviceName) {
            this.service = nodeEngine.serviceManager.getService(serviceName);
            if (service == null) {
                throw new IllegalArgumentException("Unknown service: " + serviceName);
            }
        }

        ServiceProxy getProxy(Object proxyId) {
            ServiceProxy proxy = proxies.get(proxyId);
            if (proxy == null) {
                proxy = service.createProxy(proxyId);
                ServiceProxy current = proxies.putIfAbsent(proxyId, proxy);
                if (current == null) {
                    final DistributedObjectEvent event = createEvent(proxyId, CREATED);
                    publish(event);
                    nodeEngine.getEventService().executeEvent(new Runnable() {
                        public void run() {
                            for (DistributedObjectListener listener : listeners) {
                                listener.distributedObjectCreated(event);
                            }
                        }
                    });
                } else {
                    proxy = current;
                }
            }
            return proxy;
        }

        void destroyProxy(Object proxyId) {
            if (proxies.remove(proxyId) != null) {
                final DistributedObjectEvent event = createEvent(proxyId, DESTROYED);
                publish(event);
                nodeEngine.getEventService().executeEvent(new Runnable() {
                    public void run() {
                        for (DistributedObjectListener listener : listeners) {
                            listener.distributedObjectDestroyed(event);
                        }
                    }
                });
            }
        }

        void removeProxy(Object proxyId) {
            proxies.remove(proxyId);
        }

        private void publish(DistributedObjectEvent event) {
            final EventService eventService = nodeEngine.getEventService();
            final Collection<EventRegistration> registrations = eventService.getRegistrations(NAME, NAME);
            eventService.publishEvent(NAME, registrations, event);
        }

        private DistributedObjectEvent createEvent(Object proxyId, DistributedObjectEvent.EventType type) {
            final DistributedObjectEvent event = new DistributedObjectEvent(type, service.getServiceName(), proxyId);
            event.setHazelcastInstance(nodeEngine.getNode().hazelcastInstance);
            return event;
        }

        private boolean contains(Object objectId) {
            return proxies.containsKey(objectId);
        }

        void destroy() {
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
            proxyService.destroyDistributedObject(serviceName, objectId);
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
