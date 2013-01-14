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
import com.hazelcast.spi.*;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
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

    ProxyServiceImpl(NodeEngineImpl nodeEngine) {
        this.nodeEngine = nodeEngine;
    }

    void init() {
        nodeEngine.getEventService().registerListener(NAME, NAME, this);
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
        nodeEngine.getExecutionService().execute(new Runnable() {
            public void run() {
                final String serviceName = event.getServiceName();
                final RemoteService service = nodeEngine.serviceManager.getService(serviceName);
                if (service == null) {
                    nodeEngine.getLogger(ProxyServiceImpl.class.getName())
                            .log(Level.WARNING, "Service: " + serviceName + " could not be found!");
                    return;
                }
                if (event.getEventType() == CREATED) {
                    service.onProxyCreate(event.getObjectId());
                } else {
                    service.onProxyDestroy(event.getObjectId());
                }
            }
        });
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

        void destroy() {
            proxies.clear();
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
