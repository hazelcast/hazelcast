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
import com.hazelcast.spi.RemoteService;
import com.hazelcast.spi.ServiceProxy;

import java.util.Collection;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @mdogan 1/11/13
 */
public class ProxyService {

    private final ServiceManager serviceManager;
    private final ConcurrentMap<String, ProxyRegistry> registries = new ConcurrentHashMap<String, ProxyRegistry>();

    ProxyService(ServiceManager serviceManager) {
        this.serviceManager = serviceManager;
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
        Collection services = serviceManager.getServices(serviceClass);
        for (Object service : services) {
            if (serviceClass.isAssignableFrom(service.getClass())) {
                return getProxy(((RemoteService) service).getServiceName(), proxyId);
            }
        }
        throw new IllegalArgumentException();
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

    private class ProxyRegistry {

        final RemoteService service;

        final ConcurrentMap<Object, ServiceProxy> proxies = new ConcurrentHashMap<Object, ServiceProxy>();

        private ProxyRegistry(String serviceName) {
            this.service = serviceManager.getService(serviceName);
            if (service == null) {
                throw new IllegalArgumentException("Unknown service: " + serviceName);
            }
        }

        public ServiceProxy getProxy(Object proxyId) {
            ServiceProxy proxy = proxies.get(proxyId);
            if (proxy == null) {
                proxy = service.createProxy(proxyId);
                ServiceProxy current = proxies.putIfAbsent(proxyId, proxy);
                proxy = current != null ? current : proxy;
            }
            return proxy;
        }
    }
}
