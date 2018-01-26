/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.impl.proxyservice.impl;

import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.InitializingObject;
import com.hazelcast.spi.RemoteService;
import com.hazelcast.spi.impl.eventservice.InternalEventService;
import com.hazelcast.util.EmptyStatement;
import com.hazelcast.util.ExceptionUtil;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.core.DistributedObjectEvent.EventType.CREATED;
import static com.hazelcast.core.DistributedObjectEvent.EventType.DESTROYED;

/**
 * A ProxyRegistry contains all proxies for a given service. For example, it contains all proxies for the IMap.
 */
public final class ProxyRegistry {

    private final ProxyServiceImpl proxyService;
    private final String serviceName;
    private final RemoteService service;
    private final ConcurrentMap<String, DistributedObjectFuture> proxies
            = new ConcurrentHashMap<String, DistributedObjectFuture>();

    ProxyRegistry(ProxyServiceImpl proxyService, String serviceName) {
        this.proxyService = proxyService;
        this.serviceName = serviceName;
        this.service = proxyService.nodeEngine.getService(serviceName);
    }

    /**
     * Returns the name of the service for this ProxyRegistry.
     *
     * @return The name of the service for this ProxyRegistry.
     */
    public String getServiceName() {
        return serviceName;
    }

    /**
     * Returns the number of proxies for this ProxyRegistry.
     *
     * @return The number of proxies for this ProxyRegistry.
     */
    public int getProxyCount() {
        return proxies.size();
    }

    /**
     * Checks if the ProxyRegistry contains the given key.
     *
     * @param name name of the key
     * @return true if the ProxyRegistry contains the key, false otherwise.
     */
    boolean contains(String name) {
        return proxies.containsKey(name);
    }

    public Collection<String> getDistributedObjectNames() {
        // todo: not happy with exposing an internal data-structure to the outside.
        return proxies.keySet();
    }

    /**
     * Gets the ProxyInfo of all fully initialized proxies in this registry.
     * The result is written into 'result'.
     *
     * @param result The ProxyInfo of all proxies in this registry.
     */
    public void getProxyInfos(Collection<ProxyInfo> result) {
        for (Map.Entry<String, DistributedObjectFuture> entry : proxies.entrySet()) {
            DistributedObjectFuture future = entry.getValue();
            if (future.isSetAndInitialized()) {
                String proxyName = entry.getKey();
                result.add(new ProxyInfo(serviceName, proxyName));
            }
        }
    }

    /**
     * Gets the DistributedObjects in this registry. The result is written into 'result'.
     *
     * @param result The DistributedObjects in this registry.
     */
    public void getDistributedObjects(Collection<DistributedObject> result) {
        Collection<DistributedObjectFuture> futures = proxies.values();
        for (DistributedObjectFuture future : futures) {
            if (!future.isSetAndInitialized()) {
                continue;
            }
            try {
                DistributedObject object = future.get();
                result.add(object);
            } catch (Throwable ignored) {
                // ignore if proxy creation failed
                EmptyStatement.ignore(ignored);
            }
        }
    }

    /**
     * Retrieves a DistributedObject proxy or creates it if it is not available.
     * DistributedObject will be initialized by calling {@link InitializingObject#initialize()},
     * if it implements {@link InitializingObject}.
     *
     * @param name         The name of the DistributedObject proxy object to retrieve or create.
     * @param publishEvent true if a DistributedObjectEvent should be fired.
     * @return The DistributedObject instance.
     */
    public DistributedObject getOrCreateProxy(String name, boolean publishEvent) {
        DistributedObjectFuture proxyFuture = getOrCreateProxyFuture(name, publishEvent, true);
        return proxyFuture.get();
    }

    /**
     * Retrieves a DistributedObjectFuture or creates it if it is not available.
     * If {@code initialize} is false and DistributedObject implements {@link InitializingObject},
     * {@link InitializingObject#initialize()} will be called before {@link DistributedObjectFuture#get()} returns.
     *
     * @param name         The name of the DistributedObject proxy object to retrieve or create.
     * @param publishEvent true if a DistributedObjectEvent should be fired.
     * @param initialize   true if the DistributedObject proxy object should be initialized.
     */
    public DistributedObjectFuture getOrCreateProxyFuture(String name, boolean publishEvent, boolean initialize) {
        DistributedObjectFuture proxyFuture = proxies.get(name);
        if (proxyFuture == null) {
            if (!proxyService.nodeEngine.isRunning()) {
                throw new HazelcastInstanceNotActiveException();
            }
            proxyFuture = createProxy(name, publishEvent, initialize);
            if (proxyFuture == null) {
                return getOrCreateProxyFuture(name, publishEvent, initialize);
            }
        }
        return proxyFuture;
    }

    /**
     * Creates a DistributedObject proxy if it is not created yet
     *
     * @param name         The name of the distributedObject proxy object.
     * @param publishEvent true if a DistributedObjectEvent should be fired.
     * @param initialize   true if he DistributedObject proxy object should be initialized.
     * @return The DistributedObject instance if it is created by this method, null otherwise.
     */
    public DistributedObjectFuture createProxy(String name, boolean publishEvent, boolean initialize) {
        if (proxies.containsKey(name)) {
            return null;
        }

        if (!proxyService.nodeEngine.isRunning()) {
            throw new HazelcastInstanceNotActiveException();
        }

        DistributedObjectFuture proxyFuture = new DistributedObjectFuture();
        if (proxies.putIfAbsent(name, proxyFuture) != null) {
            return null;
        }

        return doCreateProxy(name, publishEvent, initialize, proxyFuture);
    }

    private DistributedObjectFuture doCreateProxy(String name, boolean publishEvent, boolean initialize,
                                                  DistributedObjectFuture proxyFuture) {
        DistributedObject proxy;
        try {
            proxy = service.createDistributedObject(name);
            if (initialize && proxy instanceof InitializingObject) {
                try {
                    ((InitializingObject) proxy).initialize();
                } catch (Exception e) {
                    // log and throw exception to be handled in outer catch block
                    proxyService.logger.warning("Error while initializing proxy: " + proxy, e);
                    throw e;
                }
            }
            proxyFuture.set(proxy, initialize);
        } catch (Throwable e) {
            // proxy creation or initialization failed
            // deregister future to avoid infinite hang on future.get()
            proxyFuture.setError(e);
            proxies.remove(name);
            throw ExceptionUtil.rethrow(e);
        }

        InternalEventService eventService = proxyService.nodeEngine.getEventService();
        ProxyEventProcessor callback = new ProxyEventProcessor(proxyService.listeners.values(), CREATED, serviceName,
                name, proxy);
        eventService.executeEventCallback(callback);
        if (publishEvent) {
            publish(new DistributedObjectEventPacket(CREATED, serviceName, name));
        }
        return proxyFuture;
    }

    /**
     * Destroys a proxy.
     *
     * @param name         The name of the proxy to destroy.
     * @param publishEvent true if this destroy should be published.
     */
    void destroyProxy(String name, boolean publishEvent) {
        final DistributedObjectFuture proxyFuture = proxies.remove(name);
        if (proxyFuture == null) {
            return;
        }

        DistributedObject proxy;
        try {
            proxy = proxyFuture.get();
        } catch (Throwable t) {
            proxyService.logger.warning("Cannot destroy proxy [" + serviceName + ":" + name
                    + "], since its creation is failed with "
                    + t.getClass().getName() + ": " + t.getMessage());
            return;
        }
        InternalEventService eventService = proxyService.nodeEngine.getEventService();
        ProxyEventProcessor callback = new ProxyEventProcessor(proxyService.listeners.values(), DESTROYED, serviceName,
                name, proxy);
        eventService.executeEventCallback(callback);
        if (publishEvent) {
            publish(new DistributedObjectEventPacket(DESTROYED, serviceName, name));
        }
    }

    private void publish(DistributedObjectEventPacket event) {
        EventService eventService = proxyService.nodeEngine.getEventService();
        Collection<EventRegistration> registrations = eventService.getRegistrations(
                ProxyServiceImpl.SERVICE_NAME, ProxyServiceImpl.SERVICE_NAME);
        eventService.publishRemoteEvent(ProxyServiceImpl.SERVICE_NAME, registrations, event, event.getName().hashCode());
    }

    /**
     * Destroys this proxy registry.
     */
    void destroy() {
        for (DistributedObjectFuture future : proxies.values()) {
            if (!future.isSetAndInitialized()) {
                continue;
            }
            DistributedObject distributedObject = future.get();
            if (distributedObject instanceof AbstractDistributedObject) {
                ((AbstractDistributedObject) distributedObject).invalidate();
            }
        }
        proxies.clear();
    }
}
