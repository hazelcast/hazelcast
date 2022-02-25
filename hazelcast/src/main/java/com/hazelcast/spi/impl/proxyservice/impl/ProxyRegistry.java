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

package com.hazelcast.spi.impl.proxyservice.impl;

import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.internal.services.RemoteService;
import com.hazelcast.internal.services.TenantContextAwareService;
import com.hazelcast.internal.util.EmptyStatement;
import com.hazelcast.internal.util.SetUtil;
import com.hazelcast.internal.util.counters.MwCounter;
import com.hazelcast.spi.exception.DistributedObjectDestroyedException;
import com.hazelcast.spi.impl.AbstractDistributedObject;
import com.hazelcast.spi.impl.InitializingObject;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.eventservice.EventRegistration;
import com.hazelcast.spi.impl.eventservice.EventService;
import com.hazelcast.spi.impl.tenantcontrol.impl.TenantControlServiceImpl;
import com.hazelcast.spi.tenantcontrol.TenantControl;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.core.DistributedObjectEvent.EventType.CREATED;
import static com.hazelcast.core.DistributedObjectEvent.EventType.DESTROYED;
import static com.hazelcast.internal.util.EmptyStatement.ignore;
import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static com.hazelcast.internal.util.counters.MwCounter.newMwCounter;
import static com.hazelcast.jet.impl.JobRepository.INTERNAL_JET_OBJECTS_PREFIX;

/**
 * A ProxyRegistry contains all proxies for a given service. For example, it contains all proxies for the IMap.
 */
public final class ProxyRegistry {

    public static final Set<String> INTERNAL_OBJECTS_PREFIXES;

    static {
        INTERNAL_OBJECTS_PREFIXES = SetUtil.createHashSet(3);
        INTERNAL_OBJECTS_PREFIXES.add(INTERNAL_JET_OBJECTS_PREFIX);
        INTERNAL_OBJECTS_PREFIXES.add("__mc.");
        INTERNAL_OBJECTS_PREFIXES.add("__sql.");
    }

    private final ProxyServiceImpl proxyService;
    private final String serviceName;
    private final RemoteService service;
    private final ConcurrentMap<String, DistributedObjectFuture> proxies = new ConcurrentHashMap<>();
    private final MwCounter createdCounter = newMwCounter();

    ProxyRegistry(ProxyServiceImpl proxyService, String serviceName) {
        this.proxyService = proxyService;
        this.serviceName = serviceName;
        this.service = getService(proxyService.nodeEngine, serviceName);
    }

    /**
     * Returns the service for the given {@code serviceName} or throws an
     * exception. This method never returns {@code null}.
     *
     * @param nodeEngine  the node engine
     * @param serviceName the remote service name
     * @return the service instance
     * @throws HazelcastException                  if there is no service with the given name
     * @throws HazelcastInstanceNotActiveException if this instance is shutting down
     */
    private RemoteService getService(NodeEngineImpl nodeEngine, String serviceName) {
        try {
            return nodeEngine.getService(serviceName);
        } catch (HazelcastException e) {
            if (!nodeEngine.isRunning()) {
                throw new HazelcastInstanceNotActiveException(e.getMessage());
            } else {
                throw e;
            }
        }
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
        return proxies.keySet();
    }

    public boolean existsDistributedObject(String name) {
        return proxies.containsKey(name);
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
                result.add(new ProxyInfo(serviceName, proxyName, future.getSource()));
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
                ignore(ignored);
            }
        }
    }

    /**
     * Retrieves a DistributedObject proxy or creates it if it is not available.
     * DistributedObject will be initialized by calling {@link InitializingObject#initialize()},
     * if it implements {@link InitializingObject}.
     *
     * @param name         The name of the DistributedObject proxy object to retrieve or create.
     * @param source       The UUID of the client or member which caused this call.
     * @param publishEvent true if a DistributedObjectEvent should be fired.
     * @return The DistributedObject instance.
     */
    public DistributedObject getOrCreateProxy(String name, UUID source, boolean publishEvent) {
        DistributedObjectFuture proxyFuture = getOrCreateProxyFuture(name, source, publishEvent, true);
        return proxyFuture.get();
    }

    /**
     * Retrieves a DistributedObjectFuture or creates it if it is not available.
     * If {@code initialize} is false and DistributedObject implements {@link InitializingObject},
     * {@link InitializingObject#initialize()} will be called before {@link DistributedObjectFuture#get()} returns.
     *
     * @param name         The name of the DistributedObject proxy object to retrieve or create.
     * @param source       The UUID of the client or member which caused this call.
     * @param publishEvent true if a DistributedObjectEvent should be fired.
     * @param initialize   true if the DistributedObject proxy object should be initialized.
     */
    public DistributedObjectFuture getOrCreateProxyFuture(String name, UUID source, boolean publishEvent, boolean initialize) {
        DistributedObjectFuture proxyFuture = proxies.get(name);
        if (proxyFuture == null) {
            if (!proxyService.nodeEngine.isRunning()) {
                throw new HazelcastInstanceNotActiveException();
            }
            proxyFuture = createProxy(name, source, initialize, !publishEvent);
            if (proxyFuture == null) {
                return getOrCreateProxyFuture(name, source, publishEvent, initialize);
            }
        }
        return proxyFuture;
    }

    /**
     * Creates a DistributedObject proxy if it is not created yet
     *
     * @param name       The name of the distributedObject proxy object.
     * @param source     The UUID of the client or member which initialized createProxy.
     * @param initialize true if he DistributedObject proxy object should be initialized.
     * @param local      {@code true} if the proxy should be only created on the local member,
     *                   otherwise fires {@code DistributedObjectEvent} to trigger cluster-wide
     *                   proxy creation.
     * @return The DistributedObject instance if it is created by this method, null otherwise.
     */
    public DistributedObjectFuture createProxy(String name, UUID source, boolean initialize, boolean local) {
        if (proxies.containsKey(name)) {
            return null;
        }

        if (!proxyService.nodeEngine.isRunning()) {
            throw new HazelcastInstanceNotActiveException();
        }

        DistributedObjectFuture proxyFuture = new DistributedObjectFuture(source);
        if (proxies.putIfAbsent(name, proxyFuture) != null) {
            return null;
        }

        return doCreateProxy(name, source, initialize, proxyFuture, local);
    }

    private DistributedObjectFuture doCreateProxy(String name, UUID source, boolean initialize,
                                                  DistributedObjectFuture proxyFuture, boolean local) {
        boolean publishEvent = !local;
        DistributedObject proxy;
        try {
            TenantControlServiceImpl tenantControlService = proxyService.nodeEngine.getTenantControlService();
            TenantControl tenantControl = tenantControlService.getTenantControl(serviceName, name);

            if (tenantControl == null) {
                if (initialize && service instanceof TenantContextAwareService) {
                    try {

                        tenantControl = tenantControlService.initializeTenantControl(serviceName, name);
                    } catch (Exception e) {
                        // log and throw exception to be handled in outer catch block
                        proxyService.logger.warning("Error while initializing tenant control for service '"
                                + serviceName + "' and object '" + name + "'", e);
                        throw e;
                    }
                } else {
                    tenantControl = TenantControl.NOOP_TENANT_CONTROL;
                }
            }
            proxy = service.createDistributedObject(name, source, local);
            tenantControl.registerObject(proxy.getDestroyContextForTenant());

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
            if (INTERNAL_OBJECTS_PREFIXES.stream().noneMatch(name::startsWith)) {
                createdCounter.inc();
            }
        } catch (Throwable e) {
            // proxy creation or initialization failed
            // deregister future to avoid infinite hang on future.get()
            proxyFuture.setError(e);
            proxies.remove(name);
            throw rethrow(e);
        }

        EventService eventService = proxyService.nodeEngine.getEventService();
        ProxyEventProcessor callback = new ProxyEventProcessor(proxyService.listeners.values(), CREATED, serviceName,
                name, proxy, source);
        eventService.executeEventCallback(callback);
        if (publishEvent) {
            publish(new DistributedObjectEventPacket(CREATED, serviceName, name, source));
        }
        return proxyFuture;
    }

    /**
     * Destroys a proxy.
     *
     * @param name         The name of the proxy to destroy.
     * @param source       The UUID of the client or member which initialized destroyProxy.
     * @param publishEvent true if this destroy should be published.
     */
    void destroyProxy(String name, UUID source, boolean publishEvent) {
        final DistributedObjectFuture proxyFuture = proxies.remove(name);
        if (proxyFuture == null) {
            return;
        }

        DistributedObject proxy;
        try {
            proxy = proxyFuture.getNow();
        } catch (Throwable t) {
            proxyService.logger.warning("Cannot destroy proxy [" + serviceName + ":" + name
                    + "], since its creation is failed with "
                    + t.getClass().getName() + ": " + t.getMessage());
            return;
        }
        if (proxy == null) {
            // complete exceptionally the proxy future
            try {
                proxyFuture.setError(new DistributedObjectDestroyedException("Proxy [" + serviceName + ":" + name + "] "
                        + "was destroyed while being created. This may result in incomplete cleanup of resources."));
            } catch (IllegalStateException e) {
                // proxy was set and initialized in the meanwhile
                proxy = proxyFuture.get();
            }
        }
        if (proxy != null) {
            EventService eventService = proxyService.nodeEngine.getEventService();
            ProxyEventProcessor callback = new ProxyEventProcessor(proxyService.listeners.values(), DESTROYED, serviceName, name,
                    proxy, source);
            eventService.executeEventCallback(callback);
        }
        if (publishEvent) {
            publish(new DistributedObjectEventPacket(DESTROYED, serviceName, name, source));
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

            DistributedObject distributedObject = extractDistributedObject(future);
            invalidate(distributedObject);
        }
        proxies.clear();
    }

    private DistributedObject extractDistributedObject(DistributedObjectFuture future) {
        try {
            return future.get();
        } catch (Throwable ex) {
            EmptyStatement.ignore(ex);
        }
        return null;
    }

    private void invalidate(DistributedObject distributedObject) {
        if (distributedObject != null
                && distributedObject instanceof AbstractDistributedObject) {
            ((AbstractDistributedObject) distributedObject).invalidate();
        }
    }

    /**
     * Force-initializes and publishes all uninitialized proxies in this registry.
     * <p>
     * This method assumes that the uninitialized proxies originate from
     * {@code doCreateProxy(publishEvent=false, initialize=false, ...)}. Since local
     * events were already emitted for such proxies, only remote events are published
     * by this method.
     * <p>
     * Calling this method concurrently with
     * {@code doCreateProxy(publishEvent=true)} may result in publishing the remote
     * events multiple times for some proxies.
     *
     * @param publishAfterInitialization whether to publish distributed object
     *                                   created event after the proxy initialization
     */
    void initializeProxies(boolean publishAfterInitialization) {
        for (Map.Entry<String, DistributedObjectFuture> entry : proxies.entrySet()) {
            String name = entry.getKey();
            DistributedObjectFuture future = entry.getValue();
            if (future.isSetAndInitialized()) {
                continue;
            }
            initializeProxy(name, future);
            if (publishAfterInitialization) {
                UUID source = proxyService.nodeEngine.getLocalMember().getUuid();
                publish(new DistributedObjectEventPacket(CREATED, serviceName, name, source));
            }
        }
    }

    private void initializeProxy(String name, DistributedObjectFuture future) {
        try {
            future.get();
        } catch (Throwable e) {
            // proxy initialization failed
            // deregister future to avoid infinite hang on future.get()
            proxyService.logger.warning("Error while initializing proxy: " + name, e);
            future.setError(e);
            proxies.remove(name);
            throw rethrow(e);
        }
    }

    /**
     * Returns the total number of created non-internal proxies, even if some have already
     * been destroyed.
     *
     * @return the total count of created non-internal proxies
     */
    public long getCreatedCount() {
        return createdCounter.get();
    }
}
