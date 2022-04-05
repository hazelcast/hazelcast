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

import com.hazelcast.cluster.Member;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.DistributedObjectListener;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.metrics.StaticMetricsProvider;
import com.hazelcast.internal.services.PostJoinAwareService;
import com.hazelcast.internal.services.RemoteService;
import com.hazelcast.internal.util.ConstructorFunction;
import com.hazelcast.internal.util.FutureUtil.ExceptionHandler;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.internal.util.counters.MwCounter;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.exception.DistributedObjectDestroyedException;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.eventservice.EventPublishingService;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.spi.impl.proxyservice.InternalProxyService;
import com.hazelcast.spi.impl.proxyservice.ProxyService;
import com.hazelcast.spi.impl.proxyservice.impl.operations.DistributedObjectDestroyOperation;
import com.hazelcast.spi.impl.proxyservice.impl.operations.PostJoinProxyOperation;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import static com.hazelcast.core.DistributedObjectEvent.EventType.CREATED;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.PROXY_METRIC_CREATED_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.PROXY_METRIC_DESTROYED_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.PROXY_METRIC_PROXY_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.PROXY_PREFIX;
import static com.hazelcast.internal.metrics.ProbeLevel.MANDATORY;
import static com.hazelcast.internal.util.ConcurrencyUtil.getOrPutIfAbsent;
import static com.hazelcast.internal.util.EmptyStatement.ignore;
import static com.hazelcast.internal.util.ExceptionUtil.peel;
import static com.hazelcast.internal.util.FutureUtil.waitWithDeadline;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.counters.MwCounter.newMwCounter;
import static java.util.logging.Level.FINEST;
import static java.util.logging.Level.WARNING;

public class ProxyServiceImpl
        implements InternalProxyService, PostJoinAwareService,
        EventPublishingService<DistributedObjectEventPacket, Object>, StaticMetricsProvider {

    public static final String SERVICE_NAME = "hz:core:proxyService";

    private static final int TRY_COUNT = 10;
    private static final long DESTROY_TIMEOUT_SECONDS = 30;

    final NodeEngineImpl nodeEngine;
    final ILogger logger;
    final ConcurrentMap<UUID, DistributedObjectListener> listeners = new ConcurrentHashMap<>();

    private final ConstructorFunction<String, ProxyRegistry> registryConstructor =
            serviceName -> new ProxyRegistry(ProxyServiceImpl.this, serviceName);

    private final ConcurrentMap<String, ProxyRegistry> registries = new ConcurrentHashMap<>();

    @Probe(name = PROXY_METRIC_CREATED_COUNT, level = MANDATORY)
    private final MwCounter createdCounter = newMwCounter();
    @Probe(name = PROXY_METRIC_DESTROYED_COUNT, level = MANDATORY)
    private final MwCounter destroyedCounter = newMwCounter();

    private final ExceptionHandler destroyProxyExceptionHandler = new ExceptionHandler() {
        @Override
        public void handleException(Throwable throwable) {
            boolean causedByInactiveInstance = peel(throwable) instanceof HazelcastInstanceNotActiveException;
            Level level = causedByInactiveInstance ? FINEST : WARNING;
            logger.log(level, "Error while destroying a proxy.", throwable);
        }
    };

    public ProxyServiceImpl(NodeEngineImpl nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.logger = nodeEngine.getLogger(ProxyService.class.getName());
    }

    @Override
    public void provideStaticMetrics(MetricsRegistry registry) {
        registry.registerStaticMetrics(this, PROXY_PREFIX);
    }

    public void init() {
        nodeEngine.getEventService().registerListener(SERVICE_NAME, SERVICE_NAME, new Object());
    }

    @Probe(name = PROXY_METRIC_PROXY_COUNT)
    @Override
    public int getProxyCount() {
        int count = 0;
        for (ProxyRegistry registry : registries.values()) {
            count += registry.getProxyCount();
        }

        return count;
    }

    public void initializeProxies(boolean publishAfterInitialization) {
        for (ProxyRegistry registry : registries.values()) {
            registry.initializeProxies(publishAfterInitialization);
        }
    }

    @Override
    public void initializeDistributedObject(String serviceName, String name, UUID source) {
        checkServiceNameNotNull(serviceName);
        checkObjectNameNotNull(name);

        ProxyRegistry registry = getOrCreateRegistry(serviceName);
        registry.createProxy(name, source, true, false);
        createdCounter.inc();
    }

    public ProxyRegistry getOrCreateRegistry(String serviceName) {
        return getOrPutIfAbsent(registries, serviceName, registryConstructor);
    }

    @Override
    public DistributedObject getDistributedObject(String serviceName, String name, UUID source) {
        checkServiceNameNotNull(serviceName);
        checkObjectNameNotNull(name);

        ProxyRegistry registry = getOrCreateRegistry(serviceName);
        return registry.getOrCreateProxy(name, source, true);
    }

    @Override
    public void destroyDistributedObject(String serviceName, String name, UUID source) {
        checkServiceNameNotNull(serviceName);
        checkObjectNameNotNull(name);

        OperationService operationService = nodeEngine.getOperationService();
        Collection<Member> members = nodeEngine.getClusterService().getMembers();
        Collection<Future<Boolean>> calls = new ArrayList<>(members.size());
        for (Member member : members) {
            if (member.localMember()) {
                continue;
            }

            DistributedObjectDestroyOperation operation = new DistributedObjectDestroyOperation(serviceName, name);
            operation.setCallerUuid(source);
            Future<Boolean> f = operationService.createInvocationBuilder(SERVICE_NAME, operation, member.getAddress())
                    .setTryCount(TRY_COUNT).invoke();
            calls.add(f);
        }

        destroyLocalDistributedObject(serviceName, name, source, true);
        waitWithDeadline(calls, DESTROY_TIMEOUT_SECONDS, TimeUnit.SECONDS, destroyProxyExceptionHandler);
    }

    @Override
    public void destroyLocalDistributedObject(String serviceName, String name, UUID source, boolean fireEvent) {
        ProxyRegistry registry = registries.get(serviceName);
        if (registry != null) {
            registry.destroyProxy(name, source, fireEvent);
            destroyedCounter.inc();
        }
        RemoteService service = nodeEngine.getService(serviceName);
        service.destroyDistributedObject(name);
        String message = "DistributedObject[" + service + " -> " + name + "] has been destroyed!";
        Throwable cause = new DistributedObjectDestroyedException(message);
        nodeEngine.getOperationParker().cancelParkedOperations(serviceName, name, cause);
    }

    @Override
    public Collection<DistributedObject> getDistributedObjects(String serviceName) {
        checkServiceNameNotNull(serviceName);

        Collection<DistributedObject> result = new LinkedList<>();
        ProxyRegistry registry = registries.get(serviceName);
        if (registry != null) {
            registry.getDistributedObjects(result);
        }

        return result;
    }

    @Override
    public Collection<String> getDistributedObjectNames(String serviceName) {
        checkServiceNameNotNull(serviceName);

        ProxyRegistry registry = registries.get(serviceName);
        if (registry == null) {
            return Collections.emptySet();
        } else {
            return Collections.unmodifiableCollection(registry.getDistributedObjectNames());
        }
    }

    @Override
    public boolean existsDistributedObject(String serviceName, String objectId) {
        checkServiceNameNotNull(serviceName);

        ProxyRegistry registry = registries.get(serviceName);
        if (registry == null) {
            return false;
        }
        return registry.existsDistributedObject(objectId);
    }

    @Override
    public Collection<DistributedObject> getAllDistributedObjects() {
        Collection<DistributedObject> result = new LinkedList<>();
        for (ProxyRegistry registry : registries.values()) {
            registry.getDistributedObjects(result);
        }
        return result;
    }

    @Override
    public long getCreatedCount(@Nonnull String serviceName) {
        ProxyRegistry registry = registries.get(serviceName);
        return registry == null ? 0 : registry.getCreatedCount();
    }

    @Override
    public UUID addProxyListener(DistributedObjectListener distributedObjectListener) {
        UUID id = UuidUtil.newUnsecureUUID();
        listeners.put(id, distributedObjectListener);
        return id;
    }

    @Override
    public boolean removeProxyListener(UUID registrationId) {
        return listeners.remove(registrationId) != null;
    }

    @Override
    public void dispatchEvent(final DistributedObjectEventPacket eventPacket, Object ignore) {
        String serviceName = eventPacket.getServiceName();
        if (eventPacket.getEventType() == CREATED) {
            try {
                final ProxyRegistry registry = getOrCreateRegistry(serviceName);
                if (!registry.contains(eventPacket.getName())) {
                    registry.createProxy(eventPacket.getName(), eventPacket.getSource(), true, true);
                    // listeners will be called if proxy is created here.
                }
            } catch (HazelcastInstanceNotActiveException ignored) {
                ignore(ignored);
            }
        } else {
            final ProxyRegistry registry = registries.get(serviceName);
            if (registry != null) {
                registry.destroyProxy(eventPacket.getName(), eventPacket.getSource(), false);
            }
        }
    }

    @Override
    public Operation getPostJoinOperation() {
        Collection<ProxyInfo> proxies = new LinkedList<>();
        for (ProxyRegistry registry : registries.values()) {
            registry.getProxyInfos(proxies);
        }
        return proxies.isEmpty() ? null : new PostJoinProxyOperation(proxies);
    }

    public void shutdown() {
        for (ProxyRegistry registry : registries.values()) {
            registry.destroy();
        }
        registries.clear();
        listeners.clear();
    }

    private static void checkServiceNameNotNull(@Nonnull String serviceName) {
        checkNotNull(serviceName, "Service name is required");
    }

    private static void checkObjectNameNotNull(@Nonnull String name) {
        checkNotNull(name, "Object name is required");
    }
}
