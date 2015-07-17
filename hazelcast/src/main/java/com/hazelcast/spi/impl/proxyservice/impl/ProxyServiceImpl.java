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

package com.hazelcast.spi.impl.proxyservice.impl;

import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.DistributedObjectListener;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.EventPublishingService;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.PostJoinAwareService;
import com.hazelcast.spi.ProxyService;
import com.hazelcast.spi.RemoteService;
import com.hazelcast.spi.exception.DistributedObjectDestroyedException;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.proxyservice.InternalProxyService;
import com.hazelcast.spi.impl.proxyservice.impl.operations.DistributedObjectDestroyOperation;
import com.hazelcast.spi.impl.proxyservice.impl.operations.PostJoinProxyOperation;
import com.hazelcast.util.ConstructorFunction;
import com.hazelcast.util.EmptyStatement;
import com.hazelcast.util.FutureUtil.ExceptionHandler;
import com.hazelcast.util.UuidUtil;
import com.hazelcast.util.counters.MwCounter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import static com.hazelcast.core.DistributedObjectEvent.EventType.CREATED;
import static com.hazelcast.util.ConcurrencyUtil.getOrPutIfAbsent;
import static com.hazelcast.util.FutureUtil.logAllExceptions;
import static com.hazelcast.util.FutureUtil.waitWithDeadline;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static com.hazelcast.util.counters.MwCounter.newMwCounter;

public class ProxyServiceImpl
        implements InternalProxyService, PostJoinAwareService, EventPublishingService<DistributedObjectEventPacket, Object> {

    public static final String SERVICE_NAME = "hz:core:proxyService";

    private static final ExceptionHandler DESTROY_PROXY_EXCEPTION_HANDLER = logAllExceptions(Level.WARNING);

    private static final int TRY_COUNT = 10;
    private static final long DESTROY_TIMEOUT_SECONDS = 30;

    final NodeEngineImpl nodeEngine;
    final ILogger logger;
    final ConcurrentMap<String, DistributedObjectListener> listeners =
            new ConcurrentHashMap<String, DistributedObjectListener>();

    private final ConstructorFunction<String, ProxyRegistry> registryConstructor =
            new ConstructorFunction<String, ProxyRegistry>() {
                public ProxyRegistry createNew(String serviceName) {
                    return new ProxyRegistry(ProxyServiceImpl.this, serviceName);
                }
            };

    private final ConcurrentMap<String, ProxyRegistry> registries =
            new ConcurrentHashMap<String, ProxyRegistry>();

    @Probe(name = "createdCount")
    private final MwCounter createdCounter = newMwCounter();
    @Probe(name = "destroyedCount")
    private final MwCounter destroyedCounter = newMwCounter();

    public ProxyServiceImpl(NodeEngineImpl nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.logger = nodeEngine.getLogger(ProxyService.class.getName());
        nodeEngine.getMetricsRegistry().scanAndRegister(this, "proxy");
    }

    public void init() {
        nodeEngine.getEventService().registerListener(SERVICE_NAME, SERVICE_NAME, new Object());
    }

    @Probe(name = "proxyCount")
    @Override
    public int getProxyCount() {
        int count = 0;
        for (ProxyRegistry registry : registries.values()) {
            count += registry.getProxyCount();
        }

        return count;
    }

    @Override
    public void initializeDistributedObject(String serviceName, String name) {
        checkServiceNameNotNull(serviceName);
        checkObjectNameNotNull(name);

        ProxyRegistry registry = getOrCreateRegistry(serviceName);
        registry.createProxy(name, true, true);
        createdCounter.inc();
    }

    public ProxyRegistry getOrCreateRegistry(String serviceName) {
        return getOrPutIfAbsent(registries, serviceName, registryConstructor);
    }

    @Override
    public DistributedObject getDistributedObject(String serviceName, String name) {
        checkServiceNameNotNull(serviceName);
        checkObjectNameNotNull(name);

        ProxyRegistry registry = getOrCreateRegistry(serviceName);
        return registry.getOrCreateProxy(name, true, true);
    }

    @Override
    public void destroyDistributedObject(String serviceName, String name) {
        checkServiceNameNotNull(serviceName);
        checkObjectNameNotNull(name);

        OperationService operationService = nodeEngine.getOperationService();
        Collection<MemberImpl> members = nodeEngine.getClusterService().getMemberList();
        Collection<Future> calls = new ArrayList<Future>(members.size());
        for (MemberImpl member : members) {
            if (member.localMember()) {
                continue;
            }

            DistributedObjectDestroyOperation operation = new DistributedObjectDestroyOperation(serviceName, name);
            Future f = operationService.createInvocationBuilder(SERVICE_NAME, operation, member.getAddress())
                    .setTryCount(TRY_COUNT).invoke();
            calls.add(f);
        }

        destroyLocalDistributedObject(serviceName, name, true);

        waitWithDeadline(calls, DESTROY_TIMEOUT_SECONDS, TimeUnit.SECONDS, DESTROY_PROXY_EXCEPTION_HANDLER);
    }

    public void destroyLocalDistributedObject(String serviceName, String name, boolean fireEvent) {
        ProxyRegistry registry = registries.get(serviceName);
        if (registry != null) {
            registry.destroyProxy(name, fireEvent);
            destroyedCounter.inc();
        }
        RemoteService service = nodeEngine.getService(serviceName);
        if (service != null) {
            service.destroyDistributedObject(name);
        }
        String message = "DistributedObject[" + service + " -> " + name + "] has been destroyed!";
        Throwable cause = new DistributedObjectDestroyedException(message);
        nodeEngine.getWaitNotifyService().cancelWaitingOps(serviceName, name, cause);
    }

    @Override
    public Collection<DistributedObject> getDistributedObjects(String serviceName) {
        checkServiceNameNotNull(serviceName);

        Collection<DistributedObject> result = new LinkedList<DistributedObject>();
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
            return Collections.EMPTY_SET;
        } else {
            return registry.getDistributedObjectNames();
        }
    }

    @Override
    public Collection<DistributedObject> getAllDistributedObjects() {
        Collection<DistributedObject> result = new LinkedList<DistributedObject>();
        for (ProxyRegistry registry : registries.values()) {
            registry.getDistributedObjects(result);
        }
        return result;
    }

    @Override
    public String addProxyListener(DistributedObjectListener distributedObjectListener) {
        String id = UuidUtil.buildRandomUuidString();
        listeners.put(id, distributedObjectListener);
        return id;
    }

    @Override
    public boolean removeProxyListener(String registrationId) {
        return listeners.remove(registrationId) != null;
    }

    @Override
    public void dispatchEvent(final DistributedObjectEventPacket eventPacket, Object ignore) {
        String serviceName = eventPacket.getServiceName();
        if (eventPacket.getEventType() == CREATED) {
            try {
                final ProxyRegistry registry = getOrCreateRegistry(serviceName);
                if (!registry.contains(eventPacket.getName())) {
                    registry.createProxy(eventPacket.getName(), false,
                            true);
                    // listeners will be called if proxy is created here.
                }
            } catch (HazelcastInstanceNotActiveException ignored) {
                EmptyStatement.ignore(ignored);
            }
        } else {
            final ProxyRegistry registry = registries.get(serviceName);
            if (registry != null) {
                registry.destroyProxy(eventPacket.getName(), false);
            }
        }
    }

    @Override
    public Operation getPostJoinOperation() {
        Collection<ProxyInfo> proxies = new LinkedList<ProxyInfo>();
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

    private static String checkServiceNameNotNull(String serviceName) {
        return checkNotNull(serviceName, "Service name is required!");
    }

    private static String checkObjectNameNotNull(String name) {
        return checkNotNull(name, "Object name is required!");
    }
}
