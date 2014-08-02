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
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.EventPublishingService;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.InitializingObject;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.PostJoinAwareService;
import com.hazelcast.spi.ProxyService;
import com.hazelcast.spi.RemoteService;
import com.hazelcast.spi.exception.DistributedObjectDestroyedException;
import com.hazelcast.util.ConstructorFunction;
import com.hazelcast.util.EmptyStatement;
import com.hazelcast.util.FutureUtil;
import com.hazelcast.util.UuidUtil;
import com.hazelcast.util.executor.StripedRunnable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;

import static com.hazelcast.core.DistributedObjectEvent.EventType;
import static com.hazelcast.core.DistributedObjectEvent.EventType.CREATED;
import static com.hazelcast.core.DistributedObjectEvent.EventType.DESTROYED;
import static com.hazelcast.util.ConcurrencyUtil.getOrPutIfAbsent;

public class ProxyServiceImpl
        implements ProxyService, PostJoinAwareService, EventPublishingService<DistributedObjectEventPacket, Object> {

    static final String SERVICE_NAME = "hz:core:proxyService";

    private static final FutureUtil.ExceptionHandler DESTROY_PROXY_EXCEPTION_HANDLER = FutureUtil.logAllExceptions(Level.FINEST);

    private static final int TRY_COUNT = 10;
    private static final long TIME = 3;

    private final ConstructorFunction<String, ProxyRegistry> registryConstructor =
            new ConstructorFunction<String, ProxyRegistry>() {
                public ProxyRegistry createNew(String serviceName) {
                    return new ProxyRegistry(serviceName);
                }
            };

    private final NodeEngineImpl nodeEngine;
    private final ConcurrentMap<String, ProxyRegistry> registries =
            new ConcurrentHashMap<String, ProxyRegistry>();
    private final ConcurrentMap<String, DistributedObjectListener> listeners =
            new ConcurrentHashMap<String, DistributedObjectListener>();
    private final ILogger logger;

    ProxyServiceImpl(NodeEngineImpl nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.logger = nodeEngine.getLogger(ProxyService.class.getName());
    }

    void init() {
        nodeEngine.getEventService().registerListener(SERVICE_NAME, SERVICE_NAME, new Object());
    }


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
        if (serviceName == null) {
            throw new NullPointerException("Service name is required!");
        }
        if (name == null) {
            throw new NullPointerException("Object name is required!");
        }
        ProxyRegistry registry = getOrPutIfAbsent(registries, serviceName, registryConstructor);
        registry.createProxy(name, true, true);
    }

    @Override
    public DistributedObject getDistributedObject(String serviceName, String name) {
        if (serviceName == null) {
            throw new NullPointerException("Service name is required!");
        }
        if (name == null) {
            throw new NullPointerException("Object name is required!");
        }
        ProxyRegistry registry = getOrPutIfAbsent(registries, serviceName, registryConstructor);
        return registry.getOrCreateProxy(name, true, true);
    }

    @Override
    public void destroyDistributedObject(String serviceName, String name) {
        if (serviceName == null) {
            throw new NullPointerException("Service name is required!");
        }
        if (name == null) {
            throw new NullPointerException("Object name is required!");
        }
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

        try {
            FutureUtil.waitWithDeadline(calls, TIME, TimeUnit.SECONDS, DESTROY_PROXY_EXCEPTION_HANDLER);
        } catch (TimeoutException e) {
            logger.finest(e);
        }
    }

    private void destroyLocalDistributedObject(String serviceName, String name, boolean fireEvent) {
        ProxyRegistry registry = registries.get(serviceName);
        if (registry != null) {
            registry.destroyProxy(name, fireEvent);
        }
        final RemoteService service = nodeEngine.getService(serviceName);
        if (service != null) {
            service.destroyDistributedObject(name);
        }
        Throwable cause = new DistributedObjectDestroyedException(serviceName, name);
        nodeEngine.waitNotifyService.cancelWaitingOps(serviceName, name, cause);
    }

    @Override
    public Collection<DistributedObject> getDistributedObjects(String serviceName) {
        if (serviceName == null) {
            throw new NullPointerException("Service name is required!");
        }
        Collection<DistributedObject> objects = new LinkedList<DistributedObject>();
        ProxyRegistry registry = registries.get(serviceName);
        if (registry != null) {
            Collection<DistributedObjectFuture> futures = registry.proxies.values();
            for (DistributedObjectFuture future : futures) {
                objects.add(future.get());
            }
        }
        return objects;
    }

    @Override
    public Collection<DistributedObject> getAllDistributedObjects() {
        Collection<DistributedObject> objects = new LinkedList<DistributedObject>();
        for (ProxyRegistry registry : registries.values()) {
            Collection<DistributedObjectFuture> futures = registry.proxies.values();
            for (DistributedObjectFuture future : futures) {
                objects.add(future.get());
            }
        }
        return objects;
    }

    @Override
    public String addProxyListener(DistributedObjectListener distributedObjectListener) {
        final String id = UuidUtil.buildRandomUuidString();
        listeners.put(id, distributedObjectListener);
        return id;
    }

    @Override
    public boolean removeProxyListener(String registrationId) {
        return listeners.remove(registrationId) != null;
    }

    @Override
    public void dispatchEvent(final DistributedObjectEventPacket eventPacket, Object ignore) {
        final String serviceName = eventPacket.getServiceName();
        if (eventPacket.getEventType() == CREATED) {
            try {
                final ProxyRegistry registry = getOrPutIfAbsent(registries, serviceName, registryConstructor);
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
            for (Map.Entry<String, DistributedObjectFuture> entry : registry.proxies.entrySet()) {
                final DistributedObjectFuture future = entry.getValue();
                if (!future.isSet()) {
                    continue;
                }
                final DistributedObject distributedObject = future.get();
                if (distributedObject instanceof InitializingObject) {
                    proxies.add(new ProxyInfo(registry.serviceName, entry.getKey()));
                }
            }
        }
        return proxies.isEmpty() ? null : new PostJoinProxyOperation(proxies);
    }

    private final class ProxyRegistry {

        final String serviceName;
        final RemoteService service;
        final ConcurrentMap<String, DistributedObjectFuture> proxies = new ConcurrentHashMap<String, DistributedObjectFuture>();

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

        /**
         * Retrieves a DistributedObject proxy or creates it if it's not available
         *
         * @param name         name of the proxy object
         * @param publishEvent true if a DistributedObjectEvent should  be fired
         * @param initialize   true if proxy object should be initialized
         * @return a DistributedObject instance
         */
        DistributedObject getOrCreateProxy(final String name, boolean publishEvent, boolean initialize) {
            DistributedObjectFuture proxyFuture = proxies.get(name);
            if (proxyFuture == null) {
                if (!nodeEngine.isActive()) {
                    throw new HazelcastInstanceNotActiveException();
                }
                proxyFuture = createProxy(name, publishEvent, initialize);
                if (proxyFuture == null) {
                    // warning; recursive call! I (@mdogan) do not think this will ever cause a stack overflow..
                    return getOrCreateProxy(name, publishEvent, initialize);
                }
            }
            return proxyFuture.get();
        }

        /**
         * Creates a DistributedObject proxy if it's not created yet
         *
         * @param name         name of the proxy object
         * @param publishEvent true if a DistributedObjectEvent should  be fired
         * @param initialize   true if proxy object should be initialized
         * @return a DistributedObject instance if it's created by this method, null otherwise
         */
        DistributedObjectFuture createProxy(final String name, boolean publishEvent, boolean initialize) {
            if (!proxies.containsKey(name)) {
                if (!nodeEngine.isActive()) {
                    throw new HazelcastInstanceNotActiveException();
                }
                DistributedObjectFuture proxyFuture = new DistributedObjectFuture();
                if (proxies.putIfAbsent(name, proxyFuture) == null) {
                    DistributedObject proxy = service.createDistributedObject(name);
                    if (initialize && proxy instanceof InitializingObject) {
                        try {
                            ((InitializingObject) proxy).initialize();
                        } catch (Exception e) {
                            logger.warning("Error while initializing proxy: " + proxy, e);
                        }
                    }
                    nodeEngine.eventService.executeEventCallback(new ProxyEventProcessor(CREATED, serviceName, proxy));
                    if (publishEvent) {
                        publish(new DistributedObjectEventPacket(CREATED, serviceName, name));
                    }
                    proxyFuture.set(proxy);
                    return proxyFuture;
                }
            }
            return null;
        }

        void destroyProxy(String name, boolean publishEvent) {
            final DistributedObjectFuture proxyFuture = proxies.remove(name);
            if (proxyFuture != null) {
                DistributedObject proxy = proxyFuture.get();
                nodeEngine.eventService.executeEventCallback(new ProxyEventProcessor(DESTROYED, serviceName, proxy));
                if (publishEvent) {
                    publish(new DistributedObjectEventPacket(DESTROYED, serviceName, name));
                }
            }
        }

        private void publish(DistributedObjectEventPacket event) {
            final EventService eventService = nodeEngine.getEventService();
            final Collection<EventRegistration> registrations = eventService.getRegistrations(SERVICE_NAME, SERVICE_NAME);
            eventService.publishEvent(SERVICE_NAME, registrations, event, event.getName().hashCode());
        }

        private boolean contains(String name) {
            return proxies.containsKey(name);
        }

        void destroy() {
            for (DistributedObjectFuture future : proxies.values()) {
                DistributedObject distributedObject = future.get();
                if (distributedObject instanceof AbstractDistributedObject) {
                    ((AbstractDistributedObject) distributedObject).invalidate();
                }
            }
            proxies.clear();
        }

        public int getProxyCount() {
            return proxies.size();
        }
    }

    private static class DistributedObjectFuture {

        volatile DistributedObject proxy;

        boolean isSet() {
            return proxy != null;
        }

        DistributedObject get() {
            if (proxy == null) {
                boolean interrupted = false;
                synchronized (this) {
                    while (proxy == null) {
                        try {
                            wait();
                        } catch (InterruptedException e) {
                            interrupted = true;
                        }
                    }
                }
                if (interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
            return proxy;
        }

        void set(DistributedObject o) {
            if (o == null) {
                throw new IllegalArgumentException();
            }
            synchronized (this) {
                proxy = o;
                notifyAll();
            }
        }
    }

    private final class ProxyEventProcessor
            implements StripedRunnable {

        final EventType type;
        final String serviceName;
        final DistributedObject object;

        private ProxyEventProcessor(EventType eventType, String serviceName, DistributedObject object) {
            this.type = eventType;
            this.serviceName = serviceName;
            this.object = object;
        }

        @Override
        public void run() {
            DistributedObjectEvent event = new DistributedObjectEvent(type, serviceName, object);
            for (DistributedObjectListener listener : listeners.values()) {
                if (EventType.CREATED.equals(type)) {
                    listener.distributedObjectCreated(event);
                } else if (EventType.DESTROYED.equals(type)) {
                    listener.distributedObjectDestroyed(event);
                }
            }
        }

        @Override
        public int getKey() {
            return object.getName().hashCode();
        }
    }

    public static class DistributedObjectDestroyOperation
            extends AbstractOperation {

        private String serviceName;
        private String name;

        public DistributedObjectDestroyOperation() {
        }

        public DistributedObjectDestroyOperation(String serviceName, String name) {
            this.serviceName = serviceName;
            this.name = name;
        }

        @Override
        public void run()
                throws Exception {
            ProxyServiceImpl proxyService = getService();
            proxyService.destroyLocalDistributedObject(serviceName, name, false);
        }

        @Override
        public boolean returnsResponse() {
            return true;
        }

        @Override
        public Object getResponse() {
            return Boolean.TRUE;
        }

        @Override
        protected void writeInternal(ObjectDataOutput out)
                throws IOException {
            super.writeInternal(out);
            out.writeUTF(serviceName);
            out.writeObject(name);
            // writing as object for backward-compatibility
        }

        @Override
        protected void readInternal(ObjectDataInput in)
                throws IOException {
            super.readInternal(in);
            serviceName = in.readUTF();
            name = in.readObject();
        }
    }

    public static class PostJoinProxyOperation
            extends AbstractOperation {

        private Collection<ProxyInfo> proxies;

        public PostJoinProxyOperation() {
        }

        public PostJoinProxyOperation(Collection<ProxyInfo> proxies) {
            this.proxies = proxies;
        }

        @Override
        public void run()
                throws Exception {
            if (proxies != null && proxies.size() > 0) {
                NodeEngine nodeEngine = getNodeEngine();
                ProxyServiceImpl proxyService = getService();
                for (ProxyInfo proxy : proxies) {
                    final ProxyRegistry registry = getOrPutIfAbsent(proxyService.registries, proxy.serviceName,
                            proxyService.registryConstructor);
                    DistributedObjectFuture future = registry.createProxy(proxy.objectName, false, false);
                    if (future != null) {
                        final DistributedObject object = future.get();
                        if (object instanceof InitializingObject) {
                            nodeEngine.getExecutionService().execute(ExecutionService.SYSTEM_EXECUTOR, new Runnable() {
                                public void run() {
                                    try {
                                        ((InitializingObject) object).initialize();
                                    } catch (Exception e) {
                                        getLogger().warning("Error while initializing proxy: " + object, e);
                                    }
                                }
                            });
                        }
                    }
                }
            }
        }

        @Override
        public String getServiceName() {
            return ProxyServiceImpl.SERVICE_NAME;
        }

        @Override
        public boolean returnsResponse() {
            return false;
        }

        @Override
        protected void writeInternal(ObjectDataOutput out)
                throws IOException {
            super.writeInternal(out);
            int len = proxies != null ? proxies.size() : 0;
            out.writeInt(len);
            if (len > 0) {
                for (ProxyInfo proxy : proxies) {
                    out.writeUTF(proxy.serviceName);
                    out.writeObject(proxy.objectName);
                    // writing as object for backward-compatibility
                }
            }
        }

        @Override
        protected void readInternal(ObjectDataInput in)
                throws IOException {
            super.readInternal(in);
            int len = in.readInt();
            if (len > 0) {
                proxies = new ArrayList<ProxyInfo>(len);
                for (int i = 0; i < len; i++) {
                    ProxyInfo proxy = new ProxyInfo(in.readUTF(), (String) in.readObject());
                    proxies.add(proxy);
                }
            }
        }
    }

    private static final class ProxyInfo {
        final String serviceName;
        final String objectName;

        private ProxyInfo(String serviceName, String objectName) {
            this.serviceName = serviceName;
            this.objectName = objectName;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("ProxyInfo{");
            sb.append("serviceName='").append(serviceName).append('\'');
            sb.append(", objectName='").append(objectName).append('\'');
            sb.append('}');
            return sb.toString();
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
