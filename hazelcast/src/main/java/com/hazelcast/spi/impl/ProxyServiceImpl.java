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
import com.hazelcast.spi.DistributedObjectAccessor;
import com.hazelcast.spi.EventPublishingService;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.InitializingObject;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PostJoinAwareService;
import com.hazelcast.spi.ProxyService;
import com.hazelcast.spi.RemoteService;
import com.hazelcast.spi.exception.DistributedObjectDestroyedException;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;
import com.hazelcast.util.UuidUtil;
import com.hazelcast.util.executor.StripedRunnable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.core.DistributedObjectEvent.EventType;
import static com.hazelcast.core.DistributedObjectEvent.EventType.CREATED;
import static com.hazelcast.core.DistributedObjectEvent.EventType.DESTROYED;

/**
 * @author mdogan 1/11/13
 */
public class ProxyServiceImpl implements ProxyService, PostJoinAwareService,
        EventPublishingService<DistributedObjectEventPacket, Object> {

    static final String SERVICE_NAME = "hz:core:proxyService";

    private final NodeEngineImpl nodeEngine;
    private final ConcurrentMap<String, ProxyRegistry> registries = new ConcurrentHashMap<String, ProxyRegistry>();
    private final ConcurrentMap<String, DistributedObjectListener> listeners = new ConcurrentHashMap<String, DistributedObjectListener>();
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

    @Override
    public int getProxyCount() {
        int count = 0;
        for (ProxyRegistry registry : registries.values()) {
            count += registry.getProxyCount();
        }

        return count;
    }

    public void initializeDistributedObject(String serviceName, String name) {
        if (serviceName == null) {
            throw new NullPointerException("Service name is required!");
        }
        if (name == null) {
            throw new NullPointerException("Object name is required!");
        }
        ProxyRegistry registry = ConcurrencyUtil.getOrPutIfAbsent(registries, serviceName, registryConstructor);
        registry.getProxy(name, true, true);
    }

    public DistributedObject getDistributedObject(String serviceName, String name) {
        if (serviceName == null) {
            throw new NullPointerException("Service name is required!");
        }
        if (name == null) {
            throw new NullPointerException("Object name is required!");
        }
        ProxyRegistry registry = ConcurrencyUtil.getOrPutIfAbsent(registries, serviceName, registryConstructor);
        return registry.getProxy(name, true, true);
    }

    public void destroyDistributedObject(String serviceName, String name) {
        if (serviceName == null) {
            throw new NullPointerException("Service name is required!");
        }
        if (name == null) {
            throw new NullPointerException("Object name is required!");
        }
        Collection<MemberImpl> members = nodeEngine.getClusterService().getMemberList();
        Collection<Future> calls = new ArrayList<Future>(members.size());
        for (MemberImpl member : members) {
            if (member.localMember()) continue;

            Future f = nodeEngine.getOperationService().createInvocationBuilder(SERVICE_NAME,
                    new DistributedObjectDestroyOperation(serviceName, name), member.getAddress())
                    .setTryCount(10).invoke();
            calls.add(f);
        }

        destroyLocalDistributedObject(serviceName, name, true);

        for (Future f : calls) {
            try {
                f.get(3, TimeUnit.SECONDS);
            } catch (Exception e) {
                logger.finest(e);
            }
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
        final String id = UuidUtil.buildRandomUuidString();
        listeners.put(id, distributedObjectListener);
        return id;
    }

    public boolean removeProxyListener(String registrationId) {
        return listeners.remove(registrationId) != null;
    }

    public void dispatchEvent(final DistributedObjectEventPacket eventPacket, Object ignore) {
        final String serviceName = eventPacket.getServiceName();
        if (eventPacket.getEventType() == CREATED) {
            try {
                final ProxyRegistry registry = ConcurrencyUtil.getOrPutIfAbsent(registries, serviceName, registryConstructor);
                if (!registry.contains(eventPacket.getName())) {
                    registry.getProxy(eventPacket.getName(), false, true); // listeners will be called if proxy is created here.
                }
            } catch (HazelcastInstanceNotActiveException ignored) {
            }
        } else {
            final ProxyRegistry registry = registries.get(serviceName);
            if (registry != null) {
                registry.destroyProxy(eventPacket.getName(), false);
            }
        }
    }

    public Operation getPostJoinOperation() {
        Collection<ProxyInfo> proxies = new LinkedList<ProxyInfo>();
        for (ProxyRegistry registry : registries.values()) {
            for (DistributedObject distributedObject : registry.proxies.values()) {
                if (distributedObject instanceof InitializingObject) {
                    proxies.add(new ProxyInfo(registry.serviceName, distributedObject.getName()));
                }
            }
        }
        return proxies.isEmpty() ? null : new PostJoinProxyOperation(proxies);
    }

    private class ProxyRegistry {

        final String serviceName;
        final RemoteService service;
        final ConcurrentMap<String, DistributedObject> proxies = new ConcurrentHashMap<String, DistributedObject>();

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

        DistributedObject getProxy(final String name, boolean publishEvent, boolean initialize) {
            DistributedObject proxy = proxies.get(name);
            if (proxy == null) {
                if (!nodeEngine.isActive()) {
                    throw new HazelcastInstanceNotActiveException();
                }
                proxy = service.createDistributedObject(name);
                DistributedObject current = proxies.putIfAbsent(name, proxy);
                if (current == null) {
                    if (initialize && proxy instanceof InitializingObject) {
                        try {
                            ((InitializingObject) proxy).initialize();
                        } catch (Exception e) {
                            logger.warning("Error while initializing proxy: " + proxy, e);
                        }
                    }
                    nodeEngine.eventService.executeEvent(new ProxyEventProcessor(CREATED, serviceName, proxy));
                    if (publishEvent) {
                        publish(new DistributedObjectEventPacket(CREATED, serviceName, name));
                    }
                } else {
                    proxy = current;
                }
            }
            return proxy;
        }

        DistributedObject destroyProxy(String name, boolean publishEvent) {
            final DistributedObject proxy;
            if ((proxy = proxies.remove(name)) != null) {
                nodeEngine.eventService.executeEvent(new ProxyEventProcessor(DESTROYED, serviceName, proxy));
                if (publishEvent) {
                    publish(new DistributedObjectEventPacket(DESTROYED, serviceName, name));
                }
                return proxy;
            }
            return null;
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
            for (DistributedObject distributedObject : proxies.values()) {
                if (distributedObject instanceof AbstractDistributedObject) {
                    DistributedObjectAccessor.onNodeShutdown((AbstractDistributedObject) distributedObject);
                }
            }
            proxies.clear();
        }

        public int getProxyCount() {
            return proxies.size();
        }
    }

    private class ProxyEventProcessor implements StripedRunnable {

        final EventType type;
        final String serviceName;
        final DistributedObject object;

        private ProxyEventProcessor(EventType eventType, String serviceName, DistributedObject object) {
            this.type = eventType;
            this.serviceName = serviceName;
            this.object = object;
        }

        public void run() {
            DistributedObjectEvent event = new DistributedObjectEvent(type, serviceName, object);
            for (DistributedObjectListener listener : listeners.values()) {
                if (EventType.CREATED.equals(type)){
                    listener.distributedObjectCreated(event);
                } else if (EventType.DESTROYED.equals(type)){
                    listener.distributedObjectDestroyed(event);
                }
            }
        }

        public int getKey() {
            return object.getId().hashCode();
        }
    }

    public static class DistributedObjectDestroyOperation extends AbstractOperation {

        private String serviceName;
        private String name;

        public DistributedObjectDestroyOperation() {
        }

        public DistributedObjectDestroyOperation(String serviceName, String name) {
            this.serviceName = serviceName;
            this.name = name;
        }

        public void run() throws Exception {
            ProxyServiceImpl proxyService = getService();
            proxyService.destroyLocalDistributedObject(serviceName, name, false);
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
            out.writeObject(name); // writing as object for backward-compatibility
        }

        @Override
        protected void readInternal(ObjectDataInput in) throws IOException {
            super.readInternal(in);
            serviceName = in.readUTF();
            name = in.readObject();
        }
    }

    public static class PostJoinProxyOperation extends AbstractOperation {

        private Collection<ProxyInfo> proxies;

        public PostJoinProxyOperation() {
        }

        public PostJoinProxyOperation(Collection<ProxyInfo> proxies) {
            this.proxies = proxies;
        }

        @Override
        public void run() throws Exception {
            if (proxies != null && proxies.size() > 0) {
                NodeEngine nodeEngine = getNodeEngine();
                ProxyServiceImpl proxyService = getService();
                for (ProxyInfo proxy : proxies) {
                    final ProxyRegistry registry = ConcurrencyUtil.getOrPutIfAbsent(proxyService.registries, proxy.serviceName, proxyService.registryConstructor);
                    final DistributedObject object = registry.getProxy(proxy.objectName, false, false);

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

                    proxyService.getDistributedObject(proxy.serviceName, proxy.objectName);
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
        protected void writeInternal(ObjectDataOutput out) throws IOException {
            super.writeInternal(out);
            int len = proxies != null ? proxies.size() : 0;
            out.writeInt(len);
            if (len > 0) {
                for (ProxyInfo proxy : proxies) {
                    out.writeUTF(proxy.serviceName);
                    out.writeObject(proxy.objectName); // writing as object for backward-compatibility
                }
            }
        }

        @Override
        protected void readInternal(ObjectDataInput in) throws IOException {
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

    private static class ProxyInfo {
        final String serviceName;
        final String objectName;

        private ProxyInfo(String serviceName, String objectName) {
            this.serviceName = serviceName;
            this.objectName = objectName;
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
